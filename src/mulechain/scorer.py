"""Scorer: a bank's detection worker. Kafka events in, Valkey state out.

    BANK=B python -m mulechain.scorer

The patterns this file shows (docs/patterns.md):

- The log is the source of truth. Kafka keeps every event; everything in Valkey is
  derived from it. Valkey also holds how far into each partition its state goes, and
  a worker starts reading there, so an emptied Valkey is rebuilt from the start of
  the topic with no other step.
- Partition ownership. Events are keyed by account, so each account lives in one
  partition and one worker owns it at a time. Workers share no account state, and
  KEDA adds workers as consumer lag grows, up to one per partition.
- State and position move together. An event's writes and its partition's next
  offset commit in one MULTI/EXEC, so a crash can't leave one without the other.
  Scoring an event again rewrites the same keys, so a replay is harmless too.
- Event time. Windows use the payment's own time, not arrival time, so replay speed
  and late events don't change what is detected.

Valkey layout (everything a rule or the privacy node needs, nothing else):

    win:<account>             sorted set of the account's recent events, by event time
    pay:<payment>:<in|out>    this bank's own side of a payment (hash)
    tok:<token>               "<payment>:<in|out>", the index peers' queries land in
    link:in:<payment>         payments a rule links onward from this incoming one (set)
    link:out:<payment>        payments a rule links into this outgoing one (set)
    last:<account>:<payee>    "<unix seconds>:<flagged>", the last payment to that payee
    hits                      stream of rule hits, read by the privacy node
    offsets                   the next Kafka offset to score, per partition (hash)
    keys                      peers' public keys, written by the privacy node (hash)
"""

import hashlib
import json
import os
import signal
import threading
import time
from collections.abc import Callable
from datetime import datetime
from decimal import Decimal

from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaException
from valkey import Valkey

from mulechain import crypto, log
from mulechain.ingestor import topic

# The four rules. Amount ratios are outgoing / incoming on one account.
FAST_RATIO, FAST_DELAY = (Decimal("0.90"), Decimal("0.98")), (60, 30 * 60)
CHAIN_RATIO, CHAIN_DELAY = (Decimal("0.90"), Decimal("1.10")), (60, 72 * 3600)
REPEAT_WINDOW = 24 * 3600
WINDOW = CHAIN_DELAY[1]  # also the allowed lateness: events are kept for twice this
TTL = 7 * 24 * 3600  # garbage collection only; must exceed 2 x WINDOW
KEY_RELOAD_SECONDS = 10


class Scorer:
    def __init__(self, bank: str, db: Valkey, pair_keys: Callable[[str], list[bytes]]) -> None:
        """`db` needs decode_responses=True. `pair_keys(peer)` gives the keys shared with a peer."""
        self.bank, self.db, self.pair_keys = bank, db, pair_keys

    def process(self, e: dict, partition: int = 0, offset: int = 0) -> dict:
        """Score one event and write its state. Returns the links and hits it made.

        `partition` and `offset` say where the event sits in Kafka.
        """
        made = {"links": [], "hits": []}
        tx = self.db.pipeline(transaction=True)  # MULTI ... EXEC: all or nothing
        self._score(e, tx, made)
        tx.hset("offsets", partition, offset + 1)
        tx.execute()
        return made

    def _score(self, e: dict, tx, made: dict) -> None:
        payment, direction, account = e["payment_id"], e["direction"], e["account"]
        now = datetime.fromisoformat(e["event_time"]).timestamp()

        # 1. This bank's side of the payment, and its token if it crosses banks.
        own = {k: e[k] for k in ("account", "amount", "counterparty_bank")}
        if e["counterparty_bank"] != self.bank:
            tokens = [
                crypto.token(k, payment).hex() for k in self.pair_keys(e["counterparty_bank"])
            ]
            for token in tokens:  # one per key pair, so the peer finds it during a rotation
                tx.set(f"tok:{token}", f"{payment}:{direction}", ex=TTL)
            own["token"] = tokens[0]  # what this bank calls the transfer when it asks the peer
        tx.hset(f"pay:{payment}:{direction}", mapping=own)
        tx.expire(f"pay:{payment}:{direction}", TTL)

        # 2. The account's window, by event time. A repeat or a too-late event stops here.
        window = [
            (json.loads(member), ts)
            for member, ts in self.db.zrange(f"win:{account}", 0, -1, withscores=True)
        ]
        latest = max([now, *(ts for _, ts in window)])
        repeat = any(o["payment_id"] == payment and o["direction"] == direction for o, _ in window)
        if repeat or now < latest - WINDOW:
            return
        tx.zremrangebyscore(f"win:{account}", "-inf", f"({latest - 2 * WINDOW}")

        # 3. Fast pass-through and chain: pair this event with each opposite one.
        flagged = False
        for other, ts in window:
            if other["direction"] == direction or other["payment_id"] == payment:
                continue
            incoming, outgoing = (other, e) if direction == "out" else (e, other)
            delay = now - ts if direction == "out" else ts - now
            ratio = Decimal(str(outgoing["amount"])) / Decimal(str(incoming["amount"]))
            crosses = {incoming["counterparty_bank"], outgoing["counterparty_bank"]} != {self.bank}
            fast = within(delay, FAST_DELAY) and within(ratio, FAST_RATIO)
            chain = within(delay, CHAIN_DELAY) and within(ratio, CHAIN_RATIO) and crosses
            if not (fast or chain):
                continue
            pair = [incoming["payment_id"], outgoing["payment_id"]]
            tx.sadd(f"link:in:{pair[0]}", pair[1])
            tx.sadd(f"link:out:{pair[1]}", pair[0])
            tx.expire(f"link:in:{pair[0]}", TTL)
            tx.expire(f"link:out:{pair[1]}", TTL)
            made["links"].append(pair)
            if chain:  # only chain links are hits; a fast link alone never makes a case
                flagged = True
                self._hit(tx, made, "chain", outgoing=pair[1], incoming=pair[0])

        # 4. New payee and quick repeat: one entry per payer and payee, kept forever.
        if direction == "out":
            payee, cross = e["counterparty_account"], e["counterparty_bank"] != self.bank
            last = self.db.get(f"last:{account}:{payee}")
            last_ts, last_flagged = map(float, last.split(":")) if last else (None, 0)
            if cross and last is None:
                flagged = True
                self._hit(tx, made, "new_payee", payment)
            elif cross and last_flagged and 0 <= now - last_ts <= REPEAT_WINDOW:
                self._hit(tx, made, "quick_repeat", payment)
            if last is None or now >= last_ts:  # a late, older payment keeps the newer entry
                tx.set(f"last:{account}:{payee}", f"{now:.0f}:{int(flagged)}")

        # 5. Remember this event for the ones that follow.
        fields = ("payment_id", "direction", "amount", "counterparty_bank")
        member = json.dumps({k: e[k] for k in fields}, sort_keys=True, separators=(",", ":"))
        tx.zadd(f"win:{account}", {member: now})
        tx.expire(f"win:{account}", TTL)

    def _hit(self, tx, made: dict, rule: str, outgoing: str, incoming: str = "") -> None:
        """Append a rule hit to the stream the privacy node reads."""
        payments = [incoming, outgoing] if incoming else [outgoing]
        hit = {"id": hit_identity(self.bank, rule, *payments), "rule": rule}
        tx.xadd("hits", hit | {"incoming": incoming, "outgoing": outgoing})
        tx.expire("hits", TTL)
        made["hits"].append(rule)


def within(value, bounds) -> bool:
    return bounds[0] <= value <= bounds[1]


def hit_identity(bank: str, rule: str, *payments: str) -> str:
    """The same hit always gets the same ID, so a replayed event repeats nothing."""
    parts = json.dumps([bank, rule, *payments], separators=(",", ":"))
    return hashlib.sha256(parts.encode()).hexdigest()


class PairKeys:
    """This bank's pair keys with each peer, re-derived every few seconds.

    Own private keys come from the mounted Secret; peers' public keys from the `keys`
    hash the privacy node keeps in Valkey. Re-reading both is what makes a key rotation
    take effect without a restart.
    """

    def __init__(self, bank: str, key_dir: str, db: Valkey) -> None:
        self.bank, self.key_dir, self.db = bank, key_dir, db
        self.keys, self.loaded = {}, -KEY_RELOAD_SECONDS

    def __call__(self, peer: str) -> list[bytes]:
        while peer not in self.keys or time.monotonic() - self.loaded >= KEY_RELOAD_SECONDS:
            own = crypto.load_keys(self.key_dir)
            self.keys = {
                peer: crypto.pair_keys(self.bank, peer, own, crypto.decode(text))
                for peer, text in self.db.hgetall("keys").items()
            }
            self.loaded = time.monotonic()
            if peer not in self.keys:  # the node hasn't met that bank yet
                log("scorer", bank=self.bank, waiting_for_keys_of=peer)
                time.sleep(1)
        return self.keys[peer]


def main() -> None:
    bank = os.environ["BANK"]
    db = Valkey.from_url(os.environ["VALKEY_URL"], decode_responses=True)
    scorer = Scorer(bank, db, PairKeys(bank, os.environ["KEY_DIR"], db))
    consumer = Consumer(
        {
            "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            "group.id": f"scorer-{bank}",
            # Offsets are committed to Kafka only so that KEDA can see the lag: the
            # background commit sends the offsets stored below, after each event.
            "enable.auto.offset.store": False,
            "queued.max.messages.kbytes": 16384,  # 16 MiB prefetch, not the 64 MiB default
        }
    )
    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())

    def resume(_, partitions: list) -> None:
        """Start each partition where Valkey's state ends, or at the start of the topic."""
        done = db.hgetall("offsets")
        for p in partitions:
            p.offset = int(done.get(str(p.partition), OFFSET_BEGINNING))
        consumer.assign(partitions)

    consumer.subscribe([topic(bank)], on_assign=resume)
    try:
        while not stop.is_set():
            message = consumer.poll(1)
            if message is None:
                continue
            if message.error():
                if message.error().fatal():
                    raise KafkaException(message.error())
                log("scorer", bank=bank, error=str(message.error()))  # the client retries
                continue
            event = json.loads(message.value())
            made = scorer.process(event, message.partition(), message.offset())
            if made["links"] or made["hits"]:
                kafka_ts = message.timestamp()[1] / 1000  # when the ingestor accepted it
                log("scorer", bank=bank, payment_id=event["payment_id"], kafka_ts=kafka_ts, **made)
            consumer.store_offsets(message=message)
    finally:
        consumer.close()  # commits the stored offsets and leaves the group


if __name__ == "__main__":
    main()
