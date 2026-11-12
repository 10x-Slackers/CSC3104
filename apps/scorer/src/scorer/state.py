"""A bank's scorer state, with the entries of streaming.md's Valkey layout.

MemoryState keeps them in dicts (tests and the offline evaluator); ValkeyState keeps
them in the bank's Valkey (live). Both have the same methods.
"""

import json
from collections import defaultdict
from typing import TypedDict

from valkey import Valkey

from ingestor import Direction, Event

TTL = 7 * 24 * 3600  # garbage collection only; must exceed 2 x the 72 h chain window


class Hit(TypedDict):
    rule: str
    account: str
    payments: list[str]
    hit_id: str


class MemoryState:
    def __init__(self) -> None:
        # win:<account>: (payment_id, direction) -> (ts, event)
        self.win = defaultdict(dict)
        self.pay = {}  # pay:<payment_id>:<direction>
        self.tok = {}  # tok:<pair>:<version>:<token>
        self.link = defaultdict(set)  # link:in:<payment_id> and link:out:<payment_id>
        self.hit = {}  # hit:<hit_id>, in the order written (the hits stream)
        self.paid = {}  # last:<account>:<payee> -> (ts, flagged)

    # written by the scorer

    def window(self, account: str) -> dict[tuple[str, Direction], tuple[float, Event]]:
        return self.win[account]

    def add_event(
        self, account: str, key: tuple[str, Direction], ts: float, event: Event
    ) -> None:
        self.win[account][key] = (ts, event)

    def trim(self, account: str, before: float) -> None:
        for key, (ts, _) in list(self.win[account].items()):
            if ts < before:
                del self.win[account][key]

    def put_payment(self, payment_id: str, direction: Direction, entry: dict) -> None:
        self.pay[payment_id, direction] = entry

    def put_token(self, pair: str, version: str, token: str, entry: dict) -> None:
        self.tok[pair, version, token] = entry

    def add_link(self, incoming: str, outgoing: str) -> None:
        self.link["in", incoming].add(outgoing)
        self.link["out", outgoing].add(incoming)

    def put_hit(self, hit: Hit) -> None:
        self.hit.setdefault(hit["hit_id"], hit)

    def last_paid(self, account: str, payee: str) -> tuple[float, bool] | None:
        return self.paid.get((account, payee))

    def set_last_paid(self, account: str, payee: str, ts: float, flagged: bool) -> None:
        self.paid[account, payee] = (ts, flagged)

    def commit(self) -> None:
        pass  # every write above is already in place

    # read by the node and the offline evaluator

    def links(self, payment_id: str, direction: Direction) -> list[str]:
        return sorted(self.link.get((direction, payment_id), ()))

    def payment(self, payment_id: str, direction: Direction) -> dict | None:
        return self.pay.get((payment_id, direction))

    def lookup(self, pair: str, version: str, token: str) -> dict | None:
        return self.tok.get((pair, version, token))

    def hits(self) -> list[Hit]:
        return list(self.hit.values())


class ValkeyState:
    """Reads go straight to Valkey. Writes wait in a MULTI pipeline until commit(), so a
    crash leaves an event either fully written or not at all. Each account has one
    writer (the replica that owns its partition), so reads see only committed events.
    """

    def __init__(self, client: Valkey, ttl: int = TTL) -> None:
        self.db, self.ttl = client, ttl
        self.tx = client.pipeline(transaction=True)

    def _expire(self, key: str) -> None:
        self.tx.expire(key, self.ttl)

    # written by the scorer

    def window(self, account: str) -> dict[tuple[str, Direction], tuple[float, Event]]:
        events = {}
        for member, ts in self.db.zrange(f"win:{account}", 0, -1, withscores=True):
            e = json.loads(member)
            events[e["payment_id"], e["direction"]] = (ts, e)
        return events

    def add_event(
        self, account: str, key: tuple[str, Direction], ts: float, event: Event
    ) -> None:
        member = json.dumps(event, sort_keys=True, separators=(",", ":"))
        self.tx.zadd(f"win:{account}", {member: ts})
        self._expire(f"win:{account}")

    def trim(self, account: str, before: float) -> None:
        self.tx.zremrangebyscore(f"win:{account}", "-inf", f"({before}")

    def put_payment(self, payment_id: str, direction: Direction, entry: dict) -> None:
        key = f"pay:{payment_id}:{direction}"
        self.tx.hset(key, mapping={k: str(v) for k, v in entry.items()})
        self._expire(key)

    def put_token(self, pair: str, version: str, token: str, entry: dict) -> None:
        key = f"tok:{pair}:{version}:{token}"
        self.tx.hset(key, mapping={k: str(v) for k, v in entry.items()})
        self._expire(key)

    def add_link(self, incoming: str, outgoing: str) -> None:
        for key, member in (
            (f"link:in:{incoming}", outgoing),
            (f"link:out:{outgoing}", incoming),
        ):
            self.tx.sadd(key, member)
            self._expire(key)

    def put_hit(self, hit: Hit) -> None:
        # each hit belongs to one event, and a repeated event never gets this far
        key = f"hit:{hit['hit_id']}"
        payments = json.dumps(hit["payments"], separators=(",", ":"))
        self.tx.hset(
            key,
            mapping={
                "rule": hit["rule"],
                "account": hit["account"],
                "payments": payments,
            },
        )
        self._expire(key)
        self.tx.xadd("hits", {"hit": hit["hit_id"]})
        self._expire("hits")

    def last_paid(self, account: str, payee: str) -> tuple[float, bool] | None:
        value = self.db.get(f"last:{account}:{payee}")
        if value is None:
            return None
        ts, flagged = value.split(":")
        return float(ts), flagged == "1"

    def set_last_paid(self, account: str, payee: str, ts: float, flagged: bool) -> None:
        # no TTL: "never paid before" needs the whole history (streaming.md, Open)
        self.tx.set(f"last:{account}:{payee}", f"{ts:.0f}:{int(flagged)}")

    def commit(self) -> None:
        self.tx.execute()  # the pipeline empties itself, even on failure

    # read by the node and the offline evaluator

    def links(self, payment_id: str, direction: Direction) -> list[str]:
        return sorted(self.db.smembers(f"link:{direction}:{payment_id}"))

    def payment(self, payment_id: str, direction: Direction) -> dict | None:
        return self.db.hgetall(f"pay:{payment_id}:{direction}") or None

    def lookup(self, pair: str, version: str, token: str) -> dict | None:
        return self.db.hgetall(f"tok:{pair}:{version}:{token}") or None

    def hits(self) -> list[Hit]:
        hits = []
        for _, entry in self.db.xrange("hits"):
            h = self.db.hgetall(f"hit:{entry['hit']}")
            hits.append(
                {
                    "rule": h["rule"],
                    "account": h["account"],
                    "payments": json.loads(h["payments"]),
                    "hit_id": entry["hit"],
                }
            )
        return hits
