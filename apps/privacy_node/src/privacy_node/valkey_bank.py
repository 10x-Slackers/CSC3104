"""Bank-local view the privacy node reads from the scorer's Valkey (streaming.md).

The scorer writes `pay:`, `tok:`, `link:` and `hit:` keys and the `hits` stream; this
only reads them, so the node keeps no copy and starts with the bank's full state.
Intra-bank hops are followed when reading: a link to an intra-bank payment carries
on from that payment's other leg, up to MAX_INTERNAL_HOPS, until it reaches a
cross-bank payment. Nothing here is ever sent to a peer except tokens.
"""

import json
import socket
import threading

from valkey import Valkey
from valkey.exceptions import ResponseError

from ingestor import Direction
from privacy_node.local_state import LocalBank
from scorer.state import Hit, ValkeyState
from scorer.token_index import TokenIndex
from security import AgreementFiles, canonical_pair

MAX_INTERNAL_HOPS = 4
GROUP = "privacy-node"  # one consumer group, so replicas share the hits between them
CLAIM_MS = 60_000  # another consumer's hit unacknowledged this long: its pod is gone
OTHER: dict[Direction, Direction] = {"in": "out", "out": "in"}


class ValkeyBank:
    def __init__(
        self, db: Valkey, bank: str, key_version: str, key_paths: AgreementFiles
    ) -> None:
        """db needs decode_responses=True, as the scorer's client does."""
        self.db, self.bank, self.state = db, bank, ValkeyState(db)  # its readers only
        self.index = TokenIndex(bank, key_version, key_paths)  # keys only, no entries
        self.lock = threading.RLock()
        self.consumer = socket.gethostname()  # the pod name
        self._join()

    def _join(self) -> None:
        try:
            self.db.xgroup_create("hits", GROUP, id="0", mkstream=True)
        except ResponseError:  # BUSYGROUP: it already exists
            pass

    def peer(self, payment_id: str, direction: Direction) -> str:
        return self.db.hget(f"pay:{payment_id}:{direction}", "counterparty_bank")

    ref = LocalBank.ref  # uses only bank, peer and index

    def lookup(
        self, pair: str, key_version: str, token: bytes, direction: Direction
    ) -> str | None:
        """Return this bank's payment_id for a peer token, or None."""
        entry = self.state.lookup(pair, key_version, token.hex()) or {}
        return entry["payment_id"] if entry.get("direction") == direction else None

    def _walk(self, payment_id: str, direction: Direction) -> tuple[list[str], set]:
        """The cross-bank payments reached through intra-bank hops, and the payments
        walked through on the way (the start included)."""
        found, passed, todo = set(), set(), [(payment_id, 0)]
        while todo:
            pid, hops = todo.pop()
            passed.add(pid)
            for other in self.state.links(pid, direction):
                if self.peer(other, OTHER[direction]) != self.bank:
                    found.add(other)
                elif hops < MAX_INTERNAL_HOPS:
                    todo.append((other, hops + 1))  # the same payment's other leg
        return sorted(found), passed

    def links(self, payment_id: str, direction: Direction) -> list[str]:
        """Cross-bank payments a local rule links onward from an incoming payment ("in")
        or into an outgoing one ("out"), through any intra-bank hops."""
        return self._walk(payment_id, direction)[0]

    def ends(self, payment_id: str, direction: Direction) -> list[str]:
        """The payment itself if it crosses banks, else the cross-bank ones behind
        (incoming) or after (outgoing) its intra-bank hops."""
        if self.peer(payment_id, direction) != self.bank:
            return [payment_id]
        return self.links(payment_id, OTHER[direction])

    def internal_legs(self, incoming: str, outgoing: str) -> list[dict]:
        """A chain link's legs plus both legs of each intra-bank payment behind or
        after it, for a `local` message (case.md)."""
        legs = {(incoming, "in"), (outgoing, "out")}
        for p, d in list(legs):
            if self.peer(p, d) == self.bank:
                passed = self._walk(p, OTHER[d])[1]
                legs |= {(q, e) for q in passed for e in ("in", "out")}
        return [self.leg(p, d) for p, d in sorted(legs)]

    def leg(self, payment_id: str, direction: Direction, token: str = "") -> dict:
        """One of this bank's own payments, as the case service stores it (case.md)."""
        p = self.state.payment(payment_id, direction)
        leg = {
            "payment_id": payment_id,
            "direction": direction,
            "account": p["account"],
            "amount": float(p["amount"]),
            "pair": "",
            "token": "",
        }
        if p["counterparty_bank"] != self.bank:
            leg["pair"] = "-".join(canonical_pair(self.bank, p["counterparty_bank"]))
            leg["token"] = token or p["token"]
        return leg

    def _read(self, cursor: str, block_ms: int | None) -> list:
        reply = self.db.xreadgroup(
            GROUP, self.consumer, {"hits": cursor}, count=100, block=block_ms
        )
        return reply[0][1] if reply else []

    def new_hits(self, block_ms: int | None) -> list[tuple[str, Hit]]:
        """This consumer's unacknowledged hits first (left by an error), then those
        another consumer left for CLAIM_MS (a replaced pod), then new ones."""
        try:
            entries = (
                self._read("0", None)
                or self.db.xautoclaim(
                    "hits", GROUP, self.consumer, CLAIM_MS, count=100
                )[1]
                or self._read(">", block_ms)
            )
        except ResponseError:  # NOGROUP: Valkey restarted empty, and the scorer
            self._join()  # rebuilds it from Kafka. The caller retries.
            raise
        hits = []
        for entry_id, fields in entries:
            h = self.db.hgetall(f"hit:{fields['hit']}")  # {} once expired
            h["hit_id"] = fields["hit"]
            if h.get("payments"):
                h["payments"] = json.loads(h["payments"])
            hits.append((entry_id, h))
        return hits

    def ack(self, entry_id: str) -> None:
        self.db.xack("hits", GROUP, entry_id)
