"""What a privacy node knows about its own bank: the scorer's state in Valkey.

The node keeps no copy, so it starts with the bank's full state and a restart loses
nothing. Payment IDs, accounts and amounts read here stay inside the bank: only
tokens go to peers, and own-leg details go to this bank's case service. The one
thing the node writes is `keys`, the peers' public keys the scorer needs.

A transfer between two accounts of this bank has no token and no peer to ask, so the
node walks through such intra-bank hops until it reaches a payment that crosses banks.
"""

from valkey import Valkey

MAX_INTERNAL_HOPS = 4
OTHER = {"in": "out", "out": "in"}


class Bank:
    def __init__(self, name: str, db: Valkey) -> None:
        """`db` needs decode_responses=True, as the scorer's client does."""
        self.name, self.db = name, db

    def hits(self, after: str, block_ms: int | None) -> list[tuple[str, dict]]:
        """The scorer's rule hits after stream position `after`, as (position, hit)."""
        reply = self.db.xread({"hits": after}, count=500, block=block_ms)
        return reply[0][1] if reply else []

    def publish_keys(self, keys: dict[str, str]) -> None:
        """Give the scorer each peer's public keys, to derive its pair keys from."""
        self.db.hset("keys", mapping=keys)

    def peer(self, payment: str, direction: str) -> str:
        """The bank on the other side of one of this bank's payments."""
        return self.db.hget(f"pay:{payment}:{direction}", "counterparty_bank")

    def token(self, payment: str, direction: str) -> bytes:
        return bytes.fromhex(self.db.hget(f"pay:{payment}:{direction}", "token"))

    def held(self, token: bytes) -> tuple[str, str]:
        """(payment, direction) of this bank's payment behind a token; empty strings if none."""
        payment, _, direction = (self.db.get(f"tok:{token.hex()}") or "").rpartition(":")
        return payment, direction

    def lookup(self, token: bytes, direction: str, caller: str) -> str | None:
        """This bank's payment behind a token, if `caller` is the bank on its other side.

        Tokens travel in claims, so a third bank can learn one. Only the transfer's own
        counterparty may ask about it.
        """
        payment, held = self.held(token)
        if held == direction and self.peer(payment, direction) == caller:
            return payment
        return None

    def _walk(self, payment: str, direction: str) -> tuple[list[str], set[str]]:
        """Cross-bank payments reached from `payment` through intra-bank hops, and the
        payments passed on the way (the start included)."""
        found, passed, todo = set(), set(), [(payment, 0)]
        while todo:
            current, hops = todo.pop()
            passed.add(current)
            for linked in self.db.smembers(f"link:{direction}:{current}"):
                if self.peer(linked, OTHER[direction]) != self.name:
                    found.add(linked)
                elif hops < MAX_INTERNAL_HOPS:
                    todo.append((linked, hops + 1))  # the same payment's other leg
        return sorted(found), passed

    def links(self, payment: str, direction: str) -> list[str]:
        """Cross-bank payments the rules link onward from an incoming payment ("in"), or
        into an outgoing one ("out")."""
        return self._walk(payment, direction)[0]

    def ends(self, payment: str, direction: str) -> list[str]:
        """The payment itself if it crosses banks, else the cross-bank payments behind
        (for "in") or after (for "out") its intra-bank hops."""
        if self.peer(payment, direction) != self.name:
            return [payment]
        return self.links(payment, OTHER[direction])

    def leg(self, payment: str, direction: str) -> dict:
        """One of this bank's own payments, as its case service shows it."""
        p = self.db.hgetall(f"pay:{payment}:{direction}")
        peer = p["counterparty_bank"] if p["counterparty_bank"] != self.name else ""
        return {
            "payment_id": payment,
            "direction": direction,
            "account": p["account"],
            "amount": float(p["amount"]),
            "peer": peer,  # empty for a transfer inside this bank
            "token": p.get("token", ""),
        }

    def internal_legs(self, incoming: str, outgoing: str) -> list[dict]:
        """A link's two legs plus both legs of every intra-bank payment around it."""
        legs = {(incoming, "in"), (outgoing, "out")}
        for payment, direction in list(legs):
            if self.peer(payment, direction) == self.name:
                passed = self._walk(payment, OTHER[direction])[1]
                legs |= {(p, d) for p in passed for d in ("in", "out")}
        return [self.leg(p, d) for p, d in sorted(legs) if self.db.exists(f"pay:{p}:{d}")]
