"""Link claims: what a bank signs, and how any bank checks a chain of them.

A chain is nothing more than its signed claims. Its ID and its state (complete or cut)
are computed from the claims, so every bank that receives the same chain reaches the
same answers without coordinating. So is its tier, in each bank's case store.

Two patterns (docs/patterns.md): zero trust, because `valid_chain` checks every
signature and takes nothing a peer says on trust; and idempotency, because `digest`
gives the same chain the same ID at every bank, so a repeat is recognised.
"""

import hashlib
from itertools import pairwise

from mulechain import crypto
from mulechain.node import protocol_pb2 as pb


def claim_bytes(c: pb.LinkClaim) -> bytes:
    """The exact bytes a claim's signature covers: every field, length-prefixed."""
    parts = [b"mulechain claim", c.signer.encode(), c.in_from.encode(), c.in_token]
    parts += [c.out_to.encode(), c.out_token, b"cut" if c.cut else b""]
    return b"".join(len(part).to_bytes(2, "big") + part for part in parts)


def tokens(claims: list[pb.LinkClaim]) -> list[bytes]:
    return [t for c in claims for t in (c.in_token, c.out_token) if t]


def valid_chain(claims: list[pb.LinkClaim], keys: dict[str, list[crypto.PublicKey]]) -> bool:
    """Every claim is signed by the bank it names, and each transfer is confirmed from both ends.

    `keys` maps a bank to its published keys. Neighbouring claims must name each other
    and the same token: the sender's "I sent T to you" next to the receiver's "I got T
    from you".
    """
    return (
        bool(claims)
        and all(
            c.signer not in (c.in_from, c.out_to)
            and (c.in_token or c.out_token)
            and c.in_token != c.out_token
            and bool(c.in_token) == bool(c.in_from)
            and bool(c.out_token) == bool(c.out_to)
            and crypto.verify(keys.get(c.signer, []), claim_bytes(c), c.signature)
            for c in claims
        )
        and all(
            a.out_token
            and a.out_token == b.in_token
            and a.out_to == b.signer
            and b.in_from == a.signer
            for a, b in pairwise(claims)
        )
    )


def complete(claims: list[pb.LinkClaim]) -> bool:
    """True if the chain reached a natural end both ways (or closed a loop) and nobody cut it."""
    seen = tokens(claims)
    ends = (claims[0].in_token, claims[-1].out_token)
    return not any(c.cut for c in claims) and all(not end or seen.count(end) > 1 for end in ends)


def digest(claims: list[pb.LinkClaim]) -> str:
    """Content addressing: the same claims give the same chain ID, in whatever order."""
    return hashlib.sha256(b"".join(sorted({claim_bytes(c) for c in claims}))).hexdigest()
