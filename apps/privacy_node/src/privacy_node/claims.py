"""Canonical claim bytes and shared verification for both result receivers."""

import hashlib
from collections.abc import Iterable, Sequence
from itertools import pairwise

from google.protobuf.message import Message

from privacy_node import privacy_node_pb2 as pb
from security import PublicKeys, verify

# Per-message approved field names; descriptor tests require explicit approval of additions.
APPROVED_FIELDS: dict[str, set[str]] = {
    "TokenRef": {"pair", "token"},
    "TraceRequest": {
        "query_id",
        "sender",
        "key_version",
        "ref",
        "direction",
        "hops_remaining",
        "deadline",
        "visited",
    },
    "LinkClaim": {
        "signer",
        "recipient",
        "query_id",
        "key_version",
        "signing_key_version",
        "in_ref",
        "out_ref",
        "signature",
        "truncated",
    },
    "TraceReply": {"state", "claims"},
    "ChainResult": {"chain_digest", "sender", "state", "claims"},
    "Ack": {"chain_digest"},
}
ALLOWED_FIELDS = frozenset().union(*APPROVED_FIELDS.values())
PAIRS = {"A-B", "A-C", "B-C"}


def key(ref: pb.TokenRef) -> tuple[str, bytes] | None:
    """Hashable raw-token identity; None for a missing reference."""
    return (ref.pair, ref.token) if ref.token else None


def frame(parts: Iterable[bytes]) -> bytes:
    return b"".join(len(part).to_bytes(4, "big") + part for part in parts)


def claim_bytes(c: pb.LinkClaim) -> bytes:
    """Fixed order, 4-byte length-prefixed UTF-8 strings and raw token bytes."""
    text = (
        "csc3104.link-claim",
        "v1",
        c.signer,
        c.recipient,
        c.query_id,
        c.key_version,
        c.signing_key_version,
        c.in_ref.pair,
    )
    return frame(
        [
            *(part.encode("utf-8") for part in text),
            c.in_ref.token,
            c.out_ref.pair.encode("utf-8"),
            c.out_ref.token,
            b"1" if c.truncated else b"0",
        ]
    )


def _valid_ref(ref: pb.TokenRef) -> bool:
    return len(ref.token) == 16 and ref.pair in PAIRS if ref.token else not ref.pair


def valid_claim(c: pb.LinkClaim, public_keys: PublicKeys) -> bool:
    """Verify with a trusted versioned signing key and check token ownership."""
    public_key = public_keys.get((c.signer, c.signing_key_version))
    vouched = refs([c])
    return (
        public_key is not None
        and bool(vouched)
        and bool(c.query_id)
        and bool(c.key_version)
        and bool(c.signing_key_version)
        and all(_valid_ref(r) for r in (c.in_ref, c.out_ref))
        and all(c.signer in r.pair.split("-") for r in vouched)
        and (not c.in_ref.token or key(c.in_ref) != key(c.out_ref))
        and (
            c.recipient == c.signer
            or any(c.recipient in r.pair.split("-") for r in vouched)
        )
        and verify(claim_bytes(c), c.signature, public_key)
    )


def valid_chain(claims: Sequence[pb.LinkClaim], public_keys: PublicKeys) -> bool:
    """Each shared transfer is vouched for by its two distinct endpoint banks."""
    witnesses = {}
    for claim in claims:
        for ref in refs([claim]):
            witnesses.setdefault(key(ref), []).append(claim.signer)
    return (
        bool(claims)
        and all(valid_claim(c, public_keys) for c in claims)
        and all(len(banks) == 1 or len(set(banks)) == 2 for banks in witnesses.values())
        and all(
            bool(a.out_ref.token)
            and key(a.out_ref) == key(b.in_ref)
            and a.key_version == b.key_version
            and a.signer != b.signer
            for a, b in pairwise(claims)
        )
    )


def bound(claims: Sequence[pb.LinkClaim], recipient: str, query_id: str) -> bool:
    """Each signed claim targets its requester in one search leg."""
    recipients = [recipient] + [c.signer for c in claims[:-1]]
    return all(
        c.query_id == query_id and c.recipient == r
        for c, r in zip(claims, recipients, strict=False)
    )


def refs(claims: Iterable[pb.LinkClaim]) -> list[pb.TokenRef]:
    return [r for c in claims for r in (c.in_ref, c.out_ref) if r.token]


def complete(claims: Sequence[pb.LinkClaim]) -> bool:
    """Natural ends or a closed loop, never an unvisited token at a cut."""
    if not claims or any(c.truncated for c in claims):
        return False
    keys = [key(r) for r in refs(claims)]
    ends = key(claims[0].in_ref), key(claims[-1].out_ref)
    return all(end is None or keys.count(end) > 1 for end in ends)


def chain_digest(claims: Iterable[pb.LinkClaim]) -> str:
    """Hash distinct transfer identities in sorted order, using raw token bytes."""
    tokens = sorted({(r.pair, r.token) for r in refs(claims)})
    return hashlib.sha256(
        frame(
            [part for pair, token in tokens for part in (pair.encode("utf-8"), token)]
        )
    ).hexdigest()


def has_unknown_fields(message: Message) -> bool:
    """True if the message, or any message nested in it, carries fields our .proto doesn't define."""
    known = type(message)()
    known.CopyFrom(message)
    known.DiscardUnknownFields()  # recursive
    return known.ByteSize() != message.ByteSize()


def check_result(
    result: pb.ChainResult, public_keys: PublicKeys, bank: str
) -> pb.ChainResult:
    """Return a verified copy (cuts become PARTIAL), or raise ValueError.

    Shared by peer Deliver and bank-local case /results; neither consumer may
    store the unnormalised input after calling this function.
    """
    if has_unknown_fields(result):
        raise ValueError("unknown fields")
    claims, sender = list(result.claims), result.sender
    own = [i for i, c in enumerate(claims) if c.signer == c.recipient == sender]
    if (
        result.state not in (pb.CONFIRMED, pb.PARTIAL)
        or bank not in {c.signer for c in claims}
        or len(own) != 1
        or chain_digest(claims) != result.chain_digest
        or not valid_chain(claims, public_keys)
    ):
        raise ValueError("invalid chain result")
    up, down = claims[: own[0]][::-1], claims[own[0] + 1 :]
    if not all(bound(leg, sender, leg[0].query_id) for leg in (up, down) if leg):
        raise ValueError("invalid query or recipient binding")
    checked = pb.ChainResult()
    checked.CopyFrom(result)
    if not complete(claims):
        checked.state = pb.PARTIAL
    return checked
