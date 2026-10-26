"""Canonical bytes, signing and verification for link claims (security.md)."""

import hashlib
import json
from itertools import pairwise

from google.protobuf.json_format import MessageToDict

from apps.privacy_node import privacy_node_pb2 as pb
from apps.security.security import check_peer_payload, sign, verify

# Every field name that may appear in a serialized peer message. Caleb reviews this.
ALLOWED_FIELDS = frozenset(
    {
        *("query_id", "sender", "key_version", "ref", "direction", "hops_remaining"),
        *("deadline", "visited", "pair", "token", "state", "claims", "signer"),
        *("recipient", "in_ref", "out_ref", "signature", "chain_digest"),
    }
)


def key(ref):
    """Hashable form of a TokenRef; empty for a missing ref."""
    return f"{ref.pair}|{ref.token}" if ref.token else ""


def claim_bytes(c):
    return json.dumps(
        ["csc3104.link-claim", "v1", c.signer, c.recipient, c.query_id, c.key_version]
        + [c.in_ref.pair, c.in_ref.token, c.out_ref.pair, c.out_ref.token],
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode("ascii")


def make_claim(bank, private_key, recipient, query_id, key_version, in_ref, out_ref):
    c = pb.LinkClaim(
        signer=bank,
        recipient=recipient,
        query_id=query_id,
        key_version=key_version,
        in_ref=in_ref,
        out_ref=out_ref,
    )
    c.signature = sign(claim_bytes(c), private_key)
    return c


def valid_claim(c, public_keys):
    """Signature checks out and the signer is an endpoint of every ref it vouches for."""
    refs = [r for r in (c.in_ref, c.out_ref) if r.token]
    return (
        c.signer in public_keys
        and bool(refs)
        and all(c.signer in r.pair.split("-") for r in refs)
        and verify(claim_bytes(c), c.signature, public_keys[c.signer])
    )


def valid_chain(claims, public_keys):
    """Claims in money-flow order: each valid, each out_ref is the next claim's in_ref."""
    return all(valid_claim(c, public_keys) for c in claims) and all(
        key(a.out_ref) == key(b.in_ref) for a, b in pairwise(claims)
    )


def bound(claims, recipient, query_id):
    """Each claim was signed for the bank that asked for it, all in one query."""
    recipients = [recipient] + [c.signer for c in claims[:-1]]
    return all(
        c.query_id == query_id and c.recipient == r for c, r in zip(claims, recipients)
    )


def complete(claims):
    """Both chain ends are a natural start/end or close a loop, not a cut."""
    refs = [key(r) for c in claims for r in (c.in_ref, c.out_ref)]
    ends = key(claims[0].in_ref), key(claims[-1].out_ref)
    return all(end == "" or refs.count(end) > 1 for end in ends)


def chain_digest(claims):
    """Canonical chain identity: hash of the sorted transfer tokens it covers.

    Searches started at different banks on the same transfers get the same ID.
    """
    refs = sorted({key(r) for c in claims for r in (c.in_ref, c.out_ref)} - {""})
    return hashlib.sha256(json.dumps(refs).encode()).hexdigest()


def check_payload(message):
    """Raise ValueError if a serialized peer message carries a field not on the allowlist."""
    check_peer_payload(
        MessageToDict(message, preserving_proto_field_name=True), ALLOWED_FIELDS
    )
