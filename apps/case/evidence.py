"""Verify signed link claims before they reach the case store.

A claim that fails verification never becomes evidence: it is dropped and its result
is downgraded to invalid-claim. Signatures use Role 4's Ed25519 helpers.
"""

import json
from dataclasses import asdict, replace

from case_store import Claim, SearchResult

from apps.security.security import load_verification_key, sign, verify


def claim_bytes(claim):
    """Canonical signed bytes: domain, version, then every claim field but the signature.

    ASSUMPTION: claim encoding until protocol.md defines it, replace when agreed.
    """
    fields = asdict(claim)
    del fields["signature"]
    return json.dumps(
        ["csc3104.link-claim", "v1", fields],
        sort_keys=True,
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode("ascii")


def sign_claim(claim, private_key):
    """Reference signer for tests and the privacy node; returns the claim, signed."""
    return replace(claim, signature=sign(claim_bytes(claim), private_key).hex())


def load_public_keys(paths):
    """Trusted verification keys from config: {(bank, key_version): PEM path}."""
    return {scope: load_verification_key(path) for scope, path in paths.items()}


def valid(claim: Claim, query_id, public_keys):
    """Trust only configured keys; the signer must sit between two other banks."""
    key = public_keys.get((claim.signer, claim.key_version))
    if (
        key is None
        or claim.query_id != query_id
        or claim.signer in (claim.in_bank, claim.out_bank)
    ):
        return False
    try:
        return verify(claim_bytes(claim), bytes.fromhex(claim.signature), key)
    except ValueError:
        return False


def verify_result(result: SearchResult, public_keys):
    """Keep only valid claims; any invalid claim downgrades the result to invalid-claim."""
    claims = tuple(c for c in result.claims if valid(c, result.query_id, public_keys))
    if len(claims) == len(result.claims):
        return result
    return SearchResult(result.query_id, "invalid-claim", claims)
