"""Verify a delivered ChainResult before it reaches the case store.

Mirrors the privacy node's own Deliver check (apps/privacy_node/node.py): this bank
signed part of the chain, the digest matches the claims, and every claim verifies
with Role 3's claims.valid_chain. A failed result never becomes evidence.
"""

from pathlib import Path

from apps.privacy_node.claims import chain_digest, valid_chain
from apps.security.security import load_verification_key


def load_public_keys(folder, banks="ABC"):
    """Trusted keys from the bank-public-keys ConfigMap layout: <folder>/<bank>.pem.

    DEPENDS ON Role 4 (security and privacy): Role 3 keys verifiers by bank only
    (claims.valid_claim), while security.md keys them by (bank, signing key version);
    this follows Role 3's code. Status: pending.
    """
    return {bank: load_verification_key(Path(folder) / f"{bank}.pem") for bank in banks}


def verify_result(result, public_keys, bank):
    return (
        bank in {c.signer for c in result.claims}
        and chain_digest(result.claims) == result.chain_digest
        and valid_chain(result.claims, public_keys)
    )
