"""Role 4 primitives; protocol schemas and transport remain with their owners."""

import hashlib
import hmac
import json
import re
from collections.abc import Collection, Mapping
from pathlib import Path

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)

FORBIDDEN_FIELDS = frozenset(
    {
        "account",
        "account_number",
        "customer_id",
        "customer_identifier",
        "amount",
        "payment_id",
        "payment_record",
        "payment_records",
        "transaction_history",
        "transactions",
        "chain_id",
        "hop",
        "scenario",
        "altman_laundering",
    }
)


def canonical_pair(bank_a: str, bank_b: str) -> tuple[str, str]:
    """Sort exact, case-sensitive bank IDs; same-bank tokens are unsupported."""
    if not bank_a or not bank_b or bank_a == bank_b:
        raise ValueError("A token requires two distinct nonempty bank IDs")
    return tuple(sorted((bank_a, bank_b)))


def canonical_token_input(
    payment_id: str, bank_a: str, bank_b: str, key_version: str
) -> bytes:
    """Encode the domain, protocol, pair, key version and unchanged payment ID."""
    if not isinstance(payment_id, str):
        raise TypeError("Payment ID must be a string")
    if not re.fullmatch(r"[A-Za-z0-9_-]+", key_version):
        raise ValueError("Key version must use ASCII letters, digits, '_' or '-'")
    return json.dumps(
        [
            "csc3104.payment-token",
            "v1",
            list(canonical_pair(bank_a, bank_b)),
            key_version,
            payment_id,
        ],
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode("ascii")


def _check_hmac_key(key: bytes) -> None:
    if len(key) < 32:
        raise ValueError("Pairwise HMAC keys must contain at least 32 bytes")


def derive_token(
    payment_id: str, bank_a: str, bank_b: str, shared_key: bytes, key_version: str
) -> str:
    """Return a pair/version-scoped HMAC-SHA256 payment token."""
    _check_hmac_key(shared_key)
    message = canonical_token_input(payment_id, bank_a, bank_b, key_version)
    digest = hmac.new(shared_key, message, hashlib.sha256).hexdigest()
    return f"ppmc:v1:{key_version}:{digest}"


def load_pairwise_keys(
    bank_id: str, key_paths: Mapping[tuple[str, str], str | Path]
) -> dict[tuple[str, str], bytes]:
    """Read raw Secret-mounted files for this bank; reject shared pair keys."""
    keys = {}
    for pair, path in key_paths.items():
        pair = canonical_pair(*pair)
        if bank_id not in pair or pair in keys:
            raise ValueError("Pair is unrelated to this bank or supplied twice")
        key = Path(path).read_bytes()
        _check_hmac_key(key)
        if key in keys.values():
            raise ValueError("Different bank pairs must use independent keys")
        keys[pair] = key
    return keys


def generate_signing_key() -> Ed25519PrivateKey:
    return Ed25519PrivateKey.generate()


def private_key_pem(private_key: Ed25519PrivateKey) -> bytes:
    """Serialize for a protected Secret mount, without a second password layer."""
    return private_key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )


def public_key_pem(public_key: Ed25519PublicKey) -> bytes:
    return public_key.public_bytes(
        serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
    )


def load_signing_key(path: str | Path) -> Ed25519PrivateKey:
    key = serialization.load_pem_private_key(Path(path).read_bytes(), password=None)
    if not isinstance(key, Ed25519PrivateKey):
        raise TypeError("Expected an Ed25519 private key")
    return key


def load_verification_key(path: str | Path) -> Ed25519PublicKey:
    key = serialization.load_pem_public_key(Path(path).read_bytes())
    if not isinstance(key, Ed25519PublicKey):
        raise TypeError("Expected an Ed25519 public key")
    return key


def sign(message_bytes: bytes, private_key: Ed25519PrivateKey) -> bytes:
    """Sign exactly these bytes; the caller owns message/context encoding."""
    return private_key.sign(message_bytes)


def verify(
    message_bytes: bytes, signature: bytes, public_key: Ed25519PublicKey
) -> bool:
    """Return False for invalid signatures; callers must reject failed claims."""
    try:
        public_key.verify(signature, message_bytes)
    except (InvalidSignature, TypeError):
        return False
    return True


def check_peer_payload(payload: object, allowed_fields: Collection[str]) -> None:
    """Check dict/list field names recursively against a caller-reviewed policy.

    This is a field check, not a schema or a detector of private values hidden
    inside permitted fields. Role 3 must adapt and review its real serializers.
    """
    if isinstance(payload, dict):
        for field, value in payload.items():
            if field in FORBIDDEN_FIELDS or field not in allowed_fields:
                raise ValueError(f"Peer field is not permitted: {field}")
            check_peer_payload(value, allowed_fields)
    elif isinstance(payload, (list, tuple)):
        for value in payload:
            check_peer_payload(value, allowed_fields)
    elif not isinstance(payload, (str, bytes, int, float, type(None))):
        raise TypeError(f"Convert {type(payload).__name__} to dicts/lists first")
