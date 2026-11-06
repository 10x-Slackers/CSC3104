"""Role 4 primitives; protocol schemas and transport remain with their owners."""

import hashlib
import hmac
import json
import re
from collections.abc import Collection, Mapping
from dataclasses import dataclass
from pathlib import Path

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)
from cryptography.hazmat.primitives.asymmetric.x25519 import (
    X25519PrivateKey,
    X25519PublicKey,
)
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

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


def _check_key_version(key_version: str) -> None:
    if not re.fullmatch(r"[A-Za-z0-9_-]+", key_version):
        raise ValueError("Key version must use ASCII letters, digits, '_' or '-'")


def canonical_token_input(
    payment_id: str, bank_a: str, bank_b: str, key_version: str
) -> bytes:
    """Encode the domain, protocol, pair, key version and unchanged payment ID."""
    if not isinstance(payment_id, str):
        raise TypeError("Payment ID must be a string")
    _check_key_version(key_version)
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
) -> bytes:
    """Return the leftmost 16 raw bytes of the pair/version-scoped HMAC."""
    _check_hmac_key(shared_key)
    message = canonical_token_input(payment_id, bank_a, bank_b, key_version)
    return hmac.new(shared_key, message, hashlib.sha256).digest()[:16]


@dataclass(frozen=True)
class AgreementFiles:
    """Only this bank's private key and trusted peers' public keys are mounted."""

    private_key: str | Path
    peer_public_keys: Mapping[str, str | Path]


def generate_agreement_key() -> X25519PrivateKey:
    return X25519PrivateKey.generate()


def load_agreement_key(path: str | Path) -> X25519PrivateKey:
    key = serialization.load_pem_private_key(Path(path).read_bytes(), password=None)
    if not isinstance(key, X25519PrivateKey):
        raise TypeError("Expected an X25519 private key")
    return key


def load_agreement_public_key(path: str | Path) -> X25519PublicKey:
    key = serialization.load_pem_public_key(Path(path).read_bytes())
    if not isinstance(key, X25519PublicKey):
        raise TypeError("Expected an X25519 public key")
    return key


def derive_pair_key(
    bank_id: str,
    peer: str,
    key_version: str,
    local_private_key: X25519PrivateKey,
    peer_public_key: X25519PublicKey,
) -> bytes:
    pair = canonical_pair(bank_id, peer)
    _check_key_version(key_version)
    return HKDF(
        algorithm=hashes.SHA256(),
        length=32,
        salt=None,
        info=f"csc3104 pair-key {'-'.join(pair)} {key_version}".encode("ascii"),
    ).derive(local_private_key.exchange(peer_public_key))


def load_pairwise_keys(
    bank_id: str,
    key_version: str,
    local_private_key: str | Path,
    peer_public_keys: Mapping[str, str | Path],
) -> dict[tuple[str, str], bytes]:
    """Re-read mounted X25519 material and derive, never transport, pair keys."""
    private = load_agreement_key(local_private_key)
    return {
        canonical_pair(bank_id, peer): derive_pair_key(
            bank_id, peer, key_version, private, load_agreement_public_key(path)
        )
        for peer, path in peer_public_keys.items()
    }


def generate_signing_key() -> Ed25519PrivateKey:
    return Ed25519PrivateKey.generate()


def private_key_pem(private_key: Ed25519PrivateKey | X25519PrivateKey) -> bytes:
    """Serialize for a protected Secret mount, without a second password layer."""
    return private_key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )


def public_key_pem(public_key: Ed25519PublicKey | X25519PublicKey) -> bytes:
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


def load_verification_keys(
    key_paths: Mapping[tuple[str, str], str | Path],
) -> dict[tuple[str, str], Ed25519PublicKey]:
    """Trusted signing keys are addressed by (bank, signing key version)."""
    return {
        identity: load_verification_key(path) for identity, path in key_paths.items()
    }


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
