"""Keys, tokens and signatures: all the cryptography in one place.

Key agreement with no dealer. Each bank makes its own private keys and never shares
them. It publishes only public keys (the privacy node's `Keys` call, over mTLS), and
any two banks then derive the same pair key on their own:

    pair key = HKDF(X25519(my private key, your public key))

A transfer's token is an HMAC of its payment ID under that pair key, so the two banks
on a transfer compute the same token and no third bank can.

Rotation needs no coordination either. A bank lists its keys oldest first. It indexes
each transfer under every (own key, peer key) pair, and names it to the peer with its
own oldest key and the peer's newest. So a bank adds a new key, waits until its peers
have fetched it, and then removes the old one: at every step each side asks with a
token the other side has indexed.
"""

import hashlib
import hmac
from pathlib import Path

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey
from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey, X25519PublicKey
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

RAW = (Encoding.Raw, PublicFormat.Raw)


class PrivateKey:
    """One key version, from 64 random bytes: 32 for key agreement, 32 for signing."""

    def __init__(self, seed: bytes) -> None:
        self.agreement = X25519PrivateKey.from_private_bytes(seed[:32])
        self.signing = Ed25519PrivateKey.from_private_bytes(seed[32:64])

    @property
    def public(self) -> "PublicKey":
        return PublicKey(
            self.agreement.public_key().public_bytes(*RAW),
            self.signing.public_key().public_bytes(*RAW),
        )


class PublicKey:
    """What a bank publishes for one key version: 32 + 32 raw bytes."""

    def __init__(self, agreement: bytes, signing: bytes) -> None:
        self.agreement, self.signing = agreement, signing


def load_keys(directory: str | Path) -> list[PrivateKey]:
    """A bank's key versions, oldest first: the files `1`, `2`, ... of its mounted Secret."""
    files = [p for p in Path(directory).iterdir() if p.name.isdigit()]
    return [PrivateKey(p.read_bytes()) for p in sorted(files, key=lambda p: int(p.name))]


def encode(keys: list[PublicKey]) -> str:
    """Public keys as text, for the stores that pass them from the node to the scorer."""
    return ",".join((k.agreement + k.signing).hex() for k in keys)


def decode(text: str) -> list[PublicKey]:
    raw = [bytes.fromhex(k) for k in text.split(",") if k]
    return [PublicKey(k[:32], k[32:]) for k in raw]


def pair_keys(bank: str, peer: str, own: list[PrivateKey], theirs: list[PublicKey]) -> list[bytes]:
    """The keys two banks share, one per (own key, peer key).

    The first is the one to query with: own oldest key, peer's newest.
    """
    info = b"mulechain pair key " + "-".join(sorted((bank, peer))).encode()
    return [
        HKDF(hashes.SHA256(), 32, salt=None, info=info).derive(
            mine.agreement.exchange(X25519PublicKey.from_public_bytes(yours.agreement))
        )
        for mine in own
        for yours in reversed(theirs)
    ]


def token(pair_key: bytes, payment_id: str) -> bytes:
    """A transfer's 16-byte token: only the two banks holding `pair_key` can derive it."""
    message = b"mulechain token " + payment_id.encode()
    return hmac.new(pair_key, message, hashlib.sha256).digest()[:16]


def sign(key: PrivateKey, message: bytes) -> bytes:
    return key.signing.sign(message)


def verify(keys: list[PublicKey], message: bytes, signature: bytes) -> bool:
    """True if any of the signer's published keys made this signature."""
    for key in keys:
        try:
            Ed25519PublicKey.from_public_bytes(key.signing).verify(signature, message)
            return True
        except InvalidSignature:
            continue
    return False
