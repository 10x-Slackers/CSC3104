import csv
import hashlib
import hmac
from pathlib import Path
from types import SimpleNamespace

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec

from security import (
    FORBIDDEN_FIELDS,
    canonical_pair,
    canonical_token_input,
    check_peer_payload,
    derive_pair_key,
    derive_token,
    generate_agreement_key,
    generate_signing_key,
    load_agreement_key,
    load_agreement_public_key,
    load_pairwise_keys,
    load_signing_key,
    load_verification_key,
    private_key_pem,
    public_key_pem,
    sign,
    verify,
)

TINY = Path(__file__).parents[2] / "dataset/fixtures/tiny"
# Predictable secrets belong only to tests; deployments generate independent keys.
PAIR_KEYS = {("A", "B"): b"A" * 32, ("A", "C"): b"B" * 32, ("B", "C"): b"C" * 32}
PRIVATE_FIELDS = {
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


def test_two_bank_fixture_tokens_match():
    views = {}
    for bank in ("A", "B"):
        with (TINY / f"bank_{bank}.csv").open(newline="", encoding="utf-8") as source:
            views[bank] = {row["payment_id"] for row in csv.DictReader(source)}
    shared = views["A"] & views["B"]
    assert {"p01", "p06", "p08", "p10", "p13"} <= shared
    for payment_id in shared:
        token_a = derive_token(payment_id, "A", "B", PAIR_KEYS["A", "B"], "k1")
        token_b = derive_token(payment_id, "B", "A", PAIR_KEYS["A", "B"], "k1")
        assert token_a == token_b


def test_frozen_token_vector():
    message = b'["csc3104.payment-token","v1",["A","B"],"k1","p01"]'
    digest = "86ef4e3f811c077dcde10d9cef4d807c1b039afda3d794d72174c5d4a92cb444"
    assert canonical_token_input("p01", "B", "A", "k1") == message
    assert hmac.new(PAIR_KEYS["A", "B"], message, hashlib.sha256).hexdigest() == digest
    token = derive_token("p01", "A", "B", PAIR_KEYS["A", "B"], "k1")
    assert isinstance(token, bytes) and len(token) == 16
    assert token == bytes.fromhex("86ef4e3f811c077dcde10d9cef4d807c")
    assert token == bytes.fromhex(digest)[:16]


def test_token_scopes_and_exact_identifiers():
    token = derive_token("p01", "A", "B", PAIR_KEYS["A", "B"], "k1")
    alternatives = [
        ("p06", "A", "B", PAIR_KEYS["A", "B"], "k1"),
        ("P01", "A", "B", PAIR_KEYS["A", "B"], "k1"),
        ("p01 ", "A", "B", PAIR_KEYS["A", "B"], "k1"),
        ("p01", "A", "C", PAIR_KEYS["A", "C"], "k1"),
        ("p01", "A", "B", PAIR_KEYS["A", "C"], "k1"),
        ("p01", "A", "B", PAIR_KEYS["A", "B"], "k2"),
        # The pair is also bound into the input if provisioning accidentally reuses a key.
        ("p01", "A", "C", PAIR_KEYS["A", "B"], "k1"),
    ]
    assert all(derive_token(*arguments) != token for arguments in alternatives)
    assert canonical_pair("B", "A") == ("A", "B")
    assert canonical_pair("a", "A") == ("A", "a")
    assert canonical_token_input("p\u00e9", "A", "B", "k1").endswith(b'"p\\u00e9"]')
    with pytest.raises(TypeError):
        derive_token(1, "A", "B", PAIR_KEYS["A", "B"], "k1")


@pytest.mark.parametrize("bank_a,bank_b", [("A", "A"), ("", "B"), ("A", "")])
def test_invalid_bank_pairs(bank_a, bank_b):
    with pytest.raises(ValueError):
        canonical_pair(bank_a, bank_b)


@pytest.mark.parametrize("version", ["", "v:1", "v 1", "v\u00e9", "v\n"])
def test_invalid_token_versions(version):
    with pytest.raises(ValueError):
        canonical_token_input("p01", "A", "B", version)


@pytest.mark.parametrize("key", [b"", b"short", b"x" * 31])
def test_short_hmac_keys_rejected(key):
    with pytest.raises(ValueError):
        derive_token("p01", "A", "B", key, "k1")


def test_x25519_pair_agreement_context_and_fresh_rotation():
    a, b, c = (generate_agreement_key() for _ in range(3))
    ab = derive_pair_key("A", "B", "k1", a, b.public_key())
    assert len(ab) == 32
    assert ab == derive_pair_key("B", "A", "k1", b, a.public_key())
    assert ab != derive_pair_key("A", "C", "k1", a, c.public_key())
    assert ab != derive_pair_key("A", "B", "k1", a, c.public_key())
    # Context includes the version, and actual rotation also creates fresh pairs.
    assert ab != derive_pair_key("A", "B", "k2", a, b.public_key())
    a2, b2 = generate_agreement_key(), generate_agreement_key()
    assert ab != derive_pair_key("A", "B", "k2", a2, b2.public_key())
    assert public_key_pem(a.public_key()) != public_key_pem(a2.public_key())


def test_mounted_x25519_keys_and_reload(tmp_path):
    a, b = generate_agreement_key(), generate_agreement_key()
    private, public = tmp_path / "local.pem", tmp_path / "peer.pem"
    private.write_bytes(private_key_pem(a))
    public.write_bytes(public_key_pem(b.public_key()))
    expected = derive_pair_key("A", "B", "k1", a, b.public_key())
    assert load_pairwise_keys("A", "k1", private, {"B": public}) == {
        ("A", "B"): expected
    }
    assert isinstance(load_agreement_key(private), type(a))
    assert public_key_pem(load_agreement_public_key(public)) == public.read_bytes()
    b2 = generate_agreement_key()
    public.write_bytes(public_key_pem(b2.public_key()))
    assert load_pairwise_keys("A", "k2", private, {"B": public})[("A", "B")] != expected
    # Signing material cannot accidentally be loaded for key agreement.
    private.write_bytes(private_key_pem(generate_signing_key()))
    with pytest.raises(TypeError):
        load_agreement_key(private)
    public.write_bytes(public_key_pem(generate_signing_key().public_key()))
    with pytest.raises(TypeError):
        load_agreement_public_key(public)


def test_signatures_authenticate_exact_bytes_and_signer():
    private = generate_signing_key()
    public = private.public_key()
    message = b"test-only claim bytes"
    signature = sign(message, private)
    assert len(signature) == 64
    assert sign(message, private) == signature
    assert verify(message, signature, public)
    assert not verify(message + b"!", signature, public)
    assert not verify(message, signature, generate_signing_key().public_key())
    assert not verify(message, signature[:-1] + bytes([signature[-1] ^ 1]), public)
    assert not verify(message, b"malformed", public)
    assert not verify(message, b"", public)
    assert not verify(message, signature.hex(), public)


def test_signing_key_pem_roundtrip(tmp_path):
    private = generate_signing_key()
    private_path = tmp_path / "signing.pem"
    public_path = tmp_path / "verification.pem"
    private_path.write_bytes(private_key_pem(private))
    public_path.write_bytes(public_key_pem(private.public_key()))
    assert private_path.read_bytes().startswith(b"-----BEGIN PRIVATE KEY-----")
    assert public_path.read_bytes().startswith(b"-----BEGIN PUBLIC KEY-----")
    loaded_private = load_signing_key(private_path)
    loaded_public = load_verification_key(public_path)
    message = b"mounted key roundtrip"
    assert sign(message, loaded_private) == sign(message, private)
    assert verify(message, sign(message, loaded_private), loaded_public)


def test_non_ed25519_pem_keys_rejected(tmp_path):
    private = ec.generate_private_key(ec.SECP256R1())
    private_path = tmp_path / "wrong-private.pem"
    public_path = tmp_path / "wrong-public.pem"
    private_path.write_bytes(
        private.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    public_path.write_bytes(
        private.public_key().public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
        )
    )
    with pytest.raises(TypeError):
        load_signing_key(private_path)
    with pytest.raises(TypeError):
        load_verification_key(public_path)


def test_nested_peer_allowlist():
    # A test policy only; Role 3 must supply each real message's approved fields.
    allowed = {"token", "claims", "signer_bank", "signature", "metadata", "query_id"}
    payload = {
        "token": "opaque-test-token",
        "claims": [{"signer_bank": "B", "signature": "test"}],
        "metadata": {"query_id": "q1"},
    }
    check_peer_payload(payload, allowed)
    with pytest.raises(ValueError):
        check_peer_payload({"metadata": {"unreviewed_field": "value"}}, allowed)
    for unchecked in ({"token"}, iter([payload]), SimpleNamespace(token="t")):
        with pytest.raises(TypeError):
            check_peer_payload(unchecked, allowed)


@pytest.mark.parametrize("field", sorted(PRIVATE_FIELDS))
def test_private_fields_rejected_even_if_caller_allows_them(field):
    assert field in FORBIDDEN_FIELDS
    with pytest.raises(ValueError):
        check_peer_payload(
            {"claims": [{field: "private test value"}]}, {"claims", field}
        )
