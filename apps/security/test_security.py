import csv
import hashlib
import hmac
from pathlib import Path

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec
from security import (
    FORBIDDEN_FIELDS,
    canonical_pair,
    canonical_token_input,
    check_peer_payload,
    derive_token,
    generate_signing_key,
    load_pairwise_keys,
    load_signing_key,
    load_verification_key,
    private_key_pem,
    public_key_pem,
    sign,
    verify,
)

TINY = Path(__file__).parents[1] / "dataset/fixtures/tiny"
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
    assert (
        derive_token("p01", "A", "B", PAIR_KEYS["A", "B"], "k1")
        == f"ppmc:v1:k1:{digest}"
    )


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


def test_pairwise_mounted_keys_are_bank_scoped(tmp_path):
    paths = {}
    for pair, key in PAIR_KEYS.items():
        path = tmp_path / f"{pair[0]}-{pair[1]}.key"
        path.write_bytes(key)
        paths[pair] = path
    loaded = {
        bank: load_pairwise_keys(
            bank, {pair: path for pair, path in paths.items() if bank in pair}
        )
        for bank in "ABC"
    }
    assert set(loaded["A"]) == {("A", "B"), ("A", "C")}
    assert set(loaded["B"]) == {("A", "B"), ("B", "C")}
    assert set(loaded["C"]) == {("A", "C"), ("B", "C")}
    for pair, key in PAIR_KEYS.items():
        assert loaded[pair[0]][pair] == loaded[pair[1]][pair] == key
    with pytest.raises(ValueError):
        load_pairwise_keys("C", {("A", "B"): paths["A", "B"]})


def test_mounted_key_bytes_are_not_transformed(tmp_path):
    path = tmp_path / "pair.key"
    key = b"x" * 31 + b"\n"
    path.write_bytes(key)
    assert load_pairwise_keys("A", {("B", "A"): path}) == {("A", "B"): key}
    path.write_bytes(b"short")
    with pytest.raises(ValueError):
        load_pairwise_keys("A", {("A", "B"): path})


def test_duplicate_pairs_and_reused_keys_rejected(tmp_path):
    ab = tmp_path / "ab.key"
    ac = tmp_path / "ac.key"
    ab.write_bytes(PAIR_KEYS["A", "B"])
    ac.write_bytes(PAIR_KEYS["A", "B"])
    with pytest.raises(ValueError):
        load_pairwise_keys("A", {("A", "B"): ab, ("B", "A"): ac})
    with pytest.raises(ValueError):
        load_pairwise_keys("A", {("A", "B"): ab, ("A", "C"): ac})


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


@pytest.mark.parametrize("field", sorted(PRIVATE_FIELDS))
def test_private_fields_rejected_even_if_caller_allows_them(field):
    assert field in FORBIDDEN_FIELDS
    with pytest.raises(ValueError):
        check_peer_payload(
            {"claims": [{field: "private test value"}]}, {"claims", field}
        )
