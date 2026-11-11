import pytest
from fastapi.testclient import TestClient

from case.app import create_app
from case.evidence import check_result, load_public_keys, parse
from privacy_node.claims import chain_digest
from sample_chains import SAMPLE, chain, m1, o1, public_keys
from security import public_key_pem


def test_real_results_verify_at_every_signing_bank():
    for result in (m1(), o1("B"), o1("C")):
        for bank in "ABC":
            assert check_result(result, public_keys(), bank)


def altered(result):
    result.claims[2].out_ref.token = b"0" * 16
    result.chain_digest = chain_digest(result.claims)


def forged(result):
    result.claims[0].signer = "C"


def wrong_digest(result):
    result.chain_digest = "0" * 64


@pytest.mark.parametrize("tamper", [altered, forged, wrong_digest])
def test_tampered_result_never_becomes_evidence(tamper):
    result = m1()
    tamper(result)
    with pytest.raises(ValueError):
        check_result(result, public_keys(), "B")


def test_unknown_signer_and_uninvolved_bank_rejected():
    keys = {scope: key for scope, key in public_keys().items() if scope[0] != "A"}
    with pytest.raises(ValueError):
        check_result(m1(), keys, "B")
    with pytest.raises(ValueError):
        check_result(m1(), public_keys(), "D")


def test_public_keys_load_own_and_peer_versions_from_bank_key_directory(tmp_path):
    for (bank, version), key in public_keys().items():
        folder = (
            tmp_path / "B/public/signing"
            if bank == "B"
            else tmp_path / "B/trusted/signing" / bank
        )
        folder.mkdir(parents=True, exist_ok=True)
        (folder / f"{version}.pem").write_bytes(public_key_pem(key))
    keys = load_public_keys(tmp_path, "B")
    assert set(keys) == set(public_keys())
    assert check_result(m1(), keys, "B")
    client = TestClient(
        create_app(tmp_path / "cases.db", "B", lambda: load_public_keys(tmp_path, "B"))
    )
    result = chain(m1(), "B")
    assert client.post("/results", json=result).status_code == 200
    # The loader re-reads own public material; missing own keys cannot verify evidence.
    (tmp_path / "B/public/signing/k1.pem").unlink()
    assert client.post("/results", json=result).status_code == 422


@pytest.mark.parametrize(
    "edit",
    [
        lambda m: m.update(key="0" * 64),  # not the chain digest
        lambda m: m.update(kind="decision"),
        lambda m: m["result"].update(note="extra"),  # unknown field
        lambda m: m["result"].update(state="PENDING"),  # never evidence
        lambda m: m.pop("legs"),
    ],
    ids=["key", "kind", "unknown-field", "pending-state", "no-legs"],
)
def test_bad_message_is_rejected(edit):
    bad = chain(m1(), "B")
    edit(bad)
    with pytest.raises((KeyError, ValueError)):
        parse(bad, public_keys(), "B")


def test_sample_kinds_parse_without_a_signature():
    kinds = [parse(m, public_keys(), "B") for m in SAMPLE[1:]]
    assert [(m.kind, m.tier, m.detail) for m in kinds] == [
        ("pending", 1, "C"),
        ("hit", 1, "new_payee"),
        ("local", 1, ""),
    ]
