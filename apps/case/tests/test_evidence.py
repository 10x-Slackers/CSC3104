import pytest
from fastapi.testclient import TestClient

from case.app import create_app
from case.evidence import check_result, load_public_keys
from privacy_node.claims import chain_digest
from sample_chains import m1, o1, public_keys
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
        create_app(
            tmp_path / "cases.db",
            "B",
            "http://127.0.0.1:9",
            lambda: load_public_keys(tmp_path, "B"),
            interval=None,
        )
    )
    result = m1().SerializeToString()
    assert client.post("/results", content=result).status_code == 200
    # The loader re-reads own public material; missing own keys cannot verify evidence.
    (tmp_path / "B/public/signing/k1.pem").unlink()
    assert client.post("/results", content=result).status_code == 422
