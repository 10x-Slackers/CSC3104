import pytest

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


def test_public_keys_load_from_versioned_configmap_layout(tmp_path):
    for (bank, version), key in public_keys().items():
        folder = tmp_path / bank
        folder.mkdir(exist_ok=True)
        (folder / f"{version}.pem").write_bytes(public_key_pem(key))
    assert check_result(m1(), load_public_keys(tmp_path), "B")
