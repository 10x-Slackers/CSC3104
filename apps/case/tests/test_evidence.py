import pytest

from case.evidence import load_public_keys, verify_result
from privacy_node.claims import chain_digest
from sample_chains import m1, o1, public_keys
from security import public_key_pem

# DEPENDS ON Role 3 (privacy node): verification is its claims.valid_chain and
# chain_digest on its real ChainResults.


def test_real_results_verify_at_every_signing_bank():
    for result in (m1(), o1("B"), o1("C")):
        for bank in "ABC":
            assert verify_result(result, public_keys(), bank)


def altered(result):
    result.claims[2].out_ref.token = "ppmc:v1:k1:" + "0" * 64
    result.chain_digest = chain_digest(result.claims)


def forged(result):
    result.claims[0].signer = "C"  # C did not sign A's claim


def wrong_digest(result):
    result.chain_digest = "0" * 64


@pytest.mark.parametrize("tamper", [altered, forged, wrong_digest])
def test_tampered_result_never_becomes_evidence(tamper):
    result = m1()
    tamper(result)
    assert not verify_result(result, public_keys(), "B")


def test_unknown_signer_and_uninvolved_bank_rejected():
    keys = {bank: key for bank, key in public_keys().items() if bank != "A"}
    assert not verify_result(m1(), keys, "B")
    assert not verify_result(m1(), public_keys(), "D")


# DEPENDS ON Role 4 (security and privacy): keys by bank only, as Role 3's code does;
# security.md still says (bank, signing key version).
def test_public_keys_load_from_configmap_layout(tmp_path):
    for bank, key in public_keys().items():
        (tmp_path / f"{bank}.pem").write_bytes(public_key_pem(key))
    assert verify_result(m1(), load_public_keys(tmp_path), "B")
