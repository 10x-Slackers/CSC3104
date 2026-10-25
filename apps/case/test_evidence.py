import sys
from dataclasses import replace
from pathlib import Path

# The pytest executable does not add the repository root for namespace imports.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pytest
from case_store import SearchResult, SqliteCaseStore
from evidence import claim_bytes, load_public_keys, sign_claim, verify_result
from test_case_store import claim, result

from apps.security.security import generate_signing_key, public_key_pem

SIGNING = {bank: generate_signing_key() for bank in "ABC"}
PUBLIC = {(bank, "s1"): key.public_key() for bank, key in SIGNING.items()}


def signed(incoming, outgoing, query="q1"):
    c = claim(incoming, outgoing, query)
    return sign_claim(c, SIGNING[c.signer])


def test_valid_claims_pass_unchanged():
    r = result(signed("p10", "p11"), signed("p11", "p12"))
    assert verify_result(r, PUBLIC) == r


def test_claim_bytes_cover_every_field_but_signature():
    c = signed("p06", "p07")
    assert claim_bytes(c) == claim_bytes(replace(c, signature="ff"))
    for field in ("signer", "in_bank", "in_token", "out_bank", "out_token", "query_id"):
        assert claim_bytes(c) != claim_bytes(replace(c, **{field: "x"}))
    assert claim_bytes(c) != claim_bytes(replace(c, key_version="s2"))


@pytest.mark.parametrize(
    "tamper",
    [
        lambda c: replace(c, out_token=claim("p10", "p11").out_token),  # altered
        lambda c: sign_claim(c, SIGNING["A"]),  # wrong signer's key
        lambda c: replace(c, key_version="s9"),  # unknown key version
        lambda c: replace(c, signature="zz"),  # malformed signature
        lambda c: sign_claim(replace(c, query_id="other"), SIGNING["B"]),  # replayed
        lambda c: sign_claim(replace(c, out_bank="B"), SIGNING["B"]),  # not a link
    ],
)
def test_invalid_claim_never_becomes_evidence(tmp_path, tamper):
    store = SqliteCaseStore(tmp_path / "cases.db")
    r = SearchResult("q1", "confirmed", (tamper(signed("p06", "p07")),))
    r = verify_result(r, PUBLIC)
    assert r == SearchResult("q1", "invalid-claim")
    assert store.record(r) is None
    assert store.cases() == []


def test_valid_rest_of_invalid_result_is_kept_but_not_confirmed(tmp_path):
    store = SqliteCaseStore(tmp_path / "cases.db")
    bad = replace(signed("p11", "p12"), signature="00" * 64)
    r = verify_result(result(signed("p10", "p11"), bad), PUBLIC)
    assert r.status == "invalid-claim" and r.claims == (signed("p10", "p11"),)
    case = store.get(store.record(r))
    assert case["state"] == "invalid-claim" and case["raised_at"] is None


def test_public_keys_load_from_pem(tmp_path):
    path = tmp_path / "B-s1.pem"
    path.write_bytes(public_key_pem(PUBLIC["B", "s1"]))
    keys = load_public_keys({("B", "s1"): path})
    r = result(signed("p06", "p07"))
    assert verify_result(r, keys) == r
