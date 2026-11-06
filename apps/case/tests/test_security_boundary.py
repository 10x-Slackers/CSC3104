"""Shared result validation and byte-token storage at the case receiver."""

import pytest
from fastapi.testclient import TestClient

from case.app import create_app
from case.case_store import SqliteCaseStore
from case.evidence import check_result
from case.report import report_input
from privacy_node import privacy_node_pb2 as pb
from privacy_node.claims import chain_digest, claim_bytes
from security import generate_signing_key, sign


@pytest.fixture
def evidence():
    private = {bank: generate_signing_key() for bank in "ABC"}
    ab, bc = (
        pb.TokenRef(pair="A-B", token=b"a" * 16),
        pb.TokenRef(pair="B-C", token=b"b" * 16),
    )
    claims = [
        pb.LinkClaim(signer="A", recipient="B", query_id="up", out_ref=ab),
        pb.LinkClaim(
            signer="B", recipient="B", query_id="anchor", in_ref=ab, out_ref=bc
        ),
        pb.LinkClaim(signer="C", recipient="B", query_id="down", in_ref=bc),
    ]
    for claim in claims:
        claim.key_version = claim.signing_key_version = "k1"
        claim.signature = sign(claim_bytes(claim), private[claim.signer])
    result = pb.ChainResult(
        sender="B", state=pb.CONFIRMED, claims=claims, chain_digest=chain_digest(claims)
    )
    keys = {(bank, "k1"): key.public_key() for bank, key in private.items()}
    return result, keys, private


def client_for(tmp_path, keys):
    return TestClient(
        create_app(
            tmp_path / "cases.db",
            "B",
            "http://127.0.0.1:9",
            lambda: keys,
            interval=None,
        )
    )


def test_case_calls_the_authoritative_validator(evidence, tmp_path):
    result, keys, _ = evidence
    from privacy_node.claims import check_result as authoritative

    assert check_result is authoritative
    client = client_for(tmp_path, keys)
    assert (
        client.post("/results", content=result.SerializeToString()).status_code == 200
    )
    result.state = pb.PARTIAL
    assert (
        client.post("/results", content=result.SerializeToString()).status_code == 200
    )


def test_signed_cut_falsely_confirmed_is_stored_only_as_partial(evidence, tmp_path):
    result, keys, private = evidence
    result.claims[1].truncated = True
    result.claims[1].signature = sign(claim_bytes(result.claims[1]), private["B"])
    client = client_for(tmp_path, keys)
    assert (
        client.post("/results", content=result.SerializeToString()).status_code == 200
    )
    [case] = client.get("/api/cases").json()
    assert case["state"] == "partial" and case["raised_at"] is None


def test_tokens_normalize_to_hex_at_case_storage_boundary(evidence, tmp_path):
    result, keys, _ = evidence
    client = client_for(tmp_path, keys)
    assert (
        client.post("/results", content=result.SerializeToString()).status_code == 200
    )
    store = SqliteCaseStore(tmp_path / "cases.db")
    case = store.get(result.chain_digest)
    assert case["claims"][1].in_token == (b"a" * 16).hex()
    assert (
        store.db.execute(
            "SELECT typeof(in_token) FROM claims WHERE signer='B'"
        ).fetchone()[0]
        == "text"
    )


def test_signed_cut_flags_remain_booleans_in_store_api_and_report(evidence, tmp_path):
    result, keys, private = evidence
    result.claims[1].truncated = True
    result.claims[1].signature = sign(claim_bytes(result.claims[1]), private["B"])
    client = client_for(tmp_path, keys)
    assert (
        client.post("/results", content=result.SerializeToString()).status_code == 200
    )
    case = SqliteCaseStore(tmp_path / "cases.db").get(result.chain_digest)
    assert [claim.truncated for claim in case["claims"]] == [False, True, False]
    assert all(type(claim.truncated) is bool for claim in case["claims"])
    response = client.get(f"/api/cases/{result.chain_digest}").json()
    assert [claim["truncated"] for claim in response["claims"]] == [False, True, False]
    assert [link["truncated"] for link in report_input(case)["links"]] == [
        False,
        True,
        False,
    ]
