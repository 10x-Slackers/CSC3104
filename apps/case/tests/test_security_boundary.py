"""Case receiver attacks and the own-bank enrichment/report boundary."""

import json
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from case.app import create_app
from case.case_store import SqliteCaseStore
from case.evidence import check_result
from case.report import TemplateClient, report_input
from privacy_node import privacy_node_pb2 as pb
from privacy_node.case_delivery import own_case_legs
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
            tmp_path / "cases.db", "B", "http://127.0.0.1:9", keys, interval=None
        )
    )


def post_enriched(client, result, legs):
    return client.post(
        "/results",
        files={
            "result": (
                "result.pb",
                result.SerializeToString(),
                "application/x-protobuf",
            )
        },
        data={"own_legs": json.dumps(legs)},
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


@pytest.mark.parametrize(
    "attack",
    [
        "tamper",
        "signer",
        "missing_key",
        "digest",
        "anchor",
        "binding",
        "signing_version",
        "unknown",
        "state",
    ],
)
def test_invalid_results_never_enter_case_store(evidence, tmp_path, attack):
    result, keys, private = evidence
    if attack == "tamper":
        result.claims[0].out_ref.token = b"x" * 16
    elif attack == "signer":
        result.claims[0].signer = "C"
    elif attack == "missing_key":
        del keys["A", "k1"]
    elif attack == "digest":
        result.chain_digest = "bad"
    elif attack == "anchor":
        result.sender = "A"
    elif attack == "binding":
        result.claims[0].recipient = "C"
        result.claims[0].signature = sign(claim_bytes(result.claims[0]), private["A"])
    elif attack == "signing_version":
        result.claims[0].signing_key_version = "k2"
    elif attack == "state":
        result.state = pb.PENDING
    payload = result.SerializeToString() + (
        b"\x98\x06\x01" if attack == "unknown" else b""
    )
    client = client_for(tmp_path, keys)
    assert client.post("/results", content=payload).status_code == 422
    assert client.get("/api/cases").json() == []


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


def local_leg(bank="B", claim_index=1):
    return {
        "bank": bank,
        "claim_index": claim_index,
        "direction": "in",
        "payment_id": "own-payment",
        "account": "own-account",
        "amount": 123.5,
    }


def test_own_bank_enrichment_is_internal_and_allowed_in_analyst_report(
    evidence, tmp_path
):
    result, keys, _ = evidence
    client = client_for(tmp_path, keys)
    assert post_enriched(client, result, [local_leg()]).status_code == 200
    store = SqliteCaseStore(tmp_path / "cases.db")
    case = store.get(result.chain_digest)
    assert case["own_legs"][0]["account"] == "own-account"
    assert case["claims"][1].in_token == b"a" * 16
    assert (
        store.db.execute(
            "SELECT typeof(in_token) FROM claims WHERE signer='B'"
        ).fetchone()[0]
        == "blob"
    )
    report = report_input(case, "B")
    assert report["own_legs"][0]["payment_id"] == "own-payment"
    assert "own-account" in TemplateClient().draft(report)
    assert "own-account" in client.get(f"/cases/{result.chain_digest}").text
    assert "own_legs" not in report_input(case)  # legacy mock intake is minimal
    assert "own-payment" not in str(result)  # enrichment never mutated peer evidence


@pytest.mark.parametrize(
    "leg",
    [local_leg("A", 0), local_leg("B", 0), local_leg() | {"customer_id": "foreign"}],
)
def test_other_banks_raw_legs_and_unapproved_fields_are_rejected(
    evidence, tmp_path, leg
):
    result, keys, _ = evidence
    client = client_for(tmp_path, keys)
    assert post_enriched(client, result, [leg]).status_code == 422
    assert client.get("/api/cases").json() == []


def test_report_rejects_foreign_raw_details(evidence, tmp_path):
    result, _, _ = evidence
    store = SqliteCaseStore(tmp_path / "cases.db")
    case = store.get(store.record(result))
    case["own_legs"] = [local_leg("A", 0)]
    with pytest.raises(ValueError, match="own bank"):
        report_input(case, "B")


def test_node_case_producer_selects_only_own_signed_legs(evidence):
    result, _, _ = evidence
    local = SimpleNamespace(
        bank="B",
        lookup=lambda pair, version, token, direction: direction,
        records={
            (direction, direction): {
                "payment_id": f"B-{direction}",
                "account": "B-local",
                "amount": 10,
            }
            for direction in ("in", "out")
        },
    )
    legs = own_case_legs(result, local)
    assert len(legs) == 2
    assert all(leg["bank"] == "B" and leg["claim_index"] == 1 for leg in legs)
    assert {leg["payment_id"] for leg in legs} == {"B-in", "B-out"}


def test_case_rejects_disconnected_genuinely_signed_fragments(evidence, tmp_path):
    result, keys, private = evidence
    split_ref = pb.TokenRef()
    split_ref.CopyFrom(result.claims[1].out_ref)
    result.claims[1].ClearField("out_ref")
    tail = result.claims[2]
    tail.ClearField("in_ref")
    tail.out_ref.CopyFrom(split_ref)
    result.claims.append(
        pb.LinkClaim(
            signer="B",
            recipient="C",
            query_id=tail.query_id,
            key_version="k1",
            signing_key_version="k1",
            in_ref=split_ref,
        )
    )
    for claim in result.claims:
        claim.signature = sign(claim_bytes(claim), private[claim.signer])
    result.chain_digest = chain_digest(result.claims)
    client = client_for(tmp_path, keys)
    assert (
        client.post("/results", content=result.SerializeToString()).status_code == 422
    )
    assert client.get("/api/cases").json() == []


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
