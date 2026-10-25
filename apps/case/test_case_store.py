import csv
import sys
import threading
from pathlib import Path

# The pytest executable does not add the repository root for namespace imports.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pytest
from case_store import (
    AlertRule,
    Claim,
    SearchResult,
    SqliteCaseStore,
    derive_case_id,
    transfers,
)

from apps.security.security import derive_token

TINY = Path(__file__).parents[1] / "dataset/fixtures/tiny"
# Predictable secrets belong only to tests; deployments generate independent keys.
PAIR_KEYS = {("A", "B"): b"A" * 32, ("A", "C"): b"B" * 32, ("B", "C"): b"C" * 32}
# Payment ID -> (sender bank, receiver bank), from the tiny fixture.
ROUTES = {
    "p06": "AB", "p07": "BC", "p10": "AB", "p11": "BC", "p12": "CA",
    "p15": "AB", "p16": "BC", "p17": "CA", "p18": "AB", "p19": "BC", "p20": "CA",
}  # fmt: skip


def token(payment_id):
    pair = tuple(sorted(ROUTES[payment_id]))
    return derive_token(payment_id, *pair, PAIR_KEYS[pair], "k1")


def claim(incoming, outgoing, query="q1"):
    """The signer is the bank between the two transfers."""
    (in_bank, signer), (_, out_bank) = ROUTES[incoming], ROUTES[outgoing]
    return Claim(
        signer, in_bank, token(incoming), out_bank, token(outgoing), query, "s1", "00"
    )


def result(*claims, status="confirmed"):
    return SearchResult(claims[0].query_id if claims else "q0", status, claims)


@pytest.fixture
def store(tmp_path):
    return SqliteCaseStore(tmp_path / "cases.db")


def test_case_id_ignores_order_and_duplicates():
    keys = transfers(claim("p10", "p11")) + transfers(claim("p11", "p12"))
    assert derive_case_id(keys) == derive_case_id(reversed(keys + keys))
    assert derive_case_id(keys) != derive_case_id(keys[:2])


def test_single_claim_alerts_mule_chain(store):
    # m1: A -> B -> C. B's one claim covers two transfers across three banks.
    case_id = store.record(result(claim("p06", "p07")))
    case = store.get(case_id)
    assert case["raised_at"] is not None
    assert case["claims"] == [claim("p06", "p07")]
    assert case_id == derive_case_id(transfers(claim("p06", "p07")))


def test_rule_comes_from_config(tmp_path):
    store = SqliteCaseStore(tmp_path / "cases.db", AlertRule(min_transfers=3))
    case_id = store.record(result(claim("p10", "p11")))
    assert store.get(case_id)["raised_at"] is None
    assert store.record(result(claim("p11", "p12"))) == case_id
    assert store.get(case_id)["raised_at"] is not None


def test_overlapping_searches_share_one_case_and_alert(store):
    # o1 loop seen from B's search (both claims) and C's search (its own claim only).
    first = store.record(result(claim("p10", "p11", "qB"), claim("p11", "p12", "qB")))
    raised = store.get(first)["raised_at"]
    assert store.record(result(claim("p11", "p12", "qC"))) == first
    assert store.record(result(claim("p10", "p11", "qB"))) == first  # redelivery
    assert store.cases() == [
        {
            "case_id": first,
            "created_at": store.get(first)["created_at"],
            "state": "confirmed",
            "raised_at": raised,
        }
    ]
    assert len(store.get(first)["claims"]) == 2


def test_bridging_result_merges_into_oldest_case(store):
    # v1 over-limit chain: two searches find disjoint pieces, a third spans both.
    old = store.record(result(claim("p15", "p16", "q1")))
    new = store.record(result(claim("p18", "p19", "q2"), status="partial"))
    assert old != new
    assert (
        store.record(result(claim("p16", "p17", "q3"), claim("p17", "p18", "q3")))
        == old
    )
    assert [c["case_id"] for c in store.cases()] == [old]
    assert store.get(new)["merged_into"] == old
    assert store.get(new)["raised_at"] is None
    assert len(store.get(old)["claims"]) == 4
    assert {r[0] for r in store.get(old)["results"]} == {"q1", "q2", "q3"}
    assert store.db.execute("SELECT COUNT(*) FROM alerts").fetchone()[0] == 1


def test_concurrent_writers_create_one_case(tmp_path):
    path = tmp_path / "cases.db"
    SqliteCaseStore(path)
    results = [result(claim("p10", "p11")), result(claim("p11", "p12"))] * 8
    ids = []
    threads = [
        threading.Thread(target=lambda r=r: ids.append(SqliteCaseStore(path).record(r)))
        for r in results
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    store = SqliteCaseStore(path)
    assert len(set(ids)) == 1 and len(store.cases()) == 1
    assert store.db.execute("SELECT COUNT(*) FROM alerts").fetchone()[0] == 1


def test_alert_survives_restart(tmp_path):
    path = tmp_path / "cases.db"
    case_id = SqliteCaseStore(path).record(result(claim("p06", "p07")))
    raised = SqliteCaseStore(path).get(case_id)["raised_at"]
    reopened = SqliteCaseStore(path)
    assert reopened.record(result(claim("p06", "p07", "retry"))) == case_id
    assert reopened.get(case_id)["raised_at"] == raised


def test_partial_case_alerts_once_confirmed(store):
    # m1 while C is offline: partial, so no alert; C recovers and the search confirms.
    case_id = store.record(result(claim("p06", "p07", "q1"), status="partial"))
    assert store.get(case_id)["state"] == "partial"
    assert store.get(case_id)["raised_at"] is None
    assert store.record(result(claim("p06", "p07", "q2"))) == case_id
    assert store.get(case_id)["state"] == "confirmed"
    assert store.get(case_id)["raised_at"] is not None


@pytest.mark.parametrize("late", ["partial", "timed-out", "no-match", "invalid-claim"])
def test_late_weaker_result_keeps_confirmed_state(store, late):
    case_id = store.record(result(claim("p06", "p07", "q1")))
    store.record(result(claim("p06", "p07", "q2"), status=late))
    assert store.get(case_id)["state"] == "confirmed"
    assert store.get(case_id)["results"] == [("q1", "confirmed"), ("q2", late)]


@pytest.mark.parametrize("status", ["no-match", "timed-out", "invalid-claim"])
def test_result_without_claims_creates_no_case(store, status):
    assert store.record(SearchResult("q1", status)) is None
    assert store.cases() == []
    row = store.db.execute("SELECT query_id, status, case_id FROM results").fetchone()
    assert tuple(row) == ("q1", status, None)


def test_unknown_status_rejected():
    with pytest.raises(ValueError):
        SearchResult("q1", "matched")


def test_no_private_fields_stored(store):
    store.record(result(claim("p06", "p07")))
    store.record(result(claim("p10", "p11"), claim("p11", "p12")))
    # Text columns only: timestamps are floats that could contain an amount by chance.
    dump = "\n".join(
        value
        for table in ("cases", "transfers", "claims", "results", "alerts")
        for row in store.db.execute(f"SELECT * FROM {table}")
        for value in row
        if isinstance(value, str)
    )
    for bank in "ABC":
        with (TINY / f"bank_{bank}.csv").open() as source:
            for row in csv.DictReader(source):
                for field in ("payment_id", "account", "amount"):
                    assert row[field] not in dump
