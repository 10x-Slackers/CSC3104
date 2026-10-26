import threading

import pytest

from apps.case.case_store import AlertRule, Claim, SqliteCaseStore, transfers
from apps.case.outbox import dispatch
from apps.case.report import TemplateClient, draft_report
from apps.case.sample_chains import as_partial, bridged, leaks, m1, o1, v1
from apps.privacy_node import privacy_node_pb2 as pb
from apps.privacy_node.claims import key

# DEPENDS ON Role 3 (privacy node): every test here uses real ChainResults from its
# nodes (sample_chains.py), so a change to privacy_node.proto affects them all.


def alerts(store):
    return store.db.execute("SELECT COUNT(*) FROM alerts").fetchone()[0]


def test_transfer_keys_match_role3():
    start, link, _ = m1().claims
    assert transfers(Claim.from_pb(link)) == [key(link.in_ref), key(link.out_ref)]
    assert transfers(Claim.from_pb(start)) == [key(start.out_ref)]  # chain starts at A


def test_case_id_is_role3_chain_digest(store):
    result = m1()
    assert store.record(result) == result.chain_digest
    case = store.get(result.chain_digest)
    assert [c.signer for c in case["claims"]] == ["A", "B", "C"]
    assert case["state"] == "confirmed" and case["raised_at"] is not None


# DEPENDS ON Role 2 (streaming and scoring): the minimum chain length is theirs to set.
def test_rule_comes_from_config(tmp_path):
    store = SqliteCaseStore(tmp_path / "cases.db", AlertRule(min_transfers=3))
    assert store.get(store.record(m1()))["raised_at"] is None  # 2 transfers
    assert store.get(store.record(o1()))["raised_at"] is not None  # 3 transfers


def test_overlapping_searches_share_one_case_and_alert(store):
    # o1 loop searched from B and from C: Role 3 gives both the same digest.
    first = store.record(o1("B"))
    assert store.record(o1("C")) == first
    assert store.record(o1("B")) == first  # redelivery
    assert [c["case_id"] for c in store.cases()] == [first]
    assert len(store.get(first)["results"]) == 2
    assert alerts(store) == 1


def test_overlapping_partials_join_one_case_without_alert(store):
    # Over-limit v1: five partial results with two digests, all sharing transfers.
    results = v1()
    assert len(results) == 5 and len({r.chain_digest for r in results}) == 2
    assert len({store.record(r) for r in results}) == 1
    [case] = store.cases()
    assert case["state"] == "partial" and case["raised_at"] is None
    # C's two searches return the same digest, so they are one (digest, sender) row.
    assert len(store.get(case["case_id"])["results"]) == 4


def test_partial_upgrades_to_confirmed_and_alerts_once(store):
    case_id = store.record(as_partial(m1()))
    assert store.get(case_id)["state"] == "partial"
    assert store.get(case_id)["raised_at"] is None
    assert store.record(m1()) == case_id
    raised = store.get(case_id)["raised_at"]
    assert store.get(case_id)["state"] == "confirmed" and raised is not None
    store.record(as_partial(m1()))  # never downgrades
    assert store.get(case_id)["state"] == "confirmed"
    assert store.get(case_id)["raised_at"] == raised


# The five result types from the workload split, plus Role 3's PENDING. Only CONFIRMED
# and PARTIAL are ever delivered (protocol.md); the rest must never become evidence.
@pytest.mark.parametrize(
    "state,expected",
    [
        (pb.CONFIRMED, ("confirmed", True)),
        (pb.PARTIAL, ("partial", False)),
        (pb.NO_MATCH, None),
        (pb.TIMED_OUT, None),
        (pb.INVALID_CLAIM, None),
        (pb.PENDING, None),
    ],
)
def test_each_result_type(store, state, expected):
    result = m1()
    result.state = state
    if expected is None:
        with pytest.raises(ValueError):
            store.record(result)
        assert store.cases() == [] and store.outbox() == []
        return
    case = store.get(store.record(result))
    assert (case["state"], case["raised_at"] is not None) == expected


def test_same_claim_submitted_twice_is_stored_once(store):
    case_id = store.record(m1())
    store.record(m1())
    case = store.get(case_id)
    assert len(case["claims"]) == 3 and len(case["results"]) == 1
    # o1 from B and from C: the same four links, signed for different queries.
    loop = store.record(o1("B"))
    store.record(o1("C"))
    assert len(store.get(loop)["claims"]) == 4
    assert len(store.get(loop)["results"]) == 2


def test_bridging_result_merges_into_oldest_case(store):
    first, second, whole = bridged()
    old, new = store.record(first), store.record(second)
    assert old != new
    assert store.record(whole) == old
    assert [c["case_id"] for c in store.cases()] == [old]
    assert store.get(new)["merged_into"] == old
    assert store.get(new)["raised_at"] is None
    assert len(store.get(old)["claims"]) == 6
    assert alerts(store) == 1


def test_concurrent_writers_create_one_case(tmp_path):
    path = tmp_path / "cases.db"
    SqliteCaseStore(path)
    ids = []
    threads = [
        threading.Thread(target=lambda r=r: ids.append(SqliteCaseStore(path).record(r)))
        for r in [o1("B"), o1("C")] * 8
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    store = SqliteCaseStore(path)
    assert len(set(ids)) == 1 and len(store.cases()) == 1
    assert alerts(store) == 1


def test_close_and_reopen_keeps_cases_alerts_and_pending_outbox(tmp_path):
    path = tmp_path / "cases.db"
    store = SqliteCaseStore(path)
    confirmed = store.record(m1())
    for result in v1():
        store.record(result)
    store.decide(confirmed, "report")
    before = store.cases()
    store.db.close()

    reopened = SqliteCaseStore(path)
    assert reopened.cases() == before
    assert reopened.get(confirmed)["raised_at"] is not None
    assert reopened.record(m1()) == confirmed  # still one alert after a redelivery
    assert [n["kind"] for n in reopened.notifications()] == ["alert"]
    sent = []
    assert dispatch(reopened, sent.append, "B") == 1  # the pending report
    assert dispatch(reopened, sent.append, "B") == 0


def test_leak_check_catches_planted_values():
    assert leaks('{"account": "A-m0", "payment": "p06", "amount": 120.0}') == {
        "A-m0",
        "p06",
        "120.0",
    }
    assert leaks("created_at 1793000120.05, pair A-B, Wired") == set()


def test_nothing_sensitive_in_any_stored_table(store):
    for result in [m1(), o1(), *v1()]:
        store.record(result)
    case_id = store.cases()[0]["case_id"]
    store.decide(case_id, "report")
    draft_report(store, store.get(case_id), TemplateClient())
    dispatch(store, lambda message: None, "B")
    tables = [row[0] for row in store.db.execute("SELECT name FROM sqlite_master")]
    dump = "\n".join(
        str(value)
        for table in tables
        if not table.startswith("sqlite_")
        for row in store.db.execute(f"SELECT * FROM {table}")
        for value in row
    )
    assert "reports" in tables and dump and leaks(dump) == set()
