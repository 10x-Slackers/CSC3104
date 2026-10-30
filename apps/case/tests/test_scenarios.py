import pytest

from case.case_store import SqliteCaseStore, transfers
from case.evidence import verify_result
from privacy_node import privacy_node_pb2 as pb
from privacy_node.claims import key
from privacy_node.demo import start_banks
from privacy_node.node import serve
from sample_chains import TINY, delivered_to, payment_ids, public_keys

M1, O1 = {"p06", "p07"}, {"p10", "p11", "p12"}
V1 = {"p15", "p16", "p17", "p18", "p19", "p20"}


# DEPENDS ON Role 3 (privacy node): results come from its nodes on the tiny fixture.
@pytest.mark.parametrize("bank", "ABC")
def test_data_md_scenarios_at_each_bank(tmp_path, bank):
    """data.md fixture table: m1 and o1 alert at A, B and C; v1 is partial and does
    not alert; l1, x1 and ordinary transfers never become cases."""
    store = SqliteCaseStore(tmp_path / f"{bank}.db")
    for result in delivered_to(bank):
        assert verify_result(result, public_keys(), bank)
        store.record(result)
    outcomes = {
        frozenset(payment_ids(store.get(c["case_id"]))): (
            c["state"],
            bool(c["raised_at"]),
        )
        for c in store.cases()
    }
    assert outcomes == {
        frozenset(M1): ("confirmed", True),
        frozenset(O1): ("confirmed", True),
        frozenset(V1): ("partial", False),
    }
    assert store.db.execute("SELECT COUNT(*) FROM alerts").fetchone()[0] == 2


@pytest.fixture
def banks(tmp_path):
    nodes, servers = start_banks(TINY, tmp_path)
    yield nodes, servers
    for server in servers.values():
        server.stop(0)


# DEPENDS ON Role 3 (privacy node): a missing bank makes the search PENDING, which it
# never delivers, and housekeeping completes it once the bank is back.
def test_missing_bank_case_waits_then_confirms(banks, tmp_path):
    nodes, servers = banks
    store = SqliteCaseStore(tmp_path / "B.db")
    port = int(nodes["B"].peers["C"].split(":")[1])
    servers["C"].stop(0)
    pending = nodes["B"].search("p06", "p07")
    assert pending.state == pb.PENDING
    with pytest.raises(ValueError):
        store.record(pending)  # not evidence yet: no case, no alert
    assert store.cases() == []

    servers["C"], _ = serve(nodes["C"], port)
    nodes["B"].housekeeping()
    [confirmed] = nodes["B"].store.chains()
    assert verify_result(confirmed, nodes["B"].public_keys, "B")
    case = store.get(store.record(confirmed))
    assert case["state"] == "confirmed" and case["raised_at"] is not None
    b = nodes["B"].local  # this run's own keys, so look its tokens up here
    expected = {key(b.refs["p06", "in"]), key(b.refs["p07", "out"])}
    assert {k for c in case["claims"] for k in transfers(c)} == expected
