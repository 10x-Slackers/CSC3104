import hashlib

import pytest
from google.protobuf.json_format import MessageToDict

from case.case_store import CaseStore
from case.evidence import parse
from privacy_node import privacy_node_pb2 as pb
from privacy_node.demo import start_banks
from privacy_node.node import serve
from sample_chains import TINY, delivered_to, leg, message, payment_ids

M1, O1 = {"p06", "p07"}, {"p10", "p11", "p12"}
V1 = {"p15", "p16", "p17", "p18", "p19", "p20"}


# DEPENDS ON Role 3 (privacy node): results come from its nodes on the tiny fixture.
@pytest.mark.parametrize("bank", "ABC")
def test_data_md_scenarios_at_each_bank(store, bank):
    """data.md fixture table, with the alert tiers: o1 and v1 alert at A, B and C;
    m1 is a Tier 1 case only at B, whose rule alone links it; l1, x1 and ordinary
    transfers never become cases."""
    for result in delivered_to(bank):
        store.record(message(result, bank))
    outcomes = {
        frozenset(payment_ids(store.get(c["case_id"]))): (c["tier"], c["state"])
        for c in store.cases()
    }
    expected = {frozenset(O1): (2, "confirmed"), frozenset(V1): (2, "partial")}
    if bank == "B":
        expected[frozenset(M1)] = (1, "confirmed")
    assert outcomes == expected
    alerts = store.db.execute("SELECT COUNT(*) AS n FROM alerts").fetchone()["n"]
    assert alerts == 2


@pytest.fixture
def banks(tmp_path):
    nodes, servers = start_banks(TINY, tmp_path)
    yield nodes, servers
    for server in servers.values():
        server.stop(0)


# DEPENDS ON Role 3 (privacy node): a missing bank makes the search PENDING, and
# housekeeping completes it once the bank is back.
def test_pending_case_waits_for_bank_c_then_upgrades(banks, tmp_path):
    nodes, servers = banks
    store = CaseStore(str(tmp_path / "B.db"))
    port = int(nodes["B"].peers["C"].split(":")[1])
    servers["C"].stop(0)
    assert nodes["B"].search("p06", "p07").state == pb.PENDING
    b = nodes["B"].local  # this run's own keys, so look its tokens up here
    legs = [
        leg("B", p, d, ref.pair, ref.token.hex())
        for p, d in (("p06", "in"), ("p07", "out"))
        for ref in [b.ref(p, d, "k1")]
    ]
    pending = {
        "key": hashlib.sha256(b"B:p06:p07").hexdigest(),
        "kind": "pending",
        "waiting_for": ["C"],
        "legs": legs,
    }
    case_id = store.record(parse(pending, nodes["B"].public_keys, "B"))
    [shown] = store.cases()  # the activity shows while C is down
    assert (shown["tier"], shown["state"]) == (1, "pending")

    servers["C"], _ = serve(nodes["C"], port)
    nodes["B"].housekeeping()
    [confirmed] = nodes["B"].store.chains()
    chain = {
        "key": confirmed.chain_digest,
        "kind": "chain",
        "result": MessageToDict(confirmed),
        "legs": legs,
    }
    assert store.record(parse(chain, nodes["B"].public_keys, "B")) == case_id
    case = store.get(case_id)
    assert (case["tier"], case["state"]) == (1, "confirmed")
    assert len(case["messages"]) == 2 and len(store.cases()) == 1
