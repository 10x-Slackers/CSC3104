import sys
from datetime import timedelta
from pathlib import Path

# The pytest executable does not add the repository root for namespace imports.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import grpc
import pytest

from apps.privacy_node import privacy_node_pb2 as pb
from apps.privacy_node.claims import (
    ALLOWED_FIELDS,
    chain_digest,
    check_payload,
    complete,
    valid_chain,
)
from apps.privacy_node.demo import start_banks
from apps.privacy_node.keys import make_keys, pair_paths
from apps.privacy_node.local_state import LocalBank
from apps.privacy_node.node import HOP_LIMIT, now, serve
from apps.privacy_node.store import Store
from apps.scorer.score import Scorer
from apps.security.security import check_peer_payload

TINY = Path(__file__).parents[1] / "dataset/fixtures/tiny"


@pytest.fixture
def banks(tmp_path):
    nodes, servers = start_banks(TINY, tmp_path)
    yield nodes, servers
    for server in servers.values():
        server.stop(0)


def run_all(nodes):
    return {
        (bank, d["incoming_payment_id"], d["outgoing_payment_id"]): r
        for bank, node in nodes.items()
        for d, r in zip(node.local.detections, node.search_detections())
    }


def transfers(result):
    return {r.token for c in result.claims for r in (c.in_ref, c.out_ref) if r.token}


def trace(node, payment, direction, sender, **changes):
    side = "in" if direction == pb.DOWNSTREAM else "out"
    if "ref" not in changes:
        changes["ref"] = node.local.refs[payment, side]
    fields = {
        "query_id": "q1",
        "sender": sender,
        "key_version": "k1",
        "direction": direction,
        "hops_remaining": 3,
        "deadline": (now() + timedelta(seconds=5)).isoformat(),
    }
    return node.Trace(pb.TraceRequest(**fields | changes), None)


def test_fixture_scenarios(banks):
    nodes, _ = banks
    results = run_all(nodes)
    # Only planted mule-chain transfers are locally suspicious (lookalike l1, absent x1 are not).
    started = {p for key in results for p in key[1:]}
    assert not started & {"p01", "p02", "p03", "p04", "p05", "p08", "p09", "p13", "p14"}

    m1 = results["B", "p06", "p07"]
    assert m1.state == pb.CONFIRMED
    assert [c.signer for c in m1.claims] == ["A", "B", "C"]
    assert len(transfers(m1)) == 2

    # Loop o1: searches from B and C agree on one chain identity and terminate.
    o1_b, o1_c = results["B", "p10", "p11"], results["C", "p11", "p12"]
    assert o1_b.state == o1_c.state == pb.CONFIRMED
    assert o1_b.chain_digest == o1_c.chain_digest
    assert len(transfers(o1_b)) == 3

    # Over-limit v1 (6 transfers): every search follows HOP_LIMIT + 1 transfers and
    # names the unfollowed one at the cut, so all starts agree on one digest.
    v1 = [r for k, r in results.items() if k[1] in {"p15", "p16", "p17", "p18", "p19"}]
    assert len(v1) == 5
    assert all(r.state == pb.PARTIAL and not complete(r.claims) for r in v1)
    assert all(len(transfers(r)) == HOP_LIMIT + 2 for r in v1)
    assert len({r.chain_digest for r in v1}) == 1


def test_one_chain_row_per_bank_after_repeats(banks):
    nodes, _ = banks
    run_all(nodes)
    run_all(nodes)  # repeated searches and deliveries
    for node in nodes.values():
        chains = node.store.chains()
        assert len(chains) == len({c.chain_digest for c in chains}) == 3
        assert node.store.pending_deliveries() == []
    digests = [{c.chain_digest for c in n.store.chains()} for n in nodes.values()]
    assert digests[0] == digests[1] == digests[2]


def test_peer_messages_carry_no_raw_fields(banks):
    nodes, _ = banks
    for result in run_all(nodes).values():
        check_payload(result)
        wire = result.SerializeToString()
        for raw in (b"p06", b"p07", b"B-m1", b"A-m0", b"9500.0", b"Wire"):
            assert raw not in wire


def test_tampered_or_wrongly_signed_claims_rejected(banks):
    nodes, _ = banks
    result = nodes["B"].search("p06", "p07")
    nodes["B"].deliver_pending()
    keys = nodes["B"].public_keys
    assert valid_chain(result.claims, keys)

    tampered = pb.ChainResult()
    tampered.CopyFrom(result)
    tampered.claims[2].out_ref.token = "ppmc:v1:k1:" + "0" * 64
    assert not valid_chain(tampered.claims, keys)

    forged = pb.ChainResult()
    forged.CopyFrom(result)
    forged.claims[0].signer = "C"  # C did not sign A's claim
    assert not valid_chain(forged.claims, keys)

    tampered.chain_digest = chain_digest(tampered.claims)
    with pytest.raises(grpc.RpcError) as error:
        nodes["A"].call("C", "Deliver", tampered, now() + timedelta(seconds=5))
    assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert [c.chain_digest for c in nodes["C"].store.chains()] == [result.chain_digest]


def test_retried_query_gets_stored_reply(banks, monkeypatch):
    nodes, _ = banks
    first = trace(nodes["C"], "p07", pb.DOWNSTREAM, "B")
    assert first.state == pb.CONFIRMED and first.claims[0].signer == "C"
    monkeypatch.setattr(nodes["C"], "follow", lambda request: pytest.fail("redone"))
    assert trace(nodes["C"], "p07", pb.DOWNSTREAM, "B") == first


def test_unknown_token_wrong_sender_and_deadline(banks):
    nodes, _ = banks
    c = nodes["C"]
    assert trace(c, "p07", pb.DOWNSTREAM, "A").state == pb.NO_MATCH  # A is not on B-C
    inbound = c.local.refs["p07", "in"]
    assert trace(c, "p07", pb.UPSTREAM, "B", ref=inbound).state == pb.NO_MATCH
    late = (now() - timedelta(seconds=1)).isoformat()
    assert (
        trace(c, "p07", pb.DOWNSTREAM, "B", query_id="q9", deadline=late).state
        == pb.TIMED_OUT
    )


def test_visited_token_closes_loop_without_forwarding(banks):
    nodes, _ = banks
    b = nodes["B"]
    seen = b.local.refs["p11", "out"]
    reply = trace(b, "p10", pb.DOWNSTREAM, "A", visited=[seen])
    assert reply.state == pb.CONFIRMED
    assert [c.signer for c in reply.claims] == ["B"]
    assert reply.claims[0].out_ref == seen


def test_hop_budget_exhausted_is_partial(banks):
    nodes, _ = banks
    b = nodes["B"]
    reply = trace(b, "p15", pb.DOWNSTREAM, "A", hops_remaining=0)
    assert reply.state == pb.PARTIAL
    assert reply.claims[0].out_ref == b.local.refs["p16", "out"]  # cut, not an end
    assert not complete(reply.claims)


def test_delivered_state_is_checked(banks):
    nodes, _ = banks
    result = nodes["B"].search("p15", "p16")
    nodes["C"].store = Store()
    deadline = now() + timedelta(seconds=5)
    result.state = pb.CONFIRMED  # sender lies about a cut chain
    nodes["B"].call("C", "Deliver", result, deadline)
    assert [c.state for c in nodes["C"].store.chains()] == [pb.PARTIAL]

    result.state = pb.PENDING  # only final states are stored
    with pytest.raises(grpc.RpcError):
        nodes["B"].call("C", "Deliver", result, deadline)


def test_peer_error_fails_search_without_retry(banks, monkeypatch):
    nodes, _ = banks
    monkeypatch.setattr(nodes["C"], "follow", lambda request: 1 / 0)
    assert nodes["B"].search("p06", "p07").state == pb.FAILED
    assert nodes["B"].store.unfinished_searches() == []


def test_intra_bank_detection_is_not_searched(tmp_path):
    make_keys(tmp_path)
    event = {"bank": "B", "account": "B-x", "payment_format": "Wire"}
    events = [
        event
        | {"payment_id": "q1", "direction": "in", "counterparty_bank": "B"}
        | {"event_time": "2022-09-05T09:00:00Z", "amount": "100"},
        event
        | {"payment_id": "q2", "direction": "out", "counterparty_bank": "C"}
        | {"event_time": "2022-09-05T09:05:00Z", "amount": "95"},
    ]
    scorer = Scorer("B")
    assert [d for e in events for d in scorer.process(e)]  # the rule does flag it
    local = LocalBank("B", "k1", pair_paths(tmp_path, "B"), events)
    assert local.onward == {} and local.detections == []


def test_offline_bank_leaves_search_pending_then_recovers(banks):
    nodes, servers = banks
    port = int(nodes["B"].peers["C"].split(":")[1])
    servers["C"].stop(0)

    result = nodes["B"].search("p06", "p07")
    assert result.state == pb.PENDING
    assert [c.signer for c in result.claims] == ["A", "B"]  # upstream still answered
    assert all(n.store.chains() == [] for n in nodes.values())

    servers["C"], _ = serve(nodes["C"], port)
    nodes["B"].housekeeping()
    for node in nodes.values():
        [chain] = node.store.chains()
        assert chain.state == pb.CONFIRMED


def test_undelivered_result_is_redelivered_after_restart(banks):
    nodes, servers = banks
    port = int(nodes["B"].peers["C"].split(":")[1])
    result = nodes["B"].search("p06", "p07")
    nodes["B"].store, nodes["C"].store = Store(), Store()  # B restarts mid-delivery
    nodes["B"].store.save_chain(result, deliver_to=["A", "C"])
    servers["C"].stop(0)

    nodes["B"].deliver_pending()
    assert [bank for bank, _ in nodes["B"].store.pending_deliveries()] == ["C"]

    servers["C"], _ = serve(nodes["C"], port)
    nodes["B"].housekeeping()
    assert nodes["B"].store.pending_deliveries() == []
    assert [c.chain_digest for c in nodes["C"].store.chains()] == [result.chain_digest]
    assert len(nodes["A"].store.chains()) == 1  # A's duplicate delivery made no new row


def test_messages_pass_allowlist_and_reject_forbidden_fields():
    check_payload(pb.TraceRequest(query_id="q", sender="A"))
    with pytest.raises(ValueError):
        check_peer_payload({"amount": 1}, ALLOWED_FIELDS)
