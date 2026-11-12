import io
import json
import signal
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path
from unittest.mock import Mock

import grpc
import pytest
from google.protobuf.json_format import MessageToDict

from case.case_store import CaseStore
from case.evidence import parse
from ingestor import Event, read_events
from privacy_node import node as node_module
from privacy_node import privacy_node_pb2 as pb
from privacy_node.claims import (
    ALLOWED_FIELDS,
    chain_digest,
    check_result,
    complete,
    valid_chain,
    valid_claim,
)
from privacy_node.demo import prepare_demo_keys, start_banks
from privacy_node.keys import pair_paths, public_keys
from privacy_node.local_state import LocalBank
from privacy_node.node import HOP_LIMIT, Node, failure_state, now, serve
from privacy_node.store import Store
from scorer.score import Scorer
from scorer.stream import consume
from security import (
    check_peer_payload,
    generate_signing_key,
    load_signing_key,
)

TINY = Path(__file__).parents[2] / "dataset/fixtures/tiny"


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
        for d, r in zip(node.local.detections, node.search_detections(), strict=True)
    }


def transfers(result):
    return {r.token for c in result.claims for r in (c.in_ref, c.out_ref) if r.token}


def trace(node, payment, direction, sender, **changes):
    side = "in" if direction == pb.DOWNSTREAM else "out"
    if "ref" not in changes:
        changes["ref"] = node.local.ref(payment, side, "k1")
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
        wire = result.SerializeToString()
        for raw in (b"p06", b"p07", b"B-m1", b"A-m0", b"9500.0", b"MEPS+"):
            assert raw not in wire


def test_tampered_or_wrongly_signed_claims_rejected(banks):
    nodes, _ = banks
    result = nodes["B"].search("p06", "p07")
    nodes["B"].deliver_pending()
    keys = nodes["B"].public_keys
    assert valid_chain(result.claims, keys)

    tampered = pb.ChainResult()
    tampered.CopyFrom(result)
    tampered.claims[2].out_ref.token = b"0" * 16
    assert not valid_chain(tampered.claims, keys)

    forged = pb.ChainResult()
    forged.CopyFrom(result)
    forged.claims[0].signer = "C"  # C did not sign A's claim
    assert not valid_chain(forged.claims, keys)

    tampered.chain_digest = chain_digest(tampered.claims)
    with pytest.raises(grpc.RpcError) as error:
        nodes["A"].call(
            "C", "Deliver", tampered, now() + timedelta(seconds=5), pb.Ack()
        )
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
    inbound = c.local.ref("p07", "in", "k1")
    assert trace(c, "p07", pb.UPSTREAM, "B", ref=inbound).state == pb.NO_MATCH
    late = (now() - timedelta(seconds=1)).isoformat()
    assert (
        trace(c, "p07", pb.DOWNSTREAM, "B", query_id="q9", deadline=late).state
        == pb.TIMED_OUT
    )


def test_visited_token_closes_loop_without_forwarding(banks):
    nodes, _ = banks
    b = nodes["B"]
    seen = b.local.ref("p11", "out", "k1")
    reply = trace(b, "p10", pb.DOWNSTREAM, "A", visited=[seen])
    assert reply.state == pb.CONFIRMED
    assert [c.signer for c in reply.claims] == ["B"]
    assert reply.claims[0].out_ref == seen


def test_hop_budget_exhausted_is_partial(banks):
    nodes, _ = banks
    b = nodes["B"]
    reply = trace(b, "p15", pb.DOWNSTREAM, "A", hops_remaining=0)
    assert reply.state == pb.PARTIAL
    assert reply.claims[0].out_ref == b.local.ref("p16", "out", "k1")  # cut, not an end
    assert not complete(reply.claims)


def test_delivered_state_is_checked(banks):
    nodes, _ = banks
    result = nodes["B"].search("p15", "p16")
    nodes["C"].store = Store()
    deadline = now() + timedelta(seconds=5)
    result.state = pb.CONFIRMED  # sender lies about a cut chain
    nodes["B"].call("C", "Deliver", result, deadline, pb.Ack())
    assert [c.state for c in nodes["C"].store.chains()] == [pb.PARTIAL]

    result.state = pb.PENDING  # only final states are stored
    with pytest.raises(grpc.RpcError):
        nodes["B"].call("C", "Deliver", result, deadline, pb.Ack())


def test_peer_error_fails_search_without_retry(banks, monkeypatch):
    nodes, _ = banks
    monkeypatch.setattr(nodes["C"], "follow", lambda request: 1 / 0)
    assert nodes["B"].search("p06", "p07").state == pb.FAILED
    assert nodes["B"].store.unfinished_searches() == []


def test_intra_bank_detection_is_not_searched(tmp_path):
    prepare_demo_keys(tmp_path)
    event = {"bank": "B", "account": "B-x", "payment_format": "MEPS+"}
    events = [
        event
        | {"payment_id": "q1", "direction": "in", "counterparty_bank": "B"}
        | {"counterparty_account": "B-y"}
        | {"event_time": "2022-09-05T09:00:00Z", "amount": "100"},
        event
        | {"payment_id": "q2", "direction": "out", "counterparty_bank": "C"}
        | {"counterparty_account": "C-z"}
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
    check_peer_payload({"query_id": "q", "sender": "A"}, ALLOWED_FIELDS)
    with pytest.raises(ValueError):
        check_peer_payload({"amount": 1}, ALLOWED_FIELDS)


def kafka_message(event):
    return Mock(
        **{
            "value.return_value": json.dumps(event).encode(),
            "key.return_value": event["account"].encode(),
            "topic.return_value": f"bank-{event['bank']}-transactions",
            "error.return_value": None,
        }
    )


def feed(local, events):
    consumer = Mock()
    consumer.commit.return_value = []
    consumer.poll.side_effect = [*[kafka_message(e) for e in events], KeyboardInterrupt]
    output = io.StringIO()
    with pytest.raises(KeyboardInterrupt):
        consume(consumer, local.scorer, output, process_event=local.process)
    assert consumer.commit.call_count == len(events)
    consumer.close.assert_called_once()
    return [json.loads(line) for line in output.getvalue().splitlines()]


@pytest.mark.parametrize("reverse", [False, True])
def test_kafka_state_drives_real_peer_search(tmp_path, monkeypatch, reverse):
    events = {}
    for bank in "ABC":
        events[bank] = list(read_events(TINY / f"bank_{bank}.csv"))
    monkeypatch.setattr(
        LocalBank,
        "from_workload",
        Mock(side_effect=AssertionError("CSV reconstruction")),
    )
    prepare_demo_keys(tmp_path)
    public = public_keys(tmp_path, "A")
    nodes, servers, ports = {}, {}, {}
    try:
        for bank in "ABC":
            local = LocalBank(bank, "k1", pair_paths(tmp_path, bank))
            nodes[bank] = Node(
                local,
                Store(),
                load_signing_key(tmp_path / bank / "private/signing/k1.pem"),
                public,
                {},
            )
            servers[bank], ports[bank] = serve(nodes[bank])
        for bank, node in nodes.items():
            node.peers.update(
                {b: f"localhost:{port}" for b, port in ports.items() if b != bank}
            )
            ordered = events[bank][::-1] if reverse else events[bank]
            expected_scorer = Scorer(bank)
            expected = [d for e in ordered for d in expected_scorer.process(e)]
            actual = feed(node.local, [e for e in ordered for _ in range(2)])
            assert actual == expected == node.local.detections
            for e in ordered:
                if e["counterparty_bank"] != bank:
                    ref = node.local.ref(e["payment_id"], e["direction"], "k1")
                    assert (
                        node.local.lookup(ref.pair, "k1", ref.token, e["direction"])
                        == e["payment_id"]
                    )
            with pytest.raises(ValueError, match="unrelated"):
                node.local.lookup(
                    "-".join(b for b in "ABC" if b != bank), "k1", "token", "in"
                )
        results = {
            (b, d["incoming_payment_id"], d["outgoing_payment_id"]): r
            for b, node in nodes.items()
            for d, r in zip(
                node.local.detections, node.search_new_detections(), strict=True
            )
        }
        assert len(results) == 8
        assert results["B", "p06", "p07"].state == pb.CONFIRMED
        assert results["B", "p10", "p11"].state == pb.CONFIRMED
        assert results["C", "p11", "p12"].state == pb.CONFIRMED
        assert all(
            r.state == pb.PARTIAL
            for k, r in results.items()
            if k[1] in {"p15", "p16", "p17", "p18", "p19"}
        )
        for result in results.values():
            assert b"B-m1" not in result.SerializeToString()
        for bank, node in nodes.items():
            identities = {d["detection_id"] for d in node.local.detections}
            feed(node.local, events[bank])
            assert {d["detection_id"] for d in node.local.detections} == identities
            assert node.search_new_detections() == []
    finally:
        for server in servers.values():
            server.stop(0)


@pytest.mark.parametrize("failure", ["handoff", "output", "commit", "missing_key"])
def test_live_processing_failure_boundaries(tmp_path, failure):
    prepare_demo_keys(tmp_path)
    paths = pair_paths(tmp_path, "B")
    if failure == "missing_key":
        from security import AgreementFiles

        paths = AgreementFiles(paths.private_key, {"C": paths.peer_public_keys["C"]})
    local = LocalBank("B", "k1", paths)
    events = [
        e for e in read_events(TINY / "bank_B.csv") if e["payment_id"] in ("p06", "p07")
    ]
    consumer = Mock()
    consumer.poll.side_effect = [
        *[kafka_message(e) for e in events],
        AssertionError("continued after failure"),
    ]
    consumer.commit.side_effect = (
        [[], RuntimeError("commit failed")] if failure == "commit" else None
    )
    consumer.commit.return_value = []
    output = Mock()
    if failure == "output":
        output.flush.side_effect = [None, OSError("output failed")]

    def process(event):
        results = local.process(event)
        if results and failure == "handoff":
            raise RuntimeError("handoff failed")
        return results

    with pytest.raises((RuntimeError, OSError, ValueError)):
        consume(consumer, local.scorer, output, process_event=process)
    assert consumer.commit.call_count == {"missing_key": 0, "commit": 2}.get(failure, 1)
    consumer.close.assert_called_once()
    if failure != "missing_key":
        rebuilt = LocalBank("B", "k1", paths)
        feed(rebuilt, events)
        assert rebuilt.detections == local.detections


def test_kafka_cli_does_not_load_workload(tmp_path, monkeypatch):
    prepare_demo_keys(tmp_path)
    monkeypatch.setattr(
        LocalBank,
        "from_workload",
        Mock(side_effect=AssertionError("CSV reconstruction")),
    )
    events = list(read_events(TINY / "bank_B.csv"))
    consumer = Mock()
    consumer.commit.return_value = []
    finished = threading.Event()
    searches = []

    def search(incoming, outgoing):
        searches.append((incoming, outgoing))
        if len(searches) == 4:
            finished.set()
        return pb.ChainResult(state=pb.CONFIRMED)

    monkeypatch.setattr(Node, "search", staticmethod(search))
    messages = iter(kafka_message(e) for e in events)

    def poll(timeout):
        try:
            return next(messages)
        except StopIteration:
            assert finished.wait(2), "live searches did not start"
            raise RuntimeError("end of test") from None

    consumer.poll.side_effect = poll
    constructor = Mock(return_value=consumer)
    monkeypatch.setattr("confluent_kafka.Consumer", constructor)
    nodes = []
    server = Mock()

    def capture(node, port):
        nodes.append(node)
        return server, port

    monkeypatch.setattr(node_module, "serve", capture)
    args = [
        "node",
        "--bank",
        "B",
        "--bootstrap-servers",
        "test-broker:9092",
        "--group",
        "fresh-bank-B",
        "--search",
        "--housekeeping-seconds",
        "0.01",
        "--key-directory",
        str(tmp_path),
    ]
    args += ["--insecure-demo"]
    monkeypatch.setattr(sys, "argv", args)
    with pytest.raises(RuntimeError, match="end of test"):
        node_module.main()
    assert len(nodes[0].local.detections) == 4
    assert len(nodes[0].local.counterparty) == 13
    assert consumer.commit.call_count == 15
    assert set(searches) == {
        ("p06", "p07"),
        ("p10", "p11"),
        ("p15", "p16"),
        ("p18", "p19"),
    }
    assert constructor.call_args.args[0]["enable.auto.commit"] is False
    assert constructor.call_args.args[0]["group.id"] == "fresh-bank-B"
    server.stop.assert_called_once_with(0)


def fixture_rows(bank: str) -> list[Event]:
    return list(read_events(TINY / f"bank_{bank}.csv"))


def delay_c(nodes, key_dir):
    nodes["B"].local = LocalBank(
        "B",
        "k1",
        pair_paths(key_dir, "B"),
        [e for e in fixture_rows("B") if e["payment_id"] in ("p06", "p07")],
    )
    nodes["C"].local = LocalBank("C", "k1", pair_paths(key_dir, "C"))


def test_delayed_peer_without_grace_stays_final(banks, tmp_path):
    nodes, _ = banks
    delay_c(nodes, tmp_path)
    assert nodes["B"].search_new_detections()[0].state == pb.NO_MATCH
    feed(nodes["C"].local, [e for e in fixture_rows("C") if e["payment_id"] == "p07"])
    nodes["B"].housekeeping()
    assert nodes["B"].search_new_detections() == []
    assert nodes["B"].store.chains() == []
    assert nodes["B"].search("p06", "p07").state == pb.CONFIRMED


@pytest.mark.parametrize("arrives", [False, True])
def test_bounded_delayed_peer_retry(banks, tmp_path, monkeypatch, arrives):
    nodes, _ = banks
    delay_c(nodes, tmp_path)
    bank = nodes["B"]
    bank.ingestion_grace_seconds = 5
    clock = [now()]
    monkeypatch.setattr(node_module, "now", lambda: clock[0])
    assert bank.search_new_detections()[0].state == pb.PENDING
    assert bank.store.unfinished_searches() == [("p06", "p07")]
    if arrives:
        feed(
            nodes["C"].local, [e for e in fixture_rows("C") if e["payment_id"] == "p07"]
        )
        clock[0] += timedelta(seconds=1)
    else:
        clock[0] += timedelta(seconds=6)
    bank.housekeeping()
    assert bank.store.unfinished_searches() == []
    expected = pb.CONFIRMED if arrives else pb.NO_MATCH
    assert bank.store._run("SELECT state FROM searches") == [(expected,)]
    if arrives:
        assert bank.store.chains()[0].state == pb.CONFIRMED
    else:
        assert bank.store.chains() == []
        monkeypatch.setattr(
            bank, "search", Mock(side_effect=AssertionError("unbounded retry"))
        )
        bank.housekeeping()


def test_replay_rebuilds_state_and_repeated_result_makes_one_case(banks, tmp_path):
    nodes, _ = banks
    old = nodes["B"]
    path = tmp_path / "protocol.db"
    old.store = Store(path)
    first = old.search("p06", "p07")
    old_ref = old.local.ref("p06", "in", "k1")
    restarted = Node(
        LocalBank("B", "k1", pair_paths(tmp_path, "B")),
        Store(path),
        old.signing_key,
        old.public_keys,
        old.peers,
    )
    assert restarted.local.detections == []
    assert restarted.local.lookup(old_ref.pair, "k1", old_ref.token, "in") is None
    assert restarted.store.chains()[0].chain_digest == first.chain_digest
    rows = fixture_rows("B")
    feed(restarted.local, [e for e in rows for _ in range(2)])
    assert restarted.local.detections == old.local.detections
    assert restarted.local.lookup(old_ref.pair, "k1", old_ref.token, "in") == "p06"
    replayed = restarted.search("p06", "p07")
    assert first.chain_digest == replayed.chain_digest
    case = CaseStore(str(tmp_path / "cases.db"))
    for result in (first, replayed, first):
        assert check_result(result, old.public_keys, "B")
        message = {"key": result.chain_digest, "kind": "chain", "legs": []}
        message["result"] = MessageToDict(result)
        case.record(parse(message, old.public_keys, "B"))
    # m1: only B's rule links it, so one low-priority case and no alert (case.md).
    assert [c["tier"] for c in case.cases()] == [1]
    assert case.cases()[0]["raised_at"] is None


def test_concurrent_state_reads_and_old_token_retention(tmp_path):
    prepare_demo_keys(tmp_path)
    local = LocalBank("B", "k1", pair_paths(tmp_path, "B"))
    rows = fixture_rows("B")

    def write():
        for _ in range(20):
            for e in rows:
                local.process(e)

    def read():
        for _ in range(200):
            for d in local.detections:
                ref = local.ref(d["incoming_payment_id"], "in", "k1")
                assert (
                    local.lookup(ref.pair, "k1", ref.token, "in")
                    == d["incoming_payment_id"]
                )

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(write), executor.submit(read)]
        for future in futures:
            future.result()
    assert len(local.detections) == 4
    ref = local.ref("p06", "in", "k1")
    local.process(
        rows[3] | {"payment_id": "later", "event_time": "2022-09-12T12:00:00Z"}
    )
    assert ("p06", "in") not in local.scorer.state.window("B-m1")
    assert local.lookup(ref.pair, "k1", ref.token, "in") == "p06"


def test_kafka_cli_sigterm_closes_consumer(tmp_path, monkeypatch):
    prepare_demo_keys(tmp_path)
    consumer = Mock()
    consumer.poll.side_effect = lambda timeout: signal.raise_signal(signal.SIGTERM)
    monkeypatch.setattr("confluent_kafka.Consumer", Mock(return_value=consumer))
    server = Mock()
    monkeypatch.setattr(node_module, "serve", lambda node, port: (server, port))
    args = [
        "node",
        "--bank",
        "B",
        "--bootstrap-servers",
        "broker:9092",
        "--key-directory",
        str(tmp_path),
    ]
    args += ["--insecure-demo"]
    monkeypatch.setattr(sys, "argv", args)
    previous = signal.getsignal(signal.SIGTERM)
    node_module.main()
    assert signal.getsignal(signal.SIGTERM) == previous
    consumer.close.assert_called_once()
    consumer.commit.assert_not_called()
    server.stop.assert_called_once_with(0)


def test_valkey_cli_starts_empty_without_kafka_or_workload(tmp_path, monkeypatch):
    prepare_demo_keys(tmp_path)
    monkeypatch.setattr("confluent_kafka.Consumer", Mock(side_effect=AssertionError))
    monkeypatch.setattr(LocalBank, "from_workload", Mock(side_effect=AssertionError))
    server = Mock()
    server.wait_for_termination.return_value = False
    serve = Mock(return_value=(server, 0))
    monkeypatch.setattr(node_module, "serve", serve)
    args = ["node", "--bank", "B", "--valkey-url", "valkey://valkey:6379"]
    args += ["--key-directory", str(tmp_path), "--insecure-demo"]
    monkeypatch.setattr(sys, "argv", args)
    node_module.main()
    assert serve.call_args.args[0].local.detections == []
    server.stop.assert_called_once_with(0)


@pytest.mark.parametrize("operation", ["search", "delivery"])
def test_shutdown_stops_between_queued_operations(banks, monkeypatch, operation):
    nodes, _ = banks
    bank = nodes["B"]
    bank.search("p06", "p07")  # queues deliveries to A and C
    stop = threading.Event()
    if operation == "search":
        bank.store.put_search("first", "p06", "p07", pb.PENDING, "")
        bank.store.put_search("second", "p10", "p11", pb.PENDING, "")
        search = Mock(side_effect=lambda *_: stop.set())
        monkeypatch.setattr(bank, "search", search)
        bank.housekeeping(stop=stop)
        search.assert_called_once()
        assert len(bank.store.unfinished_searches()) == 2
        assert len(bank.store.pending_deliveries()) == 2
    else:
        call = Mock(side_effect=lambda *_: stop.set())
        monkeypatch.setattr(bank, "call", call)
        bank.deliver_pending(stop=stop)
        call.assert_called_once()
        assert len(bank.store.pending_deliveries()) == 1


def test_live_rotation_reload_overlap_inflight_and_retirement(banks, tmp_path):
    nodes, _ = banks
    old = nodes["B"].search("p06", "p07")
    prepare_demo_keys(tmp_path, "k2")

    def config(active, query):
        return {
            "active_versions": active,
            "query_version": query,
            "signing_key_version": "k2",
            "peer_versions": {bank: active for bank in "ABC"},
        }

    for node in nodes.values():
        node.reload_key_directory(tmp_path, config(["k1", "k2"], "k1"))
    # A new replay segment is ingested under both versions while k1 queries continue.
    for bank, node in nodes.items():
        for row in fixture_rows(bank):
            if row["payment_id"] in ("p06", "p07"):
                row["payment_id"] = "rotation-" + row["payment_id"]
                stamp = node_module.datetime.fromisoformat(
                    row["event_time"].replace("Z", "+00:00")
                )
                row["event_time"] = (stamp + timedelta(days=1)).isoformat()
                node.local.process(row)
                for version in ("k1", "k2"):
                    ref = node.local.ref(row["payment_id"], row["direction"], version)
                    assert (
                        node.local.lookup(
                            ref.pair, version, ref.token, row["direction"]
                        )
                        == row["payment_id"]
                    )
    overlap = nodes["B"].search("rotation-p06", "rotation-p07")
    assert overlap.state == pb.CONFIRMED
    assert {c.key_version for c in overlap.claims} == {"k1"}
    assert {c.signing_key_version for c in overlap.claims} == {"k2"}

    with pytest.raises(ValueError, match="not ready"):
        nodes["B"].local.index.set_query_version("k2", {"A": ["k1"], "C": ["k1", "k2"]})
    for node in nodes.values():
        node.reload_key_directory(tmp_path, config(["k1", "k2"], "k2"))
    # Pending work seeded before rotation can retry with the new version too.
    nodes["B"].store._run(
        "UPDATE searches SET state=? WHERE incoming=? AND outgoing=?",
        pb.PENDING,
        "p06",
        "p07",
    )
    nodes["B"].housekeeping()
    assert ("p06", "p07") not in nodes["B"].store.unfinished_searches()
    assert nodes["B"].search("p06", "p07").state == pb.CONFIRMED
    current = nodes["B"].search("rotation-p06", "rotation-p07")
    assert current.state == pb.CONFIRMED
    assert {c.key_version for c in current.claims} == {"k2"}
    assert current.chain_digest != overlap.chain_digest

    # An old in-flight search still forwards k1 even though each node now queries k2.
    ref = nodes["B"].local.ref("p10", "in", "k1")
    deadline = now() + timedelta(seconds=5)
    request = nodes["A"].request(
        "old-flight", ref, pb.DOWNSTREAM, 3, deadline.isoformat(), [ref], "k1"
    )
    reply = nodes["A"].call("B", "Trace", request, deadline, pb.TraceReply())
    assert reply.state == pb.CONFIRMED
    assert {c.key_version for c in reply.claims} == {"k1"}
    assert valid_chain(reply.claims, nodes["B"].public_keys)

    old_ref = nodes["B"].local.ref("rotation-p06", "in", "k1")
    for node in nodes.values():
        node.reload_key_directory(tmp_path, config(["k2"], "k2"))
    assert nodes["B"].local.lookup(old_ref.pair, "k1", old_ref.token, "in") is None
    assert nodes["B"].search("rotation-p06", "rotation-p07").state == pb.CONFIRMED
    assert nodes["B"].search("p06", "p07").state == pb.CONFIRMED
    # Retaining trusted old signing public keys lets already-signed evidence verify.
    assert check_result(old, nodes["B"].public_keys, "B").state == pb.CONFIRMED


def test_branch_cap_is_a_signed_cut(banks, monkeypatch):
    nodes, _ = banks
    monkeypatch.setattr(nodes["B"].local, "links", lambda *args: ["p11", "p11"])
    reply = trace(nodes["B"], "p10", pb.DOWNSTREAM, "A")
    assert reply.state == pb.PARTIAL
    assert reply.claims[0].truncated
    assert valid_chain(reply.claims, nodes["B"].public_keys)


def test_signing_reload_cannot_mix_key_and_version_within_a_claim(banks, monkeypatch):
    nodes, _ = banks
    node = nodes["B"]
    fresh = generate_signing_key()
    node.public_keys = dict(node.public_keys) | {("B", "k2"): fresh.public_key()}
    encode = node_module.claim_bytes

    def rotate_during_encoding(claim):
        with node.local.lock:
            node.signing_key = fresh
            node.signing_key_version = "k2"
        return encode(claim)

    monkeypatch.setattr(node_module, "claim_bytes", rotate_during_encoding)
    first = node.claim(
        "B",
        "concurrent-rotation",
        node.local.ref("p06", "in", "k1"),
        node.local.ref("p07", "out", "k1"),
        node.key_version,
    )
    assert first.signing_key_version == "k1"
    assert valid_claim(first, node.public_keys)
    second = node.claim(
        "B",
        "after-rotation",
        node.local.ref("p06", "in", "k1"),
        node.local.ref("p07", "out", "k1"),
        node.key_version,
    )
    assert second.signing_key_version == "k2"
    assert valid_claim(second, node.public_keys)


def test_slow_key_reload_does_not_block_housekeeping_or_state(
    banks, tmp_path, monkeypatch
):
    nodes, _ = banks
    node = nodes["B"]
    prepare_demo_keys(tmp_path, "k2")
    started, release = threading.Event(), threading.Event()
    load = node_module.load_pairwise_keys

    def slow_load(*args):
        started.set()
        assert release.wait(5)
        return load(*args)

    monkeypatch.setattr(node_module, "load_pairwise_keys", slow_load)
    node.reload_keys = lambda: node.reload_key_directory(
        tmp_path,
        {
            "active_versions": ["k1", "k2"],
            "query_version": "k2",
            "signing_key_version": "k2",
            "peer_versions": {bank: ["k1", "k2"] for bank in "AC"},
        },
    )
    deliveries = Mock()
    monkeypatch.setattr(node, "deliver_pending", deliveries)
    try:
        node.housekeeping()
        assert started.wait(3)
        reload_thread = node._key_reload
        assert deliveries.call_count == 1
        assert node.local.lock.acquire(timeout=1)
        node.local.lock.release()
        node.housekeeping()
        assert node._key_reload is reload_thread  # no overlapping reloads
        assert deliveries.call_count == 2
    finally:
        release.set()
        node._key_reload.join(timeout=5)
    assert not node._key_reload.is_alive()
    assert node.key_version == node.signing_key_version == "k2"


@pytest.mark.parametrize("failure", ["signing_file", "readiness", "pair_material"])
def test_failed_key_reload_preserves_previous_working_set(
    banks, tmp_path, monkeypatch, failure
):
    nodes, _ = banks
    node = nodes["B"]
    prepare_demo_keys(tmp_path, "k2")
    original = node.local.index, node.signing_key, node.public_keys
    config = {
        "active_versions": ["k1", "k2"],
        "query_version": "k2",
        "signing_key_version": "k2",
        "peer_versions": {bank: ["k1", "k2"] for bank in "AC"},
    }
    if failure == "signing_file":
        monkeypatch.setattr(
            node_module, "own_key", Mock(side_effect=ValueError("bad PEM"))
        )
    elif failure == "readiness":
        config["peer_versions"]["C"] = ["k1"]
    else:
        (tmp_path / "B/private/x25519/k1.pem").write_bytes(
            (tmp_path / "B/private/x25519/k2.pem").read_bytes()
        )
    # Rejected manifests/material must fail the cheap checks before any index copy.
    monkeypatch.setattr(
        node_module, "copy", Mock(side_effect=AssertionError("copied invalid config"))
    )
    with pytest.raises(ValueError):
        node.reload_key_directory(tmp_path, config)
    assert (node.local.index, node.signing_key, node.public_keys) == original
    assert node.key_version == node.signing_key_version == "k1"
    assert node.local.index.active_versions == ("k1",)
    assert node.search("p06", "p07").state == pb.CONFIRMED


def test_unchanged_key_reload_keeps_existing_index(banks, tmp_path):
    nodes, _ = banks
    node = nodes["B"]
    original = node.local.index
    node.reload_key_directory(
        tmp_path,
        {
            "active_versions": ["k1"],
            "query_version": "k1",
            "signing_key_version": "k1",
            "peer_versions": {bank: ["k1"] for bank in "AC"},
        },
    )
    assert node.local.index is original


def test_query_version_switch_during_search_keeps_one_token_scope(
    banks, tmp_path, monkeypatch
):
    nodes, _ = banks
    prepare_demo_keys(tmp_path, "k2")
    for node in nodes.values():
        node.reload_key_directory(
            tmp_path,
            {
                "active_versions": ["k1", "k2"],
                "query_version": "k1",
                "signing_key_version": "k1",
                "peer_versions": {bank: ["k1", "k2"] for bank in "ABC"},
            },
        )
    initiator = nodes["B"]
    leg = initiator.leg

    def rotate_before_leg(*args):
        for node in nodes.values():
            node.local.index.set_query_version(
                "k2", {bank: ["k1", "k2"] for bank in "ABC"}
            )
        return leg(*args)

    monkeypatch.setattr(initiator, "leg", rotate_before_leg)
    result = initiator.search("p06", "p07")
    assert result.state == pb.CONFIRMED
    assert initiator.key_version == "k2"
    assert {claim.key_version for claim in result.claims} == {"k1"}
    assert check_result(result, initiator.public_keys, "B").state == pb.CONFIRMED


# Field 99 is not in our .proto: tag 0x9a 0x06 (field 99, length-delimited), length 10.
SMUGGLED = b"\x9a\x06\x0a9500.00USD"


def test_request_with_unknown_fields_is_rejected(banks):
    nodes, _ = banks
    request = pb.TraceRequest(
        query_id="q1",
        sender="B",
        key_version="k1",
        ref=nodes["C"].local.ref("p07", "in", "k1"),
        direction=pb.DOWNSTREAM,
        hops_remaining=3,
        deadline=(now() + timedelta(seconds=5)).isoformat(),
    )
    raw = request.SerializeToString() + SMUGGLED  # bytes a modified peer could send
    with grpc.insecure_channel(nodes["B"].peers["C"]) as channel:
        send_raw = channel.unary_unary(
            "/csc3104.privacy.v1.PrivacyNode/Trace",
            response_deserializer=pb.TraceReply.FromString,
        )
        with pytest.raises(grpc.RpcError) as error:
            send_raw(raw, timeout=5)
    assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT


def test_reply_with_unknown_fields_is_not_relayed(banks, monkeypatch):
    nodes, _ = banks
    real_follow = nodes["C"].follow

    def smuggling_follow(request):
        reply = real_follow(request)
        tainted = reply.claims[0].SerializeToString() + SMUGGLED
        reply.claims[0].CopyFrom(pb.LinkClaim.FromString(tainted))
        return reply

    monkeypatch.setattr(nodes["C"], "follow", smuggling_follow)
    assert nodes["B"].search("p06", "p07").state == pb.INVALID_CLAIM
    assert all(n.store.chains() == [] for n in nodes.values())


def test_reply_with_undefined_state_is_invalid(banks, monkeypatch):
    nodes, _ = banks
    real_follow = nodes["C"].follow

    def undefined_state_follow(request):
        reply = real_follow(request)
        reply.state = 99  # proto3 keeps an unknown enum number as a plain int
        return reply

    monkeypatch.setattr(nodes["C"], "follow", undefined_state_follow)
    assert nodes["B"].search("p06", "p07").state == pb.INVALID_CLAIM
    assert all(n.store.chains() == [] for n in nodes.values())
    nodes["B"].housekeeping()  # the background loop keeps running


def test_delivery_with_unknown_fields_is_rejected(banks):
    nodes, _ = banks
    result = nodes["B"].search("p06", "p07")  # valid: C would store it
    nodes["C"].store = Store()
    raw = result.SerializeToString() + SMUGGLED
    with grpc.insecure_channel(nodes["B"].peers["C"]) as channel:
        send_raw = channel.unary_unary(
            "/csc3104.privacy.v1.PrivacyNode/Deliver",
            response_deserializer=pb.Ack.FromString,
        )
        with pytest.raises(grpc.RpcError) as error:
            send_raw(raw, timeout=5)
    assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert nodes["C"].store.chains() == []


@pytest.mark.parametrize(
    ("code", "state"),
    [
        (grpc.StatusCode.UNAVAILABLE, pb.PENDING),
        (grpc.StatusCode.RESOURCE_EXHAUSTED, pb.PENDING),
        (grpc.StatusCode.ABORTED, pb.PENDING),
        (grpc.StatusCode.DEADLINE_EXCEEDED, pb.TIMED_OUT),
        (grpc.StatusCode.INVALID_ARGUMENT, pb.FAILED),
        (grpc.StatusCode.INTERNAL, pb.FAILED),
    ],
)
def test_failure_state_mapping(code, state):
    error = Mock()
    error.code.return_value = code
    assert failure_state(error) == state


def test_rejected_delivery_is_dead_lettered(banks):
    nodes, _ = banks
    result = nodes["B"].search("p06", "p07")
    nodes["B"].deliver_pending()
    forged = pb.ChainResult()
    forged.CopyFrom(result)
    forged.claims[0].signer = "C"  # C rejects it: A signed that claim, not C
    forged.chain_digest = "forged"
    nodes["B"].store.save_chain(forged, deliver_to=["C"])

    nodes["B"].deliver_pending()
    assert nodes["B"].store.pending_deliveries() == []
    assert nodes["B"].store.dead_letters() == [("forged", "C")]
