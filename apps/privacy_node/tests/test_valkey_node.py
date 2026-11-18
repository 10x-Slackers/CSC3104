"""The node on the scorer's Valkey: hits start searches, and messages reach the case store."""

import io
import json
import urllib.error
from pathlib import Path

import fakeredis
import pytest
from valkey.exceptions import ResponseError

from case.case_store import CaseStore
from case.evidence import parse
from ingestor import read_events
from privacy_node import privacy_node_pb2 as pb
from privacy_node.demo import prepare_demo_keys
from privacy_node.keys import BANKS, own_key, pair_paths, public_keys
from privacy_node.node import Node, serve
from privacy_node.store import Store
from privacy_node.valkey_bank import ValkeyBank
from scorer.score import Scorer
from scorer.state import ValkeyState
from scorer.token_index import TokenIndex

TINY = Path(__file__).parents[2] / "dataset/fixtures/tiny"


def start(key_dir, events):
    """Score each bank's events into its own Valkey, then start a node on it."""
    prepare_demo_keys(key_dir)
    nodes, servers = {}, []
    for bank in BANKS:
        db = fakeredis.FakeValkey(decode_responses=True)
        paths = pair_paths(key_dir, bank)
        index = TokenIndex(bank, "k1", paths)
        scorer = Scorer(bank, state=ValkeyState(db), token_index=index)
        for e in events[bank]:
            scorer.process(e)
        local = ValkeyBank(db, bank, "k1", paths)
        signing, keys = own_key(key_dir, bank), public_keys(key_dir, bank)
        url = f"http://case-{bank}"
        nodes[bank] = Node(local, Store(), signing, keys, {}, case_url=url)
    ports = {}
    for bank, node in nodes.items():
        server, ports[bank] = serve(node)
        servers.append(server)
    for bank, node in nodes.items():
        node.peers.update({b: f"localhost:{p}" for b, p in ports.items() if b != bank})
    return nodes, servers


@pytest.fixture
def run(tmp_path, monkeypatch):
    """Start the nodes on events; capture what each posts to its case service."""
    posted, servers = {b: [] for b in BANKS}, []

    def urlopen(request, timeout):
        bank = request.full_url.removeprefix("http://case-")[0]
        posted[bank].append(json.loads(request.data))
        return io.BytesIO()

    monkeypatch.setattr("urllib.request.urlopen", urlopen)

    def go(events):
        nodes, started = start(tmp_path, events)
        servers.extend(started)
        return nodes, posted

    yield go
    for server in servers:
        server.stop(0)


def tiny():
    return {b: list(read_events(TINY / f"bank_{b}.csv")) for b in BANKS}


def cases_at(node, messages, tmp_path):
    store = CaseStore(str(tmp_path / f"cases-{node.bank}.db"))
    for m in messages:
        store.record(parse(m, node.public_keys, node.bank))
    return {
        (
            node.bank,
            c["tier"],
            " ".join(sorted({leg["payment_id"] for leg in c["legs"]})),
        )
        for c in (store.get(c["case_id"]) for c in store.cases())
    }


def test_fixture_reaches_the_case_store_as_the_offline_evaluator_expects(run, tmp_path):
    nodes, posted = run(tiny())
    got = set()
    for node in nodes.values():
        node.consume_hits(block_ms=None)
    for node in [*nodes.values()] * 2:  # twice: results delivered after a bank's pass
        node.housekeeping()
    for bank, node in nodes.items():
        got |= cases_at(node, posted[bank], tmp_path)
    # apps/evaluation's test_fixture_cases, keeping each bank's own payments: the
    # case service sees only its own legs
    o1, v1 = "p10 p11 p12", "p15 p16 p17 p18 p19 p20"
    expected = {(b, 2, ids) for b in "ABC" for ids in (o1, v1)} | {
        ("A", 1, "p06"),
        ("A", 1, "p08"),
        ("A", 1, "p13"),
        ("B", 1, "p05"),
        ("B", 1, "p06 p07"),
        ("B", 1, "p09"),
        ("C", 1, "p03"),
        ("C", 1, "p14"),
    }
    own = {b: {e["payment_id"] for e in events} for b, events in tiny().items()}
    assert got == {
        (b, tier, " ".join(sorted(set(ids.split()) & own[b])))
        for b, tier, ids in expected
    }
    assert all(n.store.pending_cases() == [] for n in nodes.values())


def test_intra_bank_hop_is_followed(run):
    def event(bank, pid, direction, account, other, minute, amount):
        return {
            "payment_id": pid,
            "event_time": f"2022-09-05T09:{minute:02}:00Z",
            "bank": bank,
            "direction": direction,
            "account": account,
            "counterparty_account": other,
            "counterparty_bank": other[0],
            "amount": amount,
            "payment_format": "GIRO",
        }

    events = {
        "A": [event("A", "q1", "out", "A-a", "B-x", 0, "10000")],
        "B": [
            event("B", "q1", "in", "B-x", "A-a", 0, "10000"),
            event("B", "q2", "out", "B-x", "B-y", 10, "9800"),
            event("B", "q2", "in", "B-y", "B-x", 10, "9800"),
            event("B", "q3", "out", "B-y", "C-z", 20, "9500"),
        ],
        "C": [event("C", "q3", "in", "C-z", "B-y", 20, "9500")],
    }
    nodes, posted = run(events)
    assert nodes["B"].local.links("q1", "in") == ["q3"]
    assert nodes["B"].local.links("q3", "out") == ["q1"]
    nodes["B"].consume_hits(block_ms=None)
    nodes["B"].housekeeping()  # delivers to A and C
    for node in nodes.values():
        [chain] = node.store.chains()
        assert chain.state == pb.CONFIRMED
        assert [c.signer for c in chain.claims] == ["A", "B", "C"]
    [message] = [m for m in posted["B"] if m["kind"] == "chain"]
    assert [leg["payment_id"] for leg in message["legs"]] == ["q1", "q3"]


def test_a_failed_hit_is_retried_and_a_lost_group_recreated(run, monkeypatch, tmp_path):
    nodes, _ = run(tiny())
    node, calls = nodes["B"], []
    first = node.local.new_hits(block_ms=None)
    restarted = ValkeyBank(node.local.db, "B", "k1", pair_paths(tmp_path, "B"))
    assert first and restarted.new_hits(block_ms=None) == first  # not acknowledged

    def fail_once(hit, search):
        calls.append(hit["hit_id"])
        if len(calls) == 1:
            raise ConnectionError("Valkey restarting")

    monkeypatch.setattr(node, "handle_hit", fail_once)
    with pytest.raises(ConnectionError):
        node.consume_hits(block_ms=None)
    node.consume_hits(block_ms=None)
    assert calls[1] == calls[0]  # read again, not skipped
    assert node.consume_hits(block_ms=None) == 0  # all acknowledged
    node.local.db.flushall()  # Valkey restarted with no persistence
    with pytest.raises(ResponseError):
        node.consume_hits(block_ms=None)  # the main loop logs it and retries
    assert node.consume_hits(block_ms=None) == 0
    node.local.db.xadd("hits", {"hit": "h1"})
    node.local.db.hset("hit:h1", mapping={"rule": "new_payee", "payments": "[]"})
    assert node.consume_hits(block_ms=None) == 1


def test_case_service_rejection_is_dead_lettered_and_outage_retried(run, monkeypatch):
    nodes, _ = run({b: [] for b in BANKS})
    node = nodes["B"]
    node.store.put_case({"key": "h1", "kind": "hit", "rule": "new_payee", "legs": []})

    def down(request, timeout):
        raise urllib.error.URLError("refused")

    monkeypatch.setattr("urllib.request.urlopen", down)
    node.deliver_cases()
    assert [k for k, _, _ in node.store.pending_cases()] == ["h1"]

    def reject(request, timeout):
        raise urllib.error.HTTPError(request.full_url, 422, "", None, None)

    monkeypatch.setattr("urllib.request.urlopen", reject)
    node.deliver_cases()
    assert node.store.pending_cases() == []
