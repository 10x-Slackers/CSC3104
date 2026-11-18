"""The protocol store on SQLite and, with NODE_TEST_DATABASE_URL set, PostgreSQL."""

import json
import os
from pathlib import Path

import psycopg
import pytest

from privacy_node import privacy_node_pb2 as pb
from privacy_node.demo import start_banks
from privacy_node.node import Node, serve
from privacy_node.store import Store

TINY = Path(__file__).parents[2] / "dataset/fixtures/tiny"


# Set NODE_TEST_DATABASE_URL=postgresql://... to run these on PostgreSQL too.
PG = os.environ.get("NODE_TEST_DATABASE_URL")


@pytest.fixture(params=["sqlite"] + (["postgres"] if PG else []))
def url(request, tmp_path):
    if request.param == "sqlite":
        return str(tmp_path / "protocol.db")
    return request.getfixturevalue("pg")


@pytest.fixture
def pg():
    if not PG:
        pytest.skip("needs NODE_TEST_DATABASE_URL")
    with psycopg.connect(PG, autocommit=True) as conn:
        conn.execute(
            "DROP TABLE IF EXISTS replies, searches, chains, outbox, case_outbox"
        )
    return PG


def test_round_trip(url):
    store = Store(url)
    reply = pb.TraceReply(state=pb.CONFIRMED)
    store.put_reply("q", b"\x00ref", reply)
    store.put_reply("q", b"\x00ref", pb.TraceReply(state=pb.PARTIAL))  # first one stays
    assert store.get_reply("q", b"\x00ref") == reply
    store.put_search("s", "p1", "p2", pb.PENDING, "")
    assert store.unfinished_searches() == [("p1", "p2")]
    partial = pb.ChainResult(chain_digest="d", state=pb.PARTIAL)
    case = {"key": "d", "kind": "chain", "legs": []}
    store.save_chain(partial, ["A"], case)
    store.ack(partial, "A")
    [(_, _, sent)] = store.pending_cases()
    store.ack_case("d", "chain", sent)
    store.save_chain(partial, ["A"], case)
    assert store.pending_deliveries() == [] and store.pending_cases() == []
    confirmed = pb.ChainResult(chain_digest="d", state=pb.CONFIRMED)
    store.save_chain(confirmed, ["A"], {"key": "d", "kind": "chain", "legs": [1]})
    assert store.pending_deliveries() == [("A", confirmed)]  # the upgrade is resent
    assert [k for k, _, m in store.pending_cases() if "[1]" in m] == ["d"]
    hit = {"key": "h", "kind": "hit", "legs": []}
    store.put_case(hit)
    store.dead_case("h", "hit", json.dumps(hit))
    assert [k for k, _, _ in store.pending_cases()] == ["d"]


def test_one_replica_holds_the_housekeeping_lock(pg):
    first, second = Store(pg), Store(pg)
    assert first.try_lock() and not second.try_lock()
    first.unlock()
    assert second.try_lock()
    second.db.close()  # the replica dies: PostgreSQL releases its lock
    assert first.try_lock()


def test_another_replica_resumes_a_pending_search(pg, tmp_path):
    nodes, servers = start_banks(TINY, tmp_path)
    b = nodes["B"]
    b.store = Store(pg)
    replica = Node(b.local, Store(pg), b.signing_key, b.public_keys, b.peers)
    port = int(b.peers["C"].split(":")[1])
    servers["C"].stop(0)
    assert b.search("p06", "p07").state == pb.PENDING
    servers["C"], _ = serve(nodes["C"], port)
    replica.housekeeping()  # the other replica finds it in the shared store
    assert [c.state for c in replica.store.chains()] == [pb.CONFIRMED]
    assert [c.state for c in nodes["C"].store.chains()] == [pb.CONFIRMED]
    for server in servers.values():
        server.stop(0)


def test_an_upgrade_isnt_acknowledged_by_the_older_deliverys_ack(url):
    store = Store(url)
    partial = pb.ChainResult(chain_digest="d", state=pb.PARTIAL)
    store.save_chain(partial, ["A"])
    [(_, sent)] = store.pending_deliveries()
    confirmed = pb.ChainResult(chain_digest="d", state=pb.CONFIRMED)
    store.save_chain(confirmed, ["A"])  # by the other replica, mid-delivery
    store.ack(sent, "A")
    assert store.pending_deliveries() == [("A", confirmed)]


def test_a_peers_postgresql_outage_leaves_the_search_pending(tmp_path, monkeypatch):
    nodes, servers = start_banks(TINY, tmp_path)

    def down(*args):
        raise psycopg.OperationalError("the connection is lost")

    monkeypatch.setattr(nodes["C"].store, "get_reply", down)
    assert nodes["B"].search("p06", "p07").state == pb.PENDING  # retried, not failed
    for server in servers.values():
        server.stop(0)


def test_pending_search_backs_off_with_jitter_then_expires(url, monkeypatch):
    clock = [1000.0]
    monkeypatch.setattr("privacy_node.store.time.time", lambda: clock[0])
    monkeypatch.setattr("privacy_node.store.random.random", lambda: 1.0)  # max jitter
    store = Store(url)
    for attempts, wait in enumerate([10, 20, 40, 80, 160, 300, 300]):
        store.put_search("s", "p1", "p2", pb.PENDING, "")
        clock[0] += wait - 1
        assert store.unfinished_searches() == [], attempts
        clock[0] += 1
        assert store.unfinished_searches() == [("p1", "p2")], attempts
    clock[0] = 1000 + 24 * 3600 + 1  # 24 h after the first attempt: unresolved
    assert store.unfinished_searches() == []


def test_stored_replies_expire(url, monkeypatch):
    clock = [1000.0]
    monkeypatch.setattr("privacy_node.store.time.time", lambda: clock[0])
    store = Store(url)
    store.put_reply("q", b"r", pb.TraceReply(state=pb.NO_MATCH))
    clock[0] += 600
    store.expire_replies()
    assert store.get_reply("q", b"r")
    clock[0] += 1
    store.expire_replies()
    assert store.get_reply("q", b"r") is None
