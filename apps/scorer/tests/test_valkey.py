import io
import json
import threading
from pathlib import Path
from unittest.mock import Mock

import fakeredis
import pytest
from confluent_kafka import KafkaError, KafkaException

from ingestor import read_events
from scorer.score import BANK_TRANSFERS, Scorer
from scorer.state import TTL, MemoryState, ValkeyState
from scorer.stream import key_index, serve, settings
from security import generate_agreement_key, private_key_pem, public_key_pem

FIXTURES = Path(__file__).parents[1] / "fixtures"
TINY = Path(__file__).parents[2] / "dataset/fixtures/tiny"
KAFKA_EVENTS = Path(__file__).parents[2] / "ingestor/fixtures/kafka_events.json"


@pytest.fixture
def db():
    return fakeredis.FakeValkey(decode_responses=True)


def sample_events():
    return [r["value"] for r in json.loads(KAFKA_EVENTS.read_text())]


def dump(db):
    """Every key in the sample's format."""
    out = {}
    for key in db.scan_iter():
        kind = db.type(key)
        if kind == "zset":
            members = db.zrange(key, 0, -1, withscores=True)
            out[key] = {"type": kind, "members": [[m, s] for m, s in members]}
        elif kind == "hash":
            out[key] = {"type": kind, "value": db.hgetall(key)}
        elif kind == "set":
            out[key] = {"type": kind, "members": sorted(db.smembers(key))}
        elif kind == "stream":
            out[key] = {"type": kind, "entries": [e for _, e in db.xrange(key)]}
        else:
            out[key] = {"type": kind, "value": db.get(key)}
    return out


def test_valkey_matches_the_shared_sample(db):
    scorer = Scorer("B", state=ValkeyState(db))
    for e in sample_events():
        scorer.process(e)
    expected = json.loads((FIXTURES / "valkey_state.json").read_text())["keys"]
    # tokens need real key files; the sample's are placeholders
    expected = {k: v for k, v in expected.items() if not k.startswith("tok:")}
    for value in expected.values():
        if isinstance(value.get("value"), dict):
            value["value"].pop("token", None)
    assert dump(db) == expected
    for key in db.scan_iter():
        assert db.ttl(key) == (-1 if key.startswith("last:") else TTL)


@pytest.mark.parametrize("bank", "ABC")
def test_valkey_and_memory_agree_on_the_tiny_fixture(db, bank):
    expected = json.loads((FIXTURES / "tiny_expected.json").read_text())[bank]
    live, offline = Scorer(bank, state=ValkeyState(db)), Scorer(bank)
    events = list(read_events(TINY / f"bank_{bank}.csv"))
    assert [live.process(e) for e in events] == [offline.process(e) for e in events]
    assert live.state.hits() == offline.state.hits()
    assert (
        sorted(live.state.hits(), key=lambda h: (h["rule"], h["payments"]))
        == (expected["hits"])
    )
    for e in events:
        assert live.state.links(e["payment_id"], e["direction"]) == (
            offline.state.links(e["payment_id"], e["direction"])
        )
        if e["payment_format"] in BANK_TRANSFERS:
            entry = offline.state.payment(e["payment_id"], e["direction"])
            assert live.state.payment(e["payment_id"], e["direction"]) == {
                k: str(v) for k, v in entry.items()
            }


def test_a_crash_before_exec_writes_nothing(db):
    state = ValkeyState(db)
    state.tx.execute = Mock(side_effect=ConnectionError("pod killed"))
    with pytest.raises(ConnectionError):
        Scorer("B", state=state).process(sample_events()[2])
    assert db.dbsize() == 0
    # the restarted pod gets the same event again and writes it once
    scorer = Scorer("B", state=ValkeyState(db))
    for e in sample_events():
        scorer.process(e)
    fresh = fakeredis.FakeValkey(decode_responses=True)
    clean = Scorer("B", state=ValkeyState(fresh))
    for e in sample_events():
        clean.process(e)
    assert dump(db) == dump(fresh)


def test_a_restart_reads_state_back_and_skips_repeats(db):
    first = Scorer("B", state=ValkeyState(db))
    for e in sample_events():
        first.process(e)
    before = dump(db)
    second = Scorer("B", state=ValkeyState(db))  # new replica, same Valkey
    assert [second.process(e) for e in sample_events()] == [[], [], [], []]
    assert second.fired == []
    assert dump(db) == before


@pytest.mark.parametrize("live", [False, True])
def test_an_event_exactly_twice_the_window_back_is_kept(db, live):
    scorer = Scorer("B", state=ValkeyState(db)) if live else Scorer("B")
    events = sample_events()
    p06, p07 = events[2], events[3]
    scorer.process(p06)  # in at 5 Sep 09:10
    later = p06 | {"payment_id": "later", "event_time": "2022-09-11T09:10:00Z"}
    scorer.process(later)  # 144 h on: p06 sits exactly on the trim line
    late = p07 | {"event_time": "2022-09-08T09:10:00Z"}  # 72 h after p06, 72 h late
    assert [x["rules"] for x in scorer.process(late)] == [["chain"]]


def test_fired_is_only_the_last_event(db):
    scorer = Scorer("B", state=ValkeyState(db))
    for e in sample_events():
        scorer.process(e)
    assert scorer.fired == ["chain", "new_payee"]  # p07
    scorer.process(sample_events()[0] | {"payment_id": "p99"})
    assert scorer.fired == []


def make_key_dir(root, banks="AB", version="k1"):
    keys = {bank: generate_agreement_key() for bank in banks}
    for bank, key in keys.items():
        base = root / bank
        (base / "private" / "x25519").mkdir(parents=True)
        (base / "private" / "x25519" / f"{version}.pem").write_bytes(
            private_key_pem(key)
        )
        # like the k3d keys, each bank also trusts its own public key
        for peer, peer_key in keys.items():
            path = base / "trusted" / "x25519" / peer / f"{version}.pem"
            path.parent.mkdir(parents=True)
            path.write_bytes(public_key_pem(peer_key.public_key()))
        active = {"active_versions": [version], "query_version": version}
        (base / "active.json").write_text(json.dumps(active))
    return root


def test_tokens_in_valkey_are_found_by_the_other_bank(db, tmp_path):
    make_key_dir(tmp_path)
    scorer = Scorer("B", state=ValkeyState(db), token_index=key_index("B", tmp_path))
    p06 = sample_events()[2]  # A-m0 -> B-m1
    scorer.process(p06)
    # Bank A derives the same token for p06 from its own side
    token = key_index("A", tmp_path).tokens(
        p06 | {"bank": "A", "counterparty_bank": "B"}
    )
    token = token["k1"].hex()
    assert scorer.state.lookup("A-B", "k1", token) == {
        "payment_id": "p06",
        "direction": "in",
        "account": "B-m1",
        "amount": "10000.0",
    }
    assert scorer.state.payment("p06", "in")["token"] == token
    assert MemoryState().lookup("A-B", "k1", token) is None


def message(record):
    return Mock(
        **{
            "value.return_value": json.dumps(record["value"]).encode(),
            "key.return_value": record["key"].encode(),
            "topic.return_value": record["topic"],
            "timestamp.return_value": (1, 1793500000123),
            "error.return_value": None,
        }
    )


def stopping(stop, *items):
    """poll() results, then set stop."""
    yield from items
    stop.set()
    yield None


def test_serve_stores_offsets_and_logs_links_and_hits(db):
    stop = threading.Event()
    records = json.loads(KAFKA_EVENTS.read_text())
    consumer = Mock()
    transient = KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART, "not created yet")
    consumer.poll.side_effect = stopping(
        stop, Mock(**{"error.return_value": transient}), *map(message, records)
    )
    output = io.StringIO()
    serve(consumer, Scorer("B", state=ValkeyState(db)), stop, output)
    assert consumer.store_offsets.call_count == len(records)
    consumer.commit.assert_not_called()
    consumer.close.assert_called_once()
    error, line = [json.loads(x) for x in output.getvalue().splitlines()]
    assert error["level"] == "error" and "not created yet" in error["error"]
    assert line.pop("ts") > 0
    assert line == {
        "service": "scorer",
        "bank": "B",
        "payment_id": "p07",
        "kafka_ts": 1793500000.123,
        "links": [["p06", "p07"]],
        "hits": ["chain", "new_payee"],
    }


def test_serve_stops_on_a_wrongly_routed_record(db):
    stop = threading.Event()
    bad = json.loads(KAFKA_EVENTS.read_text())[0] | {"key": "someone-else"}
    consumer = Mock()
    consumer.poll.side_effect = stopping(stop, message(bad))
    with pytest.raises(ValueError, match="wrong topic or account key"):
        serve(consumer, Scorer("B", state=ValkeyState(db)), stop, io.StringIO())
    consumer.store_offsets.assert_not_called()
    consumer.close.assert_called_once()
    assert db.dbsize() == 0


def test_serve_stops_on_a_fatal_kafka_error(db):
    stop = threading.Event()
    fatal = KafkaError(KafkaError._FAIL, "broker gone", fatal=True)
    consumer = Mock()
    consumer.poll.side_effect = stopping(stop, Mock(**{"error.return_value": fatal}))
    with pytest.raises(KafkaException):
        serve(consumer, Scorer("B", state=ValkeyState(db)), stop, io.StringIO())
    consumer.close.assert_called_once()


def test_settings_come_from_the_environment(monkeypatch):
    for name, value in {
        "SCORER_BANK": "C",
        "KAFKA_BOOTSTRAP_SERVERS": "kafka-kafka-bootstrap:9092",
        "VALKEY_URL": "valkey://valkey:6379",
        "KEY_DIR": "/keys",
    }.items():
        monkeypatch.setenv(name, value)
    args = settings([])
    assert (args.bank, args.bootstrap_servers, args.group) == (
        "C",
        "kafka-kafka-bootstrap:9092",
        "scorer-C",
    )
    assert (args.valkey_url, args.key_dir) == ("valkey://valkey:6379", "/keys")
    assert settings(["A", "--group", "rebuild-1"]).group == "rebuild-1"


def test_key_file_flags_override_key_dir_from_the_environment(monkeypatch):
    monkeypatch.setenv("KEY_DIR", "/keys")
    flags = ["--key-version", "k1", "--x25519-private", "/k/B.pem"]
    args = settings(["B", *flags, "--peer-public-key", "A", "/k/A.pem"])
    assert (args.key_dir, args.key_version) == (None, "k1")
    assert settings(["B"]).key_dir == "/keys"


@pytest.mark.parametrize(
    "argv",
    [
        [],
        ["B", "--key-version", "k1"],
        ["B", "--key-dir", "/keys", "--key-version", "k1", "--x25519-private", "x"]
        + ["--peer-public-key", "A", "y"],
    ],
)
def test_bad_settings_are_refused(monkeypatch, argv):
    monkeypatch.delenv("SCORER_BANK", raising=False)
    with pytest.raises(SystemExit):
        settings(argv)
