import csv
import io
import json
import subprocess
import sys
import threading
from pathlib import Path
from unittest.mock import Mock

import pytest
from confluent_kafka import KafkaError, KafkaException, TopicPartition

from scorer.score import Scorer
from scorer.stream import consume, require_full_replay
from scorer.token_index import TokenIndex
from security import canonical_pair, derive_token

TINY = Path(__file__).parents[2] / "dataset/fixtures/tiny"
# Shared with the ingestor: the records it writes for one batch to Bank B
KAFKA_EVENTS = Path(__file__).parents[2] / "ingestor/fixtures/kafka_events.json"


def event(payment="in", direction="in", amount=100, time="09:00:00", **changes):
    return {
        "payment_id": payment,
        "direction": direction,
        "amount": amount,
        "event_time": f"2022-09-05T{time}Z",
        "bank": "B",
        "account": "bob",
        "counterparty_bank": "A",
        "payment_format": "ACH",
    } | changes


@pytest.mark.parametrize(
    "amount,time,expected",
    [
        (90, "09:01:00", True),
        (98, "09:30:00", True),
        (89.99, "09:10:00", False),
        (98.01, "09:10:00", False),
        (95, "09:00:59", False),
        (95, "09:30:01", False),
    ],
)
def test_boundaries(amount, time, expected):
    scorer = Scorer("B")
    assert scorer.process(event()) == []
    result = scorer.process(event("out", "out", amount, time))
    assert bool(result) == expected
    if result:
        assert result[0]["ratio"] == amount / 100


def test_duplicates_and_local_accounts():
    scorer = Scorer("B")
    incoming = event()
    outgoing = event("out", "out", 95, "09:05:00")
    scorer.process(incoming)
    assert scorer.process(incoming) == []
    assert scorer.process(outgoing | {"account": "someone_else"}) == []
    assert len(scorer.process(outgoing)) == 1
    assert scorer.process(outgoing) == []


def test_out_of_order_and_bounds():
    scorer = Scorer("B")
    scorer.process(event("out", "out", 95, "09:05:00"))
    assert len(scorer.process(event())) == 1
    scorer.process(event("future", time="11:00:00"))
    assert scorer.process(event()) == []
    assert len(scorer.history) == 1
    scorer = Scorer("B", max_events=2)
    for i in range(10):
        scorer.process(event(str(i)))
    assert len(scorer.history) == 2


def test_account_time_windows():
    scorer = Scorer("B")
    outgoing = event("out", "out", 95, "09:05:00")
    scorer.process(outgoing)
    scorer.process(event("alice-new", account="alice", time="12:00:00"))
    results = scorer.process(event())
    assert len(results) == 1
    assert (results[0]["incoming_payment_id"], results[0]["outgoing_payment_id"]) == (
        "in",
        "out",
    )
    assert scorer.process(event()) == []
    assert scorer.process(outgoing) == []
    scorer.process(event("bob-new", time="11:00:00"))
    assert scorer.process(event()) == []
    assert set(scorer.accounts) == {"alice", "bob"}
    assert len(scorer.history) == 2


def test_interleaved_account_capacity_eviction():
    scorer = Scorer("B", max_events=2)
    scorer.process(event("alice-new", account="alice", time="12:00:00"))
    scorer.process(event())
    assert len(scorer.process(event("out", "out", 95, "09:05:00"))) == 1
    assert set(scorer.accounts) == {"bob"}
    assert set(scorer.latest) == {"alice", "bob"}
    assert len(scorer.history) == 2
    assert scorer.process(event()) == []


def test_capacity_eviction_keeps_watermark():
    scorer = Scorer("B", max_events=1)
    scorer.process(event("x", time="12:00:00"))
    scorer.process(event("y", account="alice", time="12:00:00"))
    assert scorer.process(event()) == []
    assert scorer.process(event("out", "out", 95, "09:05:00")) == []


def test_late_event_finds_expired_window_partner():
    scorer = Scorer("B")
    scorer.process(event())
    scorer.process(event("later", time="09:40:00"))
    assert len(scorer.process(event("out", "out", 95, "09:20:00"))) == 1


@pytest.mark.parametrize(
    "bank,pairs",
    [
        ("A", {("p17", "p18")}),
        ("B", {("p06", "p07"), ("p10", "p11"), ("p15", "p16"), ("p18", "p19")}),
        ("C", {("p11", "p12"), ("p16", "p17"), ("p19", "p20")}),
    ],
)
def test_tiny(bank, pairs):
    with (TINY / f"bank_{bank}.csv").open() as source:
        events = list(csv.DictReader(source))

    def run():
        scorer = Scorer(bank)
        return [r for e in events for r in scorer.process(e)]

    results = run()
    assert results == run()
    assert {
        (r["incoming_payment_id"], r["outgoing_payment_id"]) for r in results
    } == pairs


def message(e):
    return Mock(
        **{
            "value.return_value": json.dumps(e).encode(),
            "key.return_value": e["account"].encode(),
            "topic.return_value": "bank-B-transactions",
            "error.return_value": None,
        }
    )


def test_consumer():
    consumer = Mock(**{"commit.return_value": []})
    transient = KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART, "not created yet")
    consumer.poll.side_effect = [
        Mock(**{"error.return_value": transient}),
        message(event()),
        message(event("out", "out", 95, "09:05:00")),
        KeyboardInterrupt,
    ]
    output = io.StringIO()
    with pytest.raises(KeyboardInterrupt):
        consume(consumer, Scorer("B"), output)
    assert json.loads(output.getvalue())["incoming_payment_id"] == "in"
    assert consumer.commit.call_count == 2
    assert consumer.commit.call_args.kwargs["asynchronous"] is False
    consumer.close.assert_called_once()


def test_consumer_reads_ingestor_sample():
    records = json.loads(KAFKA_EVENTS.read_text())
    messages = []
    for r in records:
        m = message(r["value"])
        m.topic.return_value = r["topic"]
        m.key.return_value = r["key"].encode()
        messages.append(m)
    consumer = Mock(**{"commit.return_value": []})
    consumer.poll.side_effect = [*messages, KeyboardInterrupt]
    scorer, seen = Scorer("B"), []

    def process(e):
        seen.append(e)
        return scorer.process(e)

    output = io.StringIO()
    with pytest.raises(KeyboardInterrupt):
        consume(consumer, scorer, output, process_event=process)
    # events reach the scorer whole, counterparty_account included
    assert seen == [r["value"] for r in records]
    [result] = [json.loads(line) for line in output.getvalue().splitlines()]
    assert (result["account"], result["incoming_payment_id"]) == ("B-m1", "p06")
    assert result["outgoing_payment_id"] == "p07"
    assert consumer.commit.call_count == len(records)


@pytest.mark.parametrize(
    "failure", ["flush", "commit", "partition", "poll", "subscribe"]
)
def test_consumer_failures(failure):
    consumer = Mock(**{"commit.return_value": []})
    consumer.poll.side_effect = [
        message(event()),
        AssertionError("polled after failure"),
    ]
    output = Mock()
    error = KafkaError(KafkaError._FAIL, "test failure", fatal=True)
    if failure == "flush":
        output.flush.side_effect = OSError("output failed")
    elif failure == "commit":
        consumer.commit.side_effect = KafkaException(error)
    elif failure == "partition":
        consumer.commit.return_value = [Mock(error=error)]
    elif failure == "poll":
        consumer.poll.side_effect = [Mock(**{"error.return_value": error})]
    else:
        consumer.subscribe.side_effect = KafkaException(error)
    with pytest.raises((OSError, KafkaException)):
        consume(consumer, Scorer("B"), output)
    if failure in ("flush", "poll", "subscribe"):
        consumer.commit.assert_not_called()
    else:
        output.flush.assert_called_once()
    consumer.close.assert_called_once()


@pytest.mark.parametrize("bank,count", [("A", 1), ("B", 4), ("C", 3)])
def test_consume_is_repeatable(bank, count):
    with (TINY / f"bank_{bank}.csv").open() as source:
        events = list(csv.DictReader(source))

    def run():
        consumer = Mock(**{"commit.return_value": []})
        messages = [message(e) for e in events]
        for m in messages:
            m.topic.return_value = f"bank-{bank}-transactions"
        consumer.poll.side_effect = [*messages, KeyboardInterrupt]
        output = io.StringIO()
        with pytest.raises(KeyboardInterrupt):
            consume(consumer, Scorer(bank), output)
        assert consumer.commit.call_count == len(events)
        return [json.loads(line) for line in output.getvalue().splitlines()]

    first = run()
    assert first == run()
    assert len(first) == count
    assert len({r["detection_id"] for r in first}) == count


def test_identity_and_account_index_eviction():
    incoming = event()
    outgoing = event("out", "out", 95, "09:05:00")

    def detect(bank, events):
        scorer = Scorer(bank)
        return next(r for e in events for r in scorer.process(e | {"bank": bank}))

    result = detect("B", [incoming, outgoing])
    assert result == detect("B", [outgoing, incoming])
    assert result["detection_id"] != detect("A", [incoming, outgoing])["detection_id"]
    assert (
        result["detection_id"]
        != detect("B", [incoming | {"payment_id": "other"}, outgoing])["detection_id"]
    )
    scorer = Scorer("B", max_events=1)
    scorer.process(incoming)
    scorer.process(event("unrelated", account="alice"))
    assert set(scorer.accounts) == {"alice"}
    assert scorer.process(outgoing) == []
    scorer.process(event("future", account="carol", time="10:00:00"))
    assert set(scorer.accounts) == {"carol"}


@pytest.mark.parametrize("bad", [message(event(bank="A")), message(event())])
def test_consumer_rejects_routing(bad):
    bad.key.return_value = b"wrong-account"
    consumer = Mock()
    consumer.poll.return_value = bad
    with pytest.raises(ValueError):
        consume(consumer, Scorer("B"), io.StringIO())
    consumer.commit.assert_not_called()
    consumer.close.assert_called_once()


@pytest.fixture
def pair_key_paths(tmp_path):
    paths = {}
    for pair, byte in [(("A", "B"), b"a"), (("A", "C"), b"b"), (("B", "C"), b"c")]:
        path = tmp_path / f"{pair[0]}-{pair[1]}.key"
        path.write_bytes(byte * 32)
        paths[pair] = path
    return paths


def test_fixture_token_indexes(pair_key_paths):
    endpoints = {}
    for bank in "ABC":
        paths = {pair: path for pair, path in pair_key_paths.items() if bank in pair}
        index = TokenIndex(bank, "k1", paths)
        rebuilt = TokenIndex(bank, "k1", paths)
        with (TINY / f"bank_{bank}.csv").open() as source:
            events = list(csv.DictReader(source))
        for e in events:
            token = index.add(e)
            assert index.add(e) == token
            assert rebuilt.add(e) == token
            if e["counterparty_bank"] == bank:
                assert token is None
                continue
            pair = canonical_pair(bank, e["counterparty_bank"])
            expected = [{k: e[k] for k in ("payment_id", "direction", "account")}]
            assert index.lookup(pair, "k1", token) == expected
            assert rebuilt.lookup(pair, "k1", token) == expected
            endpoints.setdefault((pair, e["payment_id"]), []).append(token)
    assert len(endpoints) == 19
    assert all(
        len(tokens) == 2 and tokens[0] == tokens[1] for tokens in endpoints.values()
    )


def test_token_scopes_versions_and_references(pair_key_paths, tmp_path):
    paths = {p: path for p, path in pair_key_paths.items() if "B" in p}
    index = TokenIndex("B", "k1", paths)
    token = index.add(event())
    assert token == derive_token("in", "A", "B", b"a" * 32, "k1")
    index.add(event(account="other"))
    assert len(index.lookup(("B", "A"), "k1", token)) == 2
    assert index.lookup(("A", "B"), "k2", token) == []
    assert index.lookup(("B", "C"), "k1", token) == []
    assert index.add(event(counterparty_bank="C")) != token
    with pytest.raises(ValueError, match="unrelated"):
        index.lookup(("A", "C"), "k1", token)
    assert index.add(event(counterparty_bank="B")) is None
    rotated_path = tmp_path / "rotated.key"
    rotated_path.write_bytes(b"d" * 32)
    rotated = TokenIndex("B", "k2", {("A", "B"): rotated_path})
    rotated_token = rotated.add(event())
    assert rotated_token != token
    assert rotated.lookup(("A", "B"), "k1", token) == []
    assert index.lookup(("A", "B"), "k2", rotated_token) == []
    same_key_new_version = TokenIndex("B", "k2", paths)
    assert same_key_new_version.add(event()) not in (token, rotated_token)
    other_key = TokenIndex("B", "k1", {("A", "B"): rotated_path})
    assert other_key.add(event()) != token


@pytest.mark.parametrize(
    "failure", ["empty", "short", "missing", "version", "unrelated", "reused"]
)
def test_invalid_index_keys(failure, pair_key_paths, tmp_path):
    paths = {("A", "B"): pair_key_paths["A", "B"]}
    version = "k1"
    if failure == "empty":
        paths = {}
    elif failure == "short":
        pair_key_paths["A", "B"].write_bytes(b"short")
    elif failure == "missing":
        paths = {("A", "B"): tmp_path / "absent.key"}
    elif failure == "version":
        version = "bad:version"
    elif failure == "unrelated":
        paths = {("A", "C"): pair_key_paths["A", "C"]}
    else:
        paths["B", "C"] = paths["A", "B"]
    with pytest.raises((ValueError, FileNotFoundError)):
        TokenIndex("B", version, paths)


def test_consumer_indexes_unflagged_and_preserves_results(pair_key_paths):
    with (TINY / "bank_B.csv").open() as source:
        events = list(csv.DictReader(source))
    paths = {p: path for p, path in pair_key_paths.items() if "B" in p}
    index = TokenIndex("B", "k1", paths)
    scorer = Scorer("B")
    expected = [r for e in events for r in scorer.process(e)]
    consumer = Mock(**{"commit.return_value": []})
    consumer.poll.side_effect = [
        *[message(e) for e in events for _ in range(2)],
        KeyboardInterrupt,
    ]
    output = io.StringIO()
    with pytest.raises(KeyboardInterrupt):
        consume(consumer, Scorer("B"), output, token_index=index)
    assert [json.loads(line) for line in output.getvalue().splitlines()] == expected
    assert consumer.commit.call_count == 2 * len(events)
    token = derive_token("p01", "A", "B", b"a" * 32, "k1")
    assert index.lookup(("A", "B"), "k1", token) == [
        {"payment_id": "p01", "direction": "in", "account": "B-bob"}
    ]


@pytest.mark.parametrize("failure", ["missing_pair", "wrong_bank"])
def test_index_failure_does_not_commit(failure, pair_key_paths):
    index = TokenIndex("B", "k1", {("B", "C"): pair_key_paths["B", "C"]})
    e = event()
    if failure == "wrong_bank":
        index = TokenIndex("A", "k1", {("A", "B"): pair_key_paths["A", "B"]})
    consumer = Mock()
    consumer.poll.return_value = message(e)
    with pytest.raises(ValueError):
        consume(consumer, Scorer("B"), io.StringIO(), token_index=index)
    consumer.commit.assert_not_called()
    consumer.close.assert_called_once()


def test_late_unflagged_events_are_indexed(pair_key_paths):
    index = TokenIndex("B", "k1", {("A", "B"): pair_key_paths["A", "B"]})
    events = [event("new", time="12:00:00"), event(), event()]
    consumer = Mock(**{"commit.return_value": []})
    consumer.poll.side_effect = [*[message(e) for e in events], KeyboardInterrupt]
    output = io.StringIO()
    with pytest.raises(KeyboardInterrupt):
        consume(consumer, Scorer("B"), output, token_index=index)
    assert output.getvalue() == ""
    token = derive_token("in", "A", "B", b"a" * 32, "k1")
    assert len(index.lookup(("A", "B"), "k1", token)) == 1
    assert consumer.commit.call_count == 3


@pytest.mark.parametrize(
    "options",
    [
        ["--key-version", "k1"],
        ["--pairwise-key", "A", "absent.key"],
        ["--key-version", "k1", "--pairwise-key", "A", "absent.key"],
    ],
)
def test_index_cli_configuration(options):
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "scorer.stream",
            "B",
            *options,
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert result.returncode != 0
    assert "ModuleNotFoundError" not in result.stderr
    assert "Indexing requires" in result.stderr or "FileNotFoundError" in result.stderr


@pytest.mark.parametrize("module", ["baseline", "stream"])
def test_cli_runs_as_module(module):
    subprocess.run(
        [sys.executable, "-m", f"scorer.{module}", "--help"],
        check=True,
        capture_output=True,
        text=True,
    )


@pytest.mark.parametrize(
    "offset,low,partitions,accepted",
    [
        (-1001, 0, 1, True),
        (0, 0, 1, True),
        (15, 0, 1, False),
        (-1001, 5, 1, False),
        (-1001, 0, 2, False),
    ],
)
def test_integrated_replay_assignment(offset, low, partitions, accepted):
    consumer = Mock()
    consumer.list_topics.return_value.topics = {
        "bank-B-transactions": Mock(partitions=dict.fromkeys(range(partitions)))
    }
    consumer.get_watermark_offsets.return_value = (low, 15)
    consumer.committed.return_value = [TopicPartition("bank-B-transactions", 0, offset)]
    assignment = [TopicPartition("bank-B-transactions", 0)]
    if accepted:
        require_full_replay(consumer, assignment)
    else:
        with pytest.raises(RuntimeError):
            require_full_replay(consumer, assignment)


def test_restart_guard_stops_before_processing():
    consumer = Mock()
    consumer.list_topics.return_value.topics = {
        "bank-B-transactions": Mock(partitions={0: None})
    }
    consumer.get_watermark_offsets.return_value = (0, 15)
    consumer.committed.return_value = [TopicPartition("bank-B-transactions", 0, 15)]

    def subscribe(topics, on_assign):
        on_assign(consumer, [TopicPartition(topics[0], 0)])

    consumer.subscribe.side_effect = subscribe
    with pytest.raises(RuntimeError, match="fresh group"):
        consume(consumer, Scorer("B"), require_replay=True)
    consumer.poll.assert_not_called()
    consumer.commit.assert_not_called()
    consumer.close.assert_called_once()


def test_shutdown_finishes_current_handoff():
    stop = threading.Event()
    consumer = Mock()
    consumer.poll.return_value = message(event())
    consumer.commit.return_value = []

    def process(e):
        stop.set()
        return []

    consume(
        consumer, Scorer("B"), io.StringIO(), process_event=process, stop_event=stop
    )
    consumer.poll.assert_called_once()
    consumer.commit.assert_called_once()
    consumer.close.assert_called_once()


def test_token_capacity_never_evicts(pair_key_paths):
    index = TokenIndex(
        "B", "k1", {("A", "B"): pair_key_paths["A", "B"]}, max_references=1
    )
    token = index.add(event())
    assert index.add(event()) == token
    consumer = Mock()
    consumer.poll.side_effect = [
        message(event()),
        message(event("out", "out", 95, "09:05:00")),
    ]
    consumer.commit.return_value = []
    with pytest.raises(RuntimeError, match="no records were evicted"):
        consume(consumer, Scorer("B"), io.StringIO(), token_index=index)
    assert len(index.lookup(("A", "B"), "k1", token)) == 1
    assert consumer.commit.call_count == 1
    consumer.close.assert_called_once()
    with pytest.raises(ValueError, match="positive"):
        TokenIndex("B", "k1", {("A", "B"): pair_key_paths["A", "B"]}, max_references=0)
