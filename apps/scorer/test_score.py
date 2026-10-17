import csv
import io
import json
import subprocess
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest
from confluent_kafka import KafkaError, KafkaException
from score import Scorer, validate
from stream import consume, ingest

TINY = Path(__file__).parents[1] / "dataset/fixtures/tiny"


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


@pytest.mark.parametrize(
    "changes",
    [
        {"amount": 0},
        {"amount": -1},
        {"amount": "NaN"},
        {"amount": True},
        {"event_time": "bad"},
        {"event_time": "2022-09-05T09:00:00"},
        {"bank": "A"},
        {"direction": "other"},
        {"account": ""},
    ],
)
def test_invalid(changes):
    with pytest.raises(ValueError):
        Scorer("B").process(event(**changes))


def test_duplicates_and_local_accounts():
    scorer = Scorer("B")
    incoming = event()
    outgoing = event("out", "out", 95, "09:05:00")
    scorer.process(incoming)
    assert scorer.process(incoming) == []
    assert scorer.process(outgoing | {"account": "someone_else"}) == []
    assert len(scorer.process(outgoing)) == 1
    assert scorer.process(outgoing) == []
    assert "scenario" not in validate(incoming | {"scenario": "mule_chain"}, "B")


def test_out_of_order_and_bounds():
    scorer = Scorer("B")
    scorer.process(event("out", "out", 95, "09:05:00"))
    assert len(scorer.process(event())) == 1
    scorer.process(event("future", time="10:00:00"))
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
    scorer.process(event("bob-new", time="10:00:00"))
    assert scorer.process(event()) == []
    assert set(scorer.accounts) == {"alice", "bob"}
    assert len(scorer.history) == 2


def test_interleaved_account_capacity_eviction():
    scorer = Scorer("B", max_events=2)
    scorer.process(event("alice-new", account="alice", time="12:00:00"))
    scorer.process(event())
    assert len(scorer.process(event("out", "out", 95, "09:05:00"))) == 1
    assert set(scorer.accounts) == {"bob"}
    assert set(scorer.latest) == {"bob"}
    assert len(scorer.history) == 2
    assert scorer.process(event()) == []


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


def test_ingest():
    producer = Mock()
    producer.flush.return_value = 0
    ingest([json.dumps(event())], "B", producer)
    args, kwargs = producer.produce.call_args
    assert args == ("bank-B-transactions",)
    assert kwargs["key"] == "bob"
    assert json.loads(kwargs["value"]) == event()
    with pytest.raises(ValueError):
        ingest([json.dumps(event(bank="A"))], "B", producer)
    assert producer.produce.call_count == 1


def test_delivery_failure():
    producer = Mock()
    producer.flush.return_value = 0

    def fail(*args, **kwargs):
        kwargs["on_delivery"]("failed", None)

    producer.produce.side_effect = fail
    with pytest.raises(RuntimeError):
        ingest([json.dumps(event())], "B", producer)


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
    consumer = Mock()
    consumer.commit.return_value = []
    consumer.poll.side_effect = [
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


@pytest.mark.parametrize(
    "failure", ["flush", "commit", "partition", "poll", "subscribe"]
)
def test_consumer_failures(failure):
    consumer = Mock()
    consumer.commit.return_value = []
    consumer.poll.side_effect = [
        message(event()),
        AssertionError("polled after failure"),
    ]
    output = Mock()
    error = KafkaError(KafkaError._FAIL, "test failure")
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


@pytest.mark.parametrize("failure", ["flush_callback", "pending", "backpressure"])
def test_producer_failures(failure):
    producer = Mock()
    producer.flush.return_value = 0
    if failure == "pending":
        producer.flush.return_value = 1
    elif failure == "flush_callback":

        def flush(timeout):
            producer.produce.call_args.kwargs["on_delivery"]("delivery failed", None)
            return 0

        producer.flush.side_effect = flush
    else:
        producer.produce.side_effect = BufferError

        def poll(timeout):
            producer.produce.call_args.kwargs["on_delivery"]("delivery failed", None)

        producer.poll.side_effect = poll
    with pytest.raises(RuntimeError):
        ingest([json.dumps(event())], "B", producer)
    producer.flush.assert_called_once_with(30)
    if failure == "backpressure":
        producer.produce.assert_called_once()


def test_backpressure_timeout(monkeypatch):
    producer = Mock()
    producer.produce.side_effect = BufferError
    producer.flush.return_value = 0
    monkeypatch.setattr("stream.time.monotonic", Mock(side_effect=[0, 31]))
    with pytest.raises(TimeoutError, match="queue remained full"):
        ingest([json.dumps(event())], "B", producer)
    producer.produce.assert_called_once()
    producer.flush.assert_called_once_with(30)


def test_backpressure_recovers():
    producer = Mock()
    producer.produce.side_effect = [BufferError, None]
    producer.flush.return_value = 0
    ingest([json.dumps(event())], "B", producer)
    assert producer.produce.call_count == 2
    producer.poll.assert_any_call(0.1)


@pytest.mark.parametrize("bank,count", [("A", 1), ("B", 4), ("C", 3)])
def test_complete_replay_after_restart(bank, count):
    replay = subprocess.run(
        [
            sys.executable,
            str(TINY.parents[1] / "replay.py"),
            str(TINY),
            bank,
            "--speed",
            "0",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    events = [json.loads(line) for line in replay.stdout.splitlines()]

    def run():
        consumer = Mock()
        consumer.commit.return_value = []
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
