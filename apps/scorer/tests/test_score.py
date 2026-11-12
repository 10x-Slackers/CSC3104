import io
import json
import subprocess
import sys
import threading
from pathlib import Path
from typing import cast
from unittest.mock import Mock

import pytest
from confluent_kafka import KafkaError, KafkaException, TopicPartition

from ingestor import Event, read_events
from scorer import score_file
from scorer.score import Scorer
from scorer.stream import consume, require_full_replay
from scorer.token_index import TokenIndex
from security import (
    AgreementFiles,
    canonical_pair,
    generate_agreement_key,
    private_key_pem,
    public_key_pem,
)

TINY = Path(__file__).parents[2] / "dataset/fixtures/tiny"
# Shared with the ingestor: the records it writes for one batch to Bank B
KAFKA_EVENTS = Path(__file__).parents[2] / "ingestor/fixtures/kafka_events.json"
# Shared with the offline evaluator: expected links and hits on the tiny fixture
TINY_EXPECTED = Path(__file__).parents[1] / "fixtures/tiny_expected.json"


def event(
    payment="in", direction="in", amount: float = 100, time="09:00:00", **changes
) -> Event:
    return cast(
        Event,
        {
            "payment_id": payment,
            "direction": direction,
            "amount": amount,
            "event_time": f"2022-09-05T{time}Z",
            "bank": "B",
            "account": "bob",
            "counterparty_account": "A-alice",
            "counterparty_bank": "A",
            "payment_format": "GIRO",
        }
        | changes,
    )


def out(payment="out", amount: float = 95, time="09:05:00", **changes) -> Event:
    """bob forwards money on to C-carol at Bank C."""
    to_c = {"counterparty_account": "C-carol", "counterparty_bank": "C"}
    return event(payment, "out", amount, time, **to_c | changes)


def run(scorer, *events):
    return [link for e in events for link in scorer.process(e)]


@pytest.mark.parametrize(
    "amount,time,rules",
    [
        (90, "09:01:00", ["fast", "chain"]),
        (98, "09:30:00", ["fast", "chain"]),
        (89.99, "09:10:00", []),
        (98.01, "09:10:00", ["chain"]),
        (110, "09:10:00", ["chain"]),
        (110.01, "09:10:00", []),
        (95, "09:00:59", []),
        (95, "09:30:01", ["chain"]),
    ],
)
def test_link_rules(amount, time, rules):
    links = run(Scorer("B"), event(), out(amount=amount, time=time))
    assert [link["rules"] for link in links] == ([rules] if rules else [])
    if links:
        assert links[0]["ratio"] == pytest.approx(amount / 100)


@pytest.mark.parametrize("time,linked", [("09:00:00", True), ("09:00:01", False)])
def test_chain_window_is_72_hours(time, linked):
    links = run(Scorer("B"), event(), out(event_time=f"2022-09-08T{time}Z"))
    assert bool(links) == linked


def test_chain_needs_a_leg_with_another_bank():
    inside = {"counterparty_bank": "B", "counterparty_account": "B-dan"}
    assert run(Scorer("B"), event(**inside), out(time="10:00:00", **inside)) == []
    assert run(Scorer("B"), event(**inside), out(time="10:00:00"))[0]["rules"] == [
        "chain"
    ]


def test_only_bank_transfers():
    assert run(Scorer("B"), event(payment_format="Cheque"), out()) == []


def test_repeats_and_either_order():
    scorer = Scorer("B")
    links = run(scorer, out(), event(), event(), out())
    assert [(x["incoming_payment_id"], x["outgoing_payment_id"]) for x in links] == [
        ("in", "out")
    ]
    assert len(scorer.state.hits()) == 2  # chain and new payee, written once


def test_late_events_and_trim():
    scorer = Scorer("B")
    scorer.process(out())
    # up to 72 h behind the account's latest event is still matched
    assert run(scorer, event(event_time="2022-09-02T09:05:00Z", amount=95)) != []
    assert run(scorer, event("late", event_time="2022-09-02T09:04:59Z")) == []
    assert ("late", "in") not in scorer.state.window("bob")
    # a much later event trims everything older than twice the window
    scorer.process(event("new", event_time="2022-09-20T09:00:00Z"))
    assert set(scorer.state.window("bob")) == {("new", "in")}


def test_late_event_finds_partner_kept_for_twice_the_window():
    scorer = Scorer("B")
    scorer.process(event())
    scorer.process(event("later", event_time="2022-09-09T13:00:00Z"))  # 100 h on
    late = out(event_time="2022-09-06T15:00:00Z")  # 30 h after the in leg, 70 h late
    assert [x["incoming_payment_id"] for x in scorer.process(late)] == ["in"]


def test_late_payment_keeps_the_newer_last_paid():
    scorer = Scorer("B")
    scorer.process(out("new", time="12:00:00"))
    scorer.process(out("old", time="09:00:00"))
    assert (
        scorer.state.last_paid("bob", "C-carol")[0]
        == scorer.state.window("bob")["new", "out"][0]
    )
    assert [h["rule"] for h in scorer.state.hits()] == ["new_payee"]


@pytest.mark.parametrize(
    "second,repeat", [("15:00:00", True), ("2022-09-06T09:00:01Z", False)]
)
def test_new_payee_and_quick_repeat(second, repeat):
    # issue #27's example: 09:00 new payee, 15:00 quick repeat, 16:00 nothing
    scorer = Scorer("A")

    def pay(payment, time):
        when = time if "T" in time else f"2022-09-05T{time}Z"
        return out(payment, 100, bank="A", account="A-x", event_time=when)

    run(
        scorer,
        pay("q1", "09:00:00"),
        pay("q2", second),
        pay("q3", "2022-09-06T10:00:00Z"),
    )
    hits = [(h["rule"], h["payments"]) for h in scorer.state.hits()]
    assert hits == [("new_payee", ["q1"])] + (
        [("quick_repeat", ["q2"])] if repeat else []
    )


def test_no_new_payee_inside_the_bank():
    scorer = Scorer("B")
    scorer.process(out(counterparty_bank="B", counterparty_account="B-dan"))
    assert scorer.state.hits() == []


@pytest.mark.parametrize(
    "bank,pairs",
    [
        ("A", {("p17", "p18")}),
        ("B", {("p06", "p07"), ("p10", "p11"), ("p15", "p16"), ("p18", "p19")}),
        ("C", {("p11", "p12"), ("p16", "p17"), ("p19", "p20")}),
    ],
)
def test_tiny(bank, pairs):
    events = list(read_events(TINY / f"bank_{bank}.csv"))

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


@pytest.mark.parametrize("bank", "ABC")
def test_score_file_matches_library_sample(bank):
    expected = json.loads(TINY_EXPECTED.read_text())[bank]
    scorer = Scorer(bank)
    events = read_events(TINY / f"bank_{bank}.csv")
    links = [link for e in events for link in scorer.process(e)]
    found = sorted(
        (
            {
                "incoming": x["incoming_payment_id"],
                "outgoing": x["outgoing_payment_id"],
                "account": x["account"],
                "rules": x["rules"],
            }
            for x in links
        ),
        key=lambda x: (x["incoming"], x["outgoing"]),
    )
    assert found == expected["links"]
    hits = sorted(scorer.state.hits(), key=lambda h: (h["rule"], h["payments"]))
    assert hits == expected["hits"]
    assert score_file(TINY, bank).hits() == scorer.state.hits()


@pytest.mark.parametrize("bank,count", [("A", 1), ("B", 4), ("C", 3)])
def test_consume_is_repeatable(bank, count):
    events = list(read_events(TINY / f"bank_{bank}.csv"))

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


def test_detection_identity():
    incoming, outgoing = event(), out()

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
    for bank in "ABC":
        key = generate_agreement_key()
        (tmp_path / f"{bank}-private.pem").write_bytes(private_key_pem(key))
        (tmp_path / f"{bank}-public.pem").write_bytes(public_key_pem(key.public_key()))
    return tmp_path


def key_files(root, bank, peers=None):
    return AgreementFiles(
        root / f"{bank}-private.pem",
        {peer: root / f"{peer}-public.pem" for peer in (peers or set("ABC") - {bank})},
    )


def test_fixture_token_indexes(pair_key_paths):
    endpoints = {}
    for bank in "ABC":
        paths = key_files(pair_key_paths, bank)
        index = TokenIndex(bank, "k1", paths)
        rebuilt = TokenIndex(bank, "k1", paths)
        events = list(read_events(TINY / f"bank_{bank}.csv"))
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
    paths = key_files(pair_key_paths, "B")
    index = TokenIndex("B", "k1", paths)
    token = index.add(event())
    assert isinstance(token, bytes) and len(token) == 16
    index.add(event(account="other"))
    assert len(index.lookup(("B", "A"), "k1", token)) == 2
    assert index.lookup(("A", "B"), "k2", token) == []
    assert index.lookup(("B", "C"), "k1", token) == []
    assert index.add(event(counterparty_bank="C")) != token
    with pytest.raises(ValueError, match="unrelated"):
        index.lookup(("A", "C"), "k1", token)
    assert index.add(event(counterparty_bank="B")) is None
    rotated_key = generate_agreement_key()
    rotated_path = tmp_path / "rotated.pem"
    rotated_path.write_bytes(private_key_pem(rotated_key))
    rotated = TokenIndex(
        "B", "k2", AgreementFiles(rotated_path, paths.peer_public_keys)
    )
    rotated_token = rotated.add(event())
    assert rotated_token != token
    assert rotated.lookup(("A", "B"), "k1", token) == []
    assert index.lookup(("A", "B"), "k2", rotated_token) == []
    same_key_new_version = TokenIndex("B", "k2", paths)
    assert same_key_new_version.add(event()) not in (token, rotated_token)
    other_key = TokenIndex(
        "B", "k1", AgreementFiles(rotated_path, paths.peer_public_keys)
    )
    assert other_key.add(event()) != token


@pytest.mark.parametrize(
    "failure", ["empty", "short", "missing", "version", "same_bank", "unknown"]
)
def test_invalid_index_keys(failure, pair_key_paths, tmp_path):
    files = key_files(pair_key_paths, "B", "A")
    private, peers = files.private_key, dict(files.peer_public_keys)
    version = "k1"
    if failure == "empty":
        peers = {}
    elif failure == "short":
        private.write_bytes(b"short")
    elif failure == "missing":
        peers = {"A": tmp_path / "absent.pem"}
    elif failure == "version":
        version = "bad:version"
    elif failure == "same_bank":
        peers = {"B": peers["A"]}
    else:
        peers = {"D": peers["A"]}
    with pytest.raises((ValueError, FileNotFoundError)):
        TokenIndex("B", version, AgreementFiles(private, peers))


def test_consumer_indexes_unflagged_and_preserves_results(pair_key_paths):
    events = list(read_events(TINY / "bank_B.csv"))
    paths = key_files(pair_key_paths, "B")
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
    token = index.token_for("p01", ("A", "B"), "k1")
    assert index.lookup(("A", "B"), "k1", token) == [
        {"payment_id": "p01", "direction": "in", "account": "B-bob"}
    ]


@pytest.mark.parametrize("failure", ["missing_pair", "wrong_bank"])
def test_index_failure_does_not_commit(failure, pair_key_paths):
    index = TokenIndex("B", "k1", key_files(pair_key_paths, "B", "C"))
    e = event()
    if failure == "wrong_bank":
        index = TokenIndex("A", "k1", key_files(pair_key_paths, "A", "B"))
    consumer = Mock()
    consumer.poll.return_value = message(e)
    with pytest.raises(ValueError):
        consume(consumer, Scorer("B"), io.StringIO(), token_index=index)
    consumer.commit.assert_not_called()
    consumer.close.assert_called_once()


def test_late_unflagged_events_are_indexed(pair_key_paths):
    index = TokenIndex("B", "k1", key_files(pair_key_paths, "B", "A"))
    events = [event("new", time="12:00:00"), event(), event()]
    consumer = Mock(**{"commit.return_value": []})
    consumer.poll.side_effect = [*[message(e) for e in events], KeyboardInterrupt]
    output = io.StringIO()
    with pytest.raises(KeyboardInterrupt):
        consume(consumer, Scorer("B"), output, token_index=index)
    assert output.getvalue() == ""
    token = index.token_for("in", ("A", "B"), "k1")
    assert len(index.lookup(("A", "B"), "k1", token)) == 1
    assert consumer.commit.call_count == 3


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
    index = TokenIndex("B", "k1", key_files(pair_key_paths, "B", "A"), max_references=1)
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
        TokenIndex("B", "k1", key_files(pair_key_paths, "B", "A"), max_references=0)


def test_overlapping_rotation_no_missed_link(pair_key_paths):
    old = key_files(pair_key_paths, "B")
    index = TokenIndex("B", "k1", old)
    old_token = index.add(event())
    # HKDF includes the version, even when provisioned X25519 files are unchanged.
    index.introduce_version("k2", old)
    assert index.active_versions == ("k1", "k2")
    assert index.lookup(("A", "B"), "k1", old_token)
    backfilled = index.token_for("in", ("A", "B"), "k2")
    assert index.lookup(("A", "B"), "k2", backfilled) == [
        {"payment_id": "in", "direction": "in", "account": "bob"}
    ]
    assert index.add(event("overlap")) == index.token_for("overlap", ("A", "B"), "k1")
    tokens = index.tokens(event("overlap"))
    assert tokens["k1"] != tokens["k2"]
    for version, token in tokens.items():
        assert index.lookup(("A", "B"), version, token) == [
            {"payment_id": "overlap", "direction": "in", "account": "bob"}
        ]
        assert index.lookup(("A", "B"), "k2" if version == "k1" else "k1", token) == []
    with pytest.raises(ValueError, match="every peer"):
        index.set_query_version("k2", {"A": ["k1", "k2"], "C": ["k1"]})
    index.set_query_version("k2", {"A": ["k1", "k2"], "C": ["k1", "k2"]})
    assert index.add(event("overlap")) == tokens["k2"]
    index.introduce_version("k1", old)
    assert index.lookup(("A", "B"), "k1", tokens["k1"])
    with pytest.raises(ValueError, match="Switch"):
        index.retire_version("k2")
    index.retire_version("k1")
    assert index.lookup(("A", "B"), "k1", tokens["k1"]) == []
    assert index.lookup(("A", "B"), "k2", tokens["k2"])
    assert index.add(event("after")) == index.token_for("after", ("A", "B"), "k2")


def test_rotation_capacity_failure_keeps_old_index(pair_key_paths, tmp_path):
    index = TokenIndex("B", "k1", key_files(pair_key_paths, "B", "A"), max_references=1)
    token = index.add(event())
    fresh = tmp_path / "fresh"
    fresh.mkdir()
    for bank in "AB":
        key = generate_agreement_key()
        (fresh / f"{bank}-private.pem").write_bytes(private_key_pem(key))
        (fresh / f"{bank}-public.pem").write_bytes(public_key_pem(key.public_key()))
    with pytest.raises(RuntimeError, match="no records were evicted"):
        index.introduce_version("k2", key_files(fresh, "B", "A"))
    assert index.active_versions == ("k1",)
    assert index.lookup(("A", "B"), "k1", token)


def test_changed_pair_key_requires_version_change(pair_key_paths):
    files = key_files(pair_key_paths, "B")
    index = TokenIndex("B", "k1", files)
    token = index.add(event())
    index.introduce_version("k1", files)
    assert index.add(event()) == token
    files.private_key.write_bytes(private_key_pem(generate_agreement_key()))
    with pytest.raises(ValueError, match="new version"):
        index.introduce_version("k1", files)


def test_incomplete_rotated_peer_set_cannot_drop_unseen_bank_pair(
    pair_key_paths, tmp_path
):
    index = TokenIndex("B", "k1", key_files(pair_key_paths, "B"))
    fresh = tmp_path / "fresh-peers"
    fresh.mkdir()
    for bank in "ABC":
        key = generate_agreement_key()
        (fresh / f"{bank}-private.pem").write_bytes(private_key_pem(key))
        (fresh / f"{bank}-public.pem").write_bytes(public_key_pem(key.public_key()))
    # No C transaction has arrived yet: readiness must still cover configured C.
    with pytest.raises(ValueError, match="same bank pairs"):
        index.introduce_version("k2", key_files(fresh, "B", "A"))
    assert index.active_versions == ("k1",)
    before = index.add(event("before", counterparty_bank="C"))
    assert index.lookup(("B", "C"), "k1", before)
    index.introduce_version("k2", key_files(fresh, "B"))
    with pytest.raises(ValueError, match="every peer"):
        index.set_query_version("k2", {"A": ["k1", "k2"], "C": ["k1"]})
    assert index.key_version == "k1"
    index.set_query_version("k2", {"A": ["k1", "k2"], "C": ["k1", "k2"]})
    after = index.add(event("after", counterparty_bank="C"))
    assert index.lookup(("B", "C"), "k2", after)
