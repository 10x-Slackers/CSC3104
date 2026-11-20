"""The four rules, and the properties that make replaying the log safe."""

from datetime import UTC, datetime, timedelta

import fakeredis
import pytest

from mulechain.scorer import Scorer

T0 = datetime(2022, 9, 5, 9, tzinfo=UTC)


def event(payment, direction, minutes, amount, peer_bank="B", account="A-1", payee="x-1"):
    return {
        "payment_id": payment,
        "direction": direction,
        "event_time": (T0 + timedelta(minutes=minutes)).isoformat(),
        "bank": "A",
        "account": account,
        "counterparty_account": payee,
        "counterparty_bank": peer_bank,
        "amount": amount,
        "payment_format": "GIRO",
    }


@pytest.fixture
def scorer():
    db = fakeredis.FakeValkey(decode_responses=True)
    return Scorer("A", db, lambda peer: [b"k" * 32])


def test_chain_links_money_passed_on_across_banks(scorer):
    scorer.process(event("in", "in", 0, 1000, "B"))
    made = scorer.process(event("out", "out", 120, 1050, "C"))
    assert made == {"links": [["in", "out"]], "hits": ["chain", "new_payee"]}


def test_fast_pass_through_inside_the_bank_links_without_a_hit(scorer):
    scorer.process(event("in", "in", 0, 1000, "A"))
    made = scorer.process(event("out", "out", 10, 950, "A"))
    assert made == {"links": [["in", "out"]], "hits": []}


@pytest.mark.parametrize(
    ("minutes", "amount"),
    [(0.5, 950), (120, 500), (120, 1200), (73 * 60, 1000)],
    ids=["too soon", "too little", "too much", "too late"],
)
def test_no_link_outside_the_ratio_or_delay(scorer, minutes, amount):
    scorer.process(event("in", "in", 0, 1000, "B"))
    assert scorer.process(event("out", "out", minutes, amount, "C"))["links"] == []


def test_new_payee_then_quick_repeat_within_a_day(scorer):
    assert scorer.process(event("p1", "out", 0, 100, "B"))["hits"] == ["new_payee"]
    assert scorer.process(event("p2", "out", 60, 100, "B"))["hits"] == ["quick_repeat"]
    assert scorer.process(event("p3", "out", 26 * 60, 100, "B"))["hits"] == []
    assert scorer.process(event("p4", "out", 27 * 60, 100, "A"))["hits"] == []  # same bank


def test_event_time_decides_not_arrival_order(scorer):
    scorer.process(event("out", "out", 120, 1050, "C"))
    made = scorer.process(event("in", "in", 0, 1000, "B"))  # arrives late, happened first
    assert made["links"] == [["in", "out"]]


def test_a_replayed_event_changes_nothing(scorer):
    events = [event("in", "in", 0, 1000, "B"), event("out", "out", 120, 1050, "C")]
    for e in events:
        scorer.process(e)
    before = {key: scorer.db.dump(key) for key in scorer.db.scan_iter() if key != "hits"}
    hits = scorer.db.xlen("hits")
    assert [scorer.process(e) for e in events] == [{"links": [], "hits": []}] * 2
    assert {key: scorer.db.dump(key) for key in before} == before
    assert scorer.db.xlen("hits") == hits


def test_a_worker_stops_before_writing_to_an_emptied_valkey(scorer):
    scorer.process(event("in", "in", 0, 1000, "B"), partition=3, offset=7)
    scorer.db.flushall()  # Valkey restarted with nothing in it
    with pytest.raises(RuntimeError):
        scorer.process(event("out", "out", 120, 1050, "C"), partition=3, offset=8)
    assert scorer.db.keys() == []  # nothing scored against the missing history


def test_an_event_later_than_the_allowed_lateness_is_not_scored(scorer):
    scorer.process(event("new", "out", 80 * 60, 1000, "C"))
    made = scorer.process(event("old", "in", 0, 1000, "B"))  # 80 h behind the account
    assert made == {"links": [], "hits": []}
    assert scorer.db.zcard("win:A-1") == 1


def test_a_repeat_after_a_key_change_keeps_the_token_the_peer_indexed():
    keys = [b"k" * 32]
    scorer = Scorer("A", fakeredis.FakeValkey(decode_responses=True), lambda peer: keys)
    scorer.process(event("p1", "out", 0, 1000))
    first = scorer.db.hget("pay:p1:out", "token")
    keys.insert(0, b"n" * 32)  # the peer published a newer key, then the log is read again
    scorer.process(event("p1", "out", 0, 1000))
    assert scorer.db.hget("pay:p1:out", "token") == first
    assert len(scorer.db.keys("tok:*")) == 2  # still findable under the new pair too
