"""The outbox, the backoff and the search queue."""

import time

from mulechain.node import store
from mulechain.node.store import Store


def later(monkeypatch, seconds: float) -> None:
    now = time.time()
    monkeypatch.setattr(time, "time", lambda: now + seconds)


def test_backoff_doubles_up_to_the_cap_with_full_jitter(monkeypatch):
    monkeypatch.setattr(store.random, "uniform", lambda low, high: (low, high))
    assert [store.backoff(n) for n in (0, 1, 2, 5, 20)] == [
        (0, 10),
        (0, 20),
        (0, 40),
        (0, 300),
        (0, 300),
    ]


def test_a_chain_and_what_it_owes_are_stored_once():
    s = Store()
    for _ in range(2):  # the same chain from two searches, or delivered twice
        s.save_chain("d1", b"chain", ["B", "C"], {"key": "d1", "kind": "chain"})
    owed = [(m["dest"], m["key"]) for m in s.due_messages()]
    assert sorted(owed) == [("B", "d1"), ("C", "d1"), ("case", "chain:d1")]


def test_an_unacknowledged_message_waits_then_is_due_again(monkeypatch):
    s = Store()
    s.queue("B", "d1", b"chain")
    (message,) = s.due_messages()
    s.settle([message], "queued")
    assert s.due_messages() == []  # backing off
    later(monkeypatch, 301)
    assert [m["attempts"] for m in s.due_messages()] == [1]
    s.settle(s.due_messages(), "sent")
    assert s.due_messages() == []


def test_a_pending_search_is_retried_until_it_expires(monkeypatch):
    s = Store()
    s.add_search("p1", "p2")
    s.add_search("p1", "p2")  # the same hit read twice
    (search,) = s.due_searches()
    s.finish_search(search, "pending")
    later(monkeypatch, 301)
    assert [row["attempts"] for row in s.due_searches()] == [1]
    later(monkeypatch, store.EXPIRY)
    assert s.due_searches() == []
