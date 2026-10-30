import threading
import time

from case import outbox
from case.case_store import SqliteCaseStore
from case.outbox import dispatch, run
from sample_chains import as_partial, bridged, m1

# DEPENDS ON Role 3 (privacy node): cases are built from its real ChainResults.


def later(seconds):
    return lambda: time.time() + seconds


def inbox(store):
    return sorted((n["kind"], n["case_id"]) for n in store.notifications())


def test_alert_notifies_once(store):
    case_id = store.record(m1())
    store.record(m1())
    store.record(as_partial(m1()))
    assert inbox(store) == [("alert", case_id)]
    assert store.outbox() == []  # alerts never leave the bank


def test_partial_case_notifies_nothing_until_confirmed(store):
    case_id = store.record(as_partial(m1()))
    assert inbox(store) == []
    store.record(m1())
    assert inbox(store) == [("alert", case_id)]


def test_merge_of_alerted_case_is_announced(store):
    first, second, whole = bridged()
    old, new = store.record(first), store.record(second)
    store.record(whole)
    assert inbox(store) == sorted([("alert", old), ("alert", new), ("merged", new)])


def test_report_delivered_once_and_acked(store):
    case_id = store.record(m1())
    store.decide(case_id, "report")
    sent = []
    assert dispatch(store, sent.append, "B") == 1
    assert dispatch(store, sent.append, "B") == 0
    [msg] = sent
    assert msg["event_id"].startswith(f"report:{case_id}:")
    assert (msg["bank"], msg["case_id"], msg["state"]) == ("B", case_id, "confirmed")
    assert [link["signer"] for link in msg["links"]] == ["A", "B", "C"]
    assert store.outbox()[0]["acked_at"] is not None


def test_each_report_decision_sends(store):
    case_id = store.record(m1())
    store.decide(case_id, "report")
    store.decide(case_id, "report")  # e.g. after more evidence arrived
    sent = []
    assert dispatch(store, sent.append, "B") == 2
    assert len({m["event_id"] for m in sent}) == 2


def test_lost_ack_resends_the_same_event(store, monkeypatch):
    store.decide(store.record(m1()), "report")
    sent = []
    monkeypatch.setattr(store, "ack", lambda event_id: None)  # crash before the ack
    dispatch(store, sent.append, "B")
    monkeypatch.undo()
    dispatch(store, sent.append, "B")
    assert len(sent) == 2 and sent[0]["event_id"] == sent[1]["event_id"]


def test_failures_back_off_then_stop(store, monkeypatch):
    monkeypatch.setattr(outbox, "MAX_ATTEMPTS", 3)
    store.decide(store.record(m1()), "report")

    def down(message):
        raise ValueError(
            "any error"
        )  # not only network errors: nothing may kill the loop

    assert dispatch(store, down, "B") == 0
    event = store.outbox()[0]
    assert event["attempts"] == 1 and event["error"] == "ValueError"
    assert dispatch(store, down, "B", later(0.5)) == 0  # backoff not over
    assert store.outbox()[0]["attempts"] == 1
    dispatch(store, down, "B", later(1.5))
    dispatch(store, down, "B", later(10))
    assert store.outbox()[0]["attempts"] == 3
    sent = []
    assert dispatch(store, sent.append, "B", later(1000)) == 0  # attempts used up
    assert store.outbox()[0]["acked_at"] is None


def test_merge_carries_history_and_pending_report(store):
    first, second, whole = bridged()
    old, new = store.record(first), store.record(second)
    store.decide(new, "freeze")
    store.decide(new, "report")
    store.record(whole)  # merges new into old
    assert [a for a, _ in store.get(old)["decisions"]] == ["freeze", "report"]
    sent = []
    dispatch(store, sent.append, "B")
    assert sent[0]["case_id"] == old and len(sent[0]["links"]) == 6


def test_run_loop_stops(tmp_path):
    path = tmp_path / "cases.db"
    store = SqliteCaseStore(path)
    store.decide(store.record(m1()), "report")
    sent, stop = [], threading.Event()
    thread = threading.Thread(
        target=lambda: run(SqliteCaseStore(path), sent.append, "B", stop, 0.01)
    )
    thread.start()
    deadline = time.time() + 5
    while not sent and time.time() < deadline:
        time.sleep(0.01)
    stop.set()
    thread.join(1)
    assert not thread.is_alive() and len(sent) == 1
