import json
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.error import HTTPError

# The pytest executable does not add the repository root for namespace imports.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pytest
from case_store import SqliteCaseStore
from outbox import RetryPolicy, dispatch, run, webhook
from test_case_store import claim, result

POLICY = RetryPolicy(max_attempts=3, backoff=10, max_backoff=100, lease=30)


@pytest.fixture
def store(tmp_path):
    return SqliteCaseStore(tmp_path / "cases.db")


def later(seconds):
    return lambda: time.time() + seconds


def test_alert_queues_one_event(store):
    case_id = store.record(result(claim("p06", "p07", "q1")))
    store.record(result(claim("p06", "p07", "q2")))
    store.record(result(claim("p06", "p07", "q3"), status="partial"))
    assert [(e["event_id"], e["kind"]) for e in store.outbox()] == [
        (f"alert:{case_id}", "alert")
    ]


def test_partial_case_queues_nothing_until_confirmed(store):
    case_id = store.record(result(claim("p06", "p07", "q1"), status="partial"))
    assert store.outbox() == []
    store.record(result(claim("p06", "p07", "q2")))
    assert [e["case_id"] for e in store.outbox()] == [case_id]


def test_delivered_once_and_acked(store):
    case_id = store.record(result(claim("p06", "p07")))
    sent = []
    assert dispatch(store, sent.append, POLICY) == 1
    assert dispatch(store, sent.append, POLICY) == 0
    assert sent == [
        {
            "event_id": f"alert:{case_id}",
            "kind": "alert",
            "case_id": case_id,
            "state": "confirmed",
            "merged_into": None,
        }
    ]
    assert store.outbox()[0]["acked_at"] is not None


def test_failures_back_off_then_stop(store):
    store.record(result(claim("p06", "p07")))

    def down(message):
        raise ConnectionError("unreachable")

    assert dispatch(store, down, POLICY) == 0
    event = store.outbox()[0]
    assert event["attempts"] == 1 and event["error"] == "ConnectionError"
    assert dispatch(store, down, POLICY, later(5)) == 0  # backoff not over
    assert store.outbox()[0]["attempts"] == 1
    dispatch(store, down, POLICY, later(11))
    dispatch(store, down, POLICY, later(100))
    assert store.outbox()[0]["attempts"] == 3
    sent = []
    assert dispatch(store, sent.append, POLICY, later(1000)) == 0  # attempts used up
    assert store.outbox()[0]["acked_at"] is None


def test_retry_succeeds_and_receiver_sees_one_alert(store):
    store.record(result(claim("p06", "p07")))
    inbox = {}

    def flaky(message):
        if not inbox.get("failed"):
            inbox["failed"] = True
            raise TimeoutError
        inbox[message["event_id"]] = message

    dispatch(store, flaky, POLICY)
    assert dispatch(store, flaky, POLICY, later(11)) == 1
    assert len(inbox) == 2  # the failure marker and one alert


def test_crash_before_ack_redelivers_after_lease(store):
    store.record(result(claim("p06", "p07")))
    store.lease_due(time.time(), POLICY.lease, POLICY.max_attempts)  # then crash
    received = {}
    assert dispatch(store, lambda m: received.update({m["event_id"]: m}), POLICY) == 0
    assert (
        dispatch(
            store, lambda m: received.update({m["event_id"]: m}), POLICY, later(31)
        )
        == 1
    )
    received_again = dict(received)
    dispatch(store, lambda m: received.update({m["event_id"]: m}), POLICY, later(1000))
    assert received == received_again and len(received) == 1


def test_pending_events_survive_restart(tmp_path):
    path = tmp_path / "cases.db"
    case_id = SqliteCaseStore(path).record(result(claim("p06", "p07")))
    sent = []
    assert dispatch(SqliteCaseStore(path), sent.append, POLICY) == 1
    assert sent[0]["case_id"] == case_id


def test_merge_of_alerted_case_is_announced(store):
    old = store.record(result(claim("p15", "p16", "q1")))
    new = store.record(result(claim("p18", "p19", "q2")))
    store.record(result(claim("p16", "p17", "q3"), claim("p17", "p18", "q3")))
    sent = []
    dispatch(store, sent.append, POLICY)
    assert [(m["kind"], m["case_id"], m["merged_into"]) for m in sent] == [
        ("alert", old, None),
        ("alert", new, old),
        ("merged", new, old),
    ]


def test_concurrent_dispatchers_send_each_event_once(tmp_path):
    path = tmp_path / "cases.db"
    store = SqliteCaseStore(path)
    for i, (a, b) in enumerate([("p06", "p07"), ("p10", "p11"), ("p15", "p16")]):
        store.record(result(claim(a, b, f"q{i}")))
    sent, lock = [], threading.Lock()

    def send(message):
        time.sleep(0.05)
        with lock:
            sent.append(message["event_id"])

    # One store per thread: a SQLite connection belongs to the thread that opened it.
    threads = [
        threading.Thread(target=lambda: dispatch(SqliteCaseStore(path), send, POLICY))
        for _ in range(4)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(sent) == 3 == len(set(sent))


def test_run_loop_stops(tmp_path):
    path = tmp_path / "cases.db"
    SqliteCaseStore(path).record(result(claim("p06", "p07")))
    sent, stop = [], threading.Event()
    thread = threading.Thread(
        target=lambda: run(SqliteCaseStore(path), sent.append, stop, POLICY, 0.01)
    )
    thread.start()
    deadline = time.time() + 5
    while not sent and time.time() < deadline:
        time.sleep(0.01)
    stop.set()
    thread.join(1)
    assert not thread.is_alive() and len(sent) == 1


@pytest.fixture
def receiver():
    received, status = [], [204]

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            received.append(
                json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            )
            self.send_response(status[0])
            self.end_headers()

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_port}/notifications", received, status
    server.shutdown()


def test_webhook_posts_json_and_raises_on_error(receiver):
    url, received, status = receiver
    webhook(url)({"event_id": "alert:x"})
    assert received == [{"event_id": "alert:x"}]
    status[0] = 500
    with pytest.raises(HTTPError):
        webhook(url)({"event_id": "alert:x"})
