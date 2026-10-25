import json
import socket
import sys
import threading
import time
import urllib.request
from pathlib import Path

# The pytest executable does not add the repository root for namespace imports.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pytest
import uvicorn
from app import create_app
from case_store import SqliteCaseStore, chain
from fastapi.testclient import TestClient
from intake import create_app as create_intake
from outbox import dispatch
from test_case_store import claim, result


@pytest.fixture
def db(tmp_path):
    store = SqliteCaseStore(tmp_path / "cases.db")
    store.record(result(claim("p06", "p07", "q1")))  # m1, confirmed
    store.record(result(claim("p15", "p16", "q2"), status="partial"))  # v1 piece
    return tmp_path / "cases.db"


@pytest.fixture
def intake(tmp_path):
    """The real mock intake on a local port, so reports travel over HTTP."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    server = uvicorn.Server(
        uvicorn.Config(
            create_intake(tmp_path / "intake.db"), port=port, log_level="error"
        )
    )
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    while not server.started:
        time.sleep(0.01)
    yield f"http://127.0.0.1:{port}/reports"
    server.should_exit = True
    thread.join(5)


@pytest.fixture
def client(db):
    return TestClient(create_app(db, "B", "http://127.0.0.1:9/reports", interval=None))


def m1_id(db):
    return SqliteCaseStore(db).cases()[0]["case_id"]


def test_chain_orders_links_and_handles_loops():
    o1 = [claim("p11", "p12"), claim("p10", "p11")]
    assert chain(o1) == o1[::-1]
    v1 = [claim("p17", "p18"), claim("p15", "p16"), claim("p16", "p17")]
    assert chain(v1) == [v1[1], v1[2], v1[0]]
    # A full loop has no start; it is walked from the first claim and still ends.
    loop = [claim("p10", "p11"), claim("p11", "p12"), claim("p12", "p10")]
    assert chain(loop) == loop


def test_index_lists_cases_with_alert_state(client, db):
    page = client.get("/").text
    assert m1_id(db)[:16] in page
    assert "confirmed" in page and "partial" in page
    assert page.count("ALERT ") == 1


def test_detail_shows_which_bank_confirmed_each_link(client, db):
    page = client.get(f"/cases/{m1_id(db)}").text
    assert "A → <b>B</b> → C" in page
    assert "B (s1)" in page
    for private in ("p06", "p07", "A-m0", "B-m1", "C-m2", "10000"):
        assert private not in page
    assert client.get("/cases/nope").status_code == 404


def test_decisions_are_recorded(client, db):
    case_id = m1_id(db)
    reply = client.post(
        f"/cases/{case_id}/decision", data={"action": "freeze"}, follow_redirects=False
    )
    assert reply.status_code == 303
    assert [a for a, _ in SqliteCaseStore(db).get(case_id)["decisions"]] == ["freeze"]
    assert "Freeze accounts" in client.get("/").text
    bad = client.post(f"/cases/{case_id}/decision", data={"action": "delete"})
    assert bad.status_code == 422
    assert (
        client.post("/cases/nope/decision", data={"action": "freeze"}).status_code
        == 404
    )


def test_alert_notification_shows_once(client, db):
    store = SqliteCaseStore(db)
    senders = client.app.state.senders(store)
    assert dispatch(store, senders) == 1
    message = {"event_id": f"alert:{m1_id(db)}", "kind": "alert", "case_id": m1_id(db)}
    assert client.post("/notifications", json=message).status_code == 204  # redelivery
    assert len(store.notifications()) == 1
    assert client.get("/").text.count(f"alert <a href='/cases/{m1_id(db)}'>") == 1


def test_report_reaches_intake_once(db, intake):
    client = TestClient(create_app(db, "B", intake, interval=None))
    case_id = m1_id(db)
    for _ in range(2):
        client.post(f"/cases/{case_id}/decision", data={"action": "report"})
    store = SqliteCaseStore(db)
    assert dispatch(store, client.app.state.senders(store)) == 2  # alert and report
    with urllib.request.urlopen(intake) as reply:
        received = json.load(reply)
    assert len(received) == 1
    body = received[0]["body"]
    assert body["bank"] == "B" and body["case_id"] == case_id
    assert body["links"][0]["signer"] == "B"
    assert received[0]["received_at"] >= received[0]["raised_at"]
    for private in ("p06", "p07", "A-m0", "B-m1", "C-m2"):
        assert private not in str(body)


def test_undelivered_report_is_shown(client, db):
    store = SqliteCaseStore(db)
    store.decide(m1_id(db), "report")
    dispatch(store, client.app.state.senders(store))  # intake on port 9 is down
    page = client.get("/").text
    assert f"report:{m1_id(db)}"[:24] in page and "URLError" in page


def test_background_dispatcher_delivers_alerts(db):
    with TestClient(create_app(db, "B", "http://127.0.0.1:9/reports", 0.01)):
        deadline = time.time() + 5
        while not SqliteCaseStore(db).notifications() and time.time() < deadline:
            time.sleep(0.01)
    assert len(SqliteCaseStore(db).notifications()) == 1


def test_json_api(client, db):
    cases = client.get("/api/cases").json()
    assert [c["state"] for c in cases] == ["confirmed", "partial"]
    case = client.get(f"/api/cases/{m1_id(db)}").json()
    assert case["claims"][0]["signer"] == "B" and case["raised_at"] is not None
