import json
import logging
import socket
import threading
import time
import urllib.request

import pytest
import uvicorn
from fastapi.testclient import TestClient

from case.app import create_app
from case.case_store import SqliteCaseStore
from case.intake import create_app as create_intake
from case.outbox import dispatch, message, webhook
from privacy_node import privacy_node_pb2 as pb
from sample_chains import leaks, m1, o1, public_keys, v1
from test_evidence import forged

DOWN = "http://127.0.0.1:9/reports"  # nothing listens here
# DEPENDS ON Role 3 (privacy node): cases and /results use its real ChainResults.


@pytest.fixture
def db(tmp_path):
    store = SqliteCaseStore(tmp_path / "cases.db")
    store.record(m1())  # confirmed
    for result in v1():  # partial
        store.record(result)
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
    return TestClient(create_app(db, "B", DOWN, public_keys(), interval=None))


def m1_id(db):
    return SqliteCaseStore(db).cases()[0]["case_id"]


def test_index_lists_cases_with_alert_state(client, db):
    page = client.get("/").text
    assert m1_id(db)[:16] in page
    assert "confirmed" in page and "partial" in page
    assert page.count("ALERT ") == 1


def test_detail_shows_which_bank_confirmed_each_link(client, db):
    page = client.get(f"/cases/{m1_id(db)}").text
    assert "start → <b>A</b> → B" in page  # A sent the first transfer
    assert "A → <b>B</b> → C" in page
    assert "B → <b>C</b> → end" in page
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
    SqliteCaseStore(db).record(m1())  # a redelivered result
    assert client.get("/").text.count(f"alert <a href='/cases/{m1_id(db)}'>") == 1


def test_report_reaches_intake_once(client, db, intake):
    case_id = m1_id(db)
    client.post(f"/cases/{case_id}/decision", data={"action": "report"})
    store = SqliteCaseStore(db)
    [event] = store.due(time.time(), 10)
    webhook(intake)(message(store, event, "B"))  # sent, but the ack was lost
    assert dispatch(store, webhook(intake), "B") == 1  # so it is sent again
    with urllib.request.urlopen(intake) as reply:
        received = json.load(reply)
    assert len(received) == 1
    body = received[0]["body"]
    assert body["bank"] == "B" and body["case_id"] == case_id
    assert [link["signer"] for link in body["links"]] == ["A", "B", "C"]
    assert received[0]["received_at"] >= received[0]["raised_at"]
    for private in ("p06", "p07", "A-m0", "B-m1", "C-m2"):
        assert private not in str(body)


def test_undelivered_report_is_shown(client, db):
    store = SqliteCaseStore(db)
    store.decide(m1_id(db), "report")
    dispatch(store, webhook(DOWN), "B")  # intake on port 9 is down
    page = client.get("/").text
    assert f"report:{m1_id(db)}"[:24] in page and "URLError" in page


def test_background_dispatcher_delivers_reports(db, intake):
    with TestClient(create_app(db, "B", intake, public_keys(), 0.01)) as client:
        client.post(f"/cases/{m1_id(db)}/decision", data={"action": "report"})
        deadline = time.time() + 5
        while SqliteCaseStore(db).outbox(pending=True) and time.time() < deadline:
            time.sleep(0.01)
    assert SqliteCaseStore(db).outbox(pending=True) == []


def test_json_api(client, db):
    cases = client.get("/api/cases").json()
    assert [c["state"] for c in cases] == ["confirmed", "partial"]
    case = client.get(f"/api/cases/{m1_id(db)}").json()
    assert case["claims"][1]["signer"] == "B" and case["raised_at"] is not None


def test_results_endpoint_verifies_before_storing(tmp_path):
    client = TestClient(
        create_app(tmp_path / "cases.db", "B", DOWN, public_keys(), interval=None)
    )
    for result in (o1("B"), o1("C"), o1("B")):  # two starts, then a redelivery
        reply = client.post("/results", content=result.SerializeToString())
        assert reply.json() == {"chain_digest": o1().chain_digest}
    pending, tampered = m1(), m1()
    pending.state = pb.PENDING
    forged(tampered)
    for bad in (pending, tampered):
        reply = client.post("/results", content=bad.SerializeToString())
        assert reply.status_code == 422
    assert [c["case_id"] for c in client.get("/api/cases").json()] == [
        o1().chain_digest
    ]


def test_nothing_sensitive_in_api_responses_pages_or_logs(db, intake, caplog, capfd):
    caplog.set_level(logging.DEBUG)
    client = TestClient(create_app(db, "B", intake, public_keys(), interval=None))
    texts = [client.post("/results", content=o1().SerializeToString()).text]
    store = SqliteCaseStore(db)
    for case in store.cases():
        case_id = case["case_id"]
        client.post(f"/cases/{case_id}/decision", data={"action": "report"})
        client.post(f"/cases/{case_id}/report")
        texts += [client.get(f"/cases/{case_id}").text]
        texts += [client.get(f"/api/cases/{case_id}").text]
    assert dispatch(store, webhook(intake), "B") == 3  # m1, v1 and o1
    texts += [client.get("/").text, client.get("/api/cases").text]
    with urllib.request.urlopen(intake) as reply:
        texts.append(reply.read().decode())
    out, err = capfd.readouterr()
    logs = caplog.text + out + err
    assert "POST" in logs  # requests were logged, so the log check is not empty
    assert leaks("\n".join(texts) + logs) == set()
