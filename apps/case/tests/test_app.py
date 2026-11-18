import json

import pytest
from fastapi.testclient import TestClient

from case.app import create_app
from sample_chains import SAMPLE, chain, foreign, leaks, m1, o1, v1
from sample_chains import public_keys as keys
from test_evidence import forged

# DEPENDS ON Role 3 (privacy node): cases and /results use its real ChainResults.


@pytest.fixture
def client(url):
    client = TestClient(create_app(url, "B", keys))
    for result in (m1(), o1(), *v1()):
        assert client.post("/results", json=chain(result, "B")).status_code == 200
    return client


def case_ids(client):
    return {c["state"]: c["case_id"] for c in client.get("/api/cases").json()}


def test_healthz(client):
    assert client.get("/healthz").json() == {"ok": True}


def test_index_lists_alerts_first_then_low_priority(client):
    cases = client.get("/api/cases").json()
    assert [(c["tier"], c["state"]) for c in cases] == [
        (2, "confirmed"),  # o1
        (2, "partial"),  # v1
        (1, "confirmed"),  # m1, only B's rule
    ]
    page = client.get("/").text
    assert page.count("class='tag t2'>ALERT") == 2
    assert page.count("class='tag t1'>low priority") == 1
    assert "<meta http-equiv=refresh content=5>" in page  # refreshes during a replay


def test_detail_shows_own_legs_and_other_banks_tokens(client):
    m1_id = client.get("/api/cases").json()[2]["case_id"]
    page = client.get(f"/cases/{m1_id}").text
    assert "start → <b>A</b> → B" in page  # A sent the first transfer
    assert "A → <b>B</b> → C" in page
    assert "B → <b>C</b> → end" in page
    for own in ("p06", "p07", "B-m1", "10,000.00"):
        assert own in page
    assert leaks(page, foreign("B")) == set()
    assert "No draft" in page  # no summary service in this test
    assert client.get("/cases/nope").status_code == 404


def test_ui_has_no_decisions_or_free_text(client):
    m1_id = client.get("/api/cases").json()[2]["case_id"]
    page = client.get(f"/cases/{m1_id}").text
    assert "<input" not in page and "<textarea" not in page
    assert client.post(f"/cases/{m1_id}/decision").status_code == 422
    assert client.post(f"/cases/{m1_id}/report").status_code == 422


def test_bookmark_and_reviewed_toggle(client):
    m1_id = client.get("/api/cases").json()[2]["case_id"]
    assert client.post(f"/cases/{m1_id}/bookmarked").status_code == 200
    assert client.post(f"/cases/{m1_id}/reviewed").status_code == 200
    assert client.post(f"/cases/{m1_id}/reviewed").status_code == 200
    case = client.get(f"/api/cases/{m1_id}").json()
    assert (case["bookmarked"], case["reviewed"]) == (True, False)
    assert "Remove bookmark" in client.get(f"/cases/{m1_id}").text
    assert client.post("/cases/nope/bookmarked").status_code == 404


def test_alert_notification_shows_once(client):
    client.post("/results", json=chain(o1(), "B"))  # a redelivered message
    o1_id = o1().chain_digest
    assert client.get("/").text.count(f"alert <a href='/cases/{o1_id}'>") == 1


def test_results_endpoint_checks_before_storing(url):
    client = TestClient(create_app(url, "B", keys))
    for message in (chain(o1("B"), "B"), chain(o1("B"), "B")):
        assert client.post("/results", json=message).json() == {
            "key": o1().chain_digest
        }
    tampered = m1()
    forged(tampered)
    pending = chain(m1(), "B")
    pending["result"]["state"] = "PENDING"
    for bad in (chain(tampered, "B"), pending, {"kind": "chain"}):
        assert client.post("/results", json=bad).status_code == 422
    assert client.post("/results", content=b"not json").status_code == 422
    assert [c["case_id"] for c in client.get("/api/cases").json()] == [
        o1().chain_digest
    ]
    for message in SAMPLE[1:]:
        assert client.post("/results", json=message).status_code == 200


def test_stored_messages_log_json_with_alert_time(url, capsys):
    client = TestClient(create_app(url, "B", keys))
    client.post("/results", json=chain(o1(), "B"))
    client.post("/results", json={"kind": "chain"})
    stored, rejected = (
        json.loads(line) for line in capsys.readouterr().out.splitlines()
    )
    assert stored["event"] == "stored" and stored["key"] == o1().chain_digest
    assert stored["tier"] == 2 and stored["raised_at"] >= stored["ts"] - 5
    assert rejected["event"] == "rejected" and "ts" in rejected


def test_json_api(client):
    o1_id = o1().chain_digest
    case = client.get(f"/api/cases/{o1_id}").json()
    assert case["claims"][1]["signer"] == "B" and case["raised_at"] is not None
    assert [g["payment_id"] for g in case["legs"]] == ["p10", "p11"]


def test_nothing_from_other_banks_in_pages_api_or_logs(client, capfd):
    client.post("/results", json=chain(o1(), "B"))
    texts = [client.get("/").text, client.get("/api/cases").text]
    for case in client.get("/api/cases").json():
        texts += [client.get(f"/cases/{case['case_id']}").text]
        texts += [client.get(f"/api/cases/{case['case_id']}").text]
    out, err = capfd.readouterr()
    assert '"event": "stored"' in out  # messages were logged, so the check is not empty
    assert leaks("\n".join(texts) + out + err, foreign("B")) == set()
    assert leaks(out + err) == set()  # logs carry no customer data at all


def test_pending_case_shows_which_bank_it_waits_for(url):
    client = TestClient(create_app(url, "B", keys))
    client.post("/results", json=SAMPLE[1])  # pending, waiting for C
    assert "Waiting for bank C" in client.get("/").text
    assert "Waiting for bank C" in client.get(f"/cases/{SAMPLE[1]['key']}").text
    client.post("/results", json=chain(m1(), "B"))  # the retry confirms
    assert "Waiting for bank" not in client.get("/").text
