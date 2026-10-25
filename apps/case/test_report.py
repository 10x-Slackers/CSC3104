import socket
import sys
from pathlib import Path

# The pytest executable does not add the repository root for namespace imports.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pytest
from app import create_app
from case_store import SqliteCaseStore
from fastapi.testclient import TestClient
from report import ALLOWED, TemplateClient, draft_report, report_input
from test_case_store import claim, result

PRIVATE = {"account": "A-m0", "amount": 10000.0, "payment_id": "p06", "note": "C-m2"}


class Recorder:
    """Test double: keeps what a client was given."""

    source = "recorder"

    def draft(self, data):
        self.seen = data
        return "body"


@pytest.fixture
def store(tmp_path):
    store = SqliteCaseStore(tmp_path / "cases.db")
    store.record(result(claim("p10", "p11", "qB"), claim("p11", "p12", "qB")))
    return store


def case_id(store):
    return store.cases()[0]["case_id"]


def keys(value):
    if isinstance(value, dict):
        return set(value) | {k for v in value.values() for k in keys(v)}
    if isinstance(value, list):
        return {k for v in value for k in keys(v)}
    return set()


def test_disallowed_fields_never_reach_report(store, monkeypatch):
    # A case carrying private fields, as if a future store change leaked them.
    leaky = store.get(case_id(store)) | PRIVATE
    monkeypatch.setattr(store, "get", lambda _: leaky)
    recorder = Recorder()
    draft_report(store, case_id(store), recorder)
    assert keys(recorder.seen) <= ALLOWED
    template = TemplateClient().draft(report_input(leaky))
    for text in (str(recorder.seen), template):
        for value in [*PRIVATE.values(), *(c.signature for c in leaky["claims"])]:
            assert str(value) not in text


def test_template_report_is_a_labelled_draft(store):
    report_id = draft_report(store, case_id(store), TemplateClient())
    [report] = store.reports(case_id(store))
    assert report["report_id"] == report_id
    assert (report["source"], report["status"]) == ("template", "draft")
    body = report["body"]
    assert body.startswith("DRAFT: template report, not AI-generated.")
    assert "banks A, B, C, confirmed by signed claims from B, C" in body
    assert "1. A -> B -> C, confirmed by B" in body
    assert "2. B -> C -> A, confirmed by C" in body
    assert "not proven laundering" in body
    assert "links may be missing" not in body


def test_partial_case_report_states_uncertainty(store):
    partial = store.record(result(claim("p15", "p16", "q2"), status="partial"))
    draft_report(store, partial, TemplateClient())
    body = store.reports(partial)[0]["body"]
    assert "Alert raised: not raised" in body
    assert "This case is partial: links may be missing or unconfirmed." in body


def test_template_needs_no_network(store, monkeypatch):
    def refuse(*args, **kwargs):
        raise AssertionError("network used")

    monkeypatch.setattr(socket, "socket", refuse)
    draft_report(store, case_id(store), TemplateClient())


def test_review_happens_once(store):
    report_id = draft_report(store, case_id(store), TemplateClient())
    with pytest.raises(ValueError):
        store.review_report(report_id, "draft")
    assert store.review_report(report_id, "approved")
    assert not store.review_report(report_id, "rejected")
    assert store.reports(case_id(store))[0]["status"] == "approved"


def test_ui_generates_and_reviews_drafts(store, tmp_path):
    client = TestClient(create_app(tmp_path / "cases.db", "B", "http://x", None))
    cid = case_id(store)
    assert client.post(f"/cases/{cid}/report").status_code == 200  # after redirect
    page = client.get(f"/cases/{cid}").text
    assert "template · draft" in page and "Approve" in page
    review = f"/cases/{cid}/reports/1/review"
    assert client.post(review, data={"status": "nope"}).status_code == 422
    assert client.post(review, data={"status": "approved"}).status_code == 200
    page = client.get(f"/cases/{cid}").text
    assert "template · approved" in page and "Approve" not in page
    assert client.post(review, data={"status": "rejected"}).status_code == 409
    assert client.post("/cases/nope/report").status_code == 404
