import socket

import pytest
from fastapi.testclient import TestClient

from apps.case.app import create_app
from apps.case.case_store import SqliteCaseStore
from apps.case.report import ALLOWED, TemplateClient, draft_report, report_input
from apps.case.sample_chains import o1, public_keys, v1

# DEPENDS ON Role 3 (privacy node): cases are built from its real ChainResults.
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
    store.record(o1())  # loop A -> B -> C -> A, confirmed
    return store


def case_id(store):
    return store.cases()[0]["case_id"]


def keys(value):
    if isinstance(value, dict):
        return set(value) | {k for v in value.values() for k in keys(v)}
    if isinstance(value, list):
        return {k for v in value for k in keys(v)}
    return set()


# DEPENDS ON Role 4 (security and privacy): ALLOWED is pending their review.
def test_disallowed_fields_never_reach_report(store):
    # A case carrying private fields, as if a future store change leaked them.
    leaky = store.get(case_id(store)) | PRIVATE
    recorder = Recorder()
    draft_report(store, leaky, recorder)
    assert keys(recorder.seen) <= ALLOWED
    template = TemplateClient().draft(report_input(leaky))
    for text in (str(recorder.seen), template):
        for value in [*PRIVATE.values(), *(c.signature for c in leaky["claims"])]:
            assert str(value) not in text


def test_template_report_is_a_labelled_draft(store):
    report_id = draft_report(store, store.get(case_id(store)), TemplateClient())
    [report] = store.reports(case_id(store))
    assert report["report_id"] == report_id
    assert (report["source"], report["status"]) == ("template", "draft")
    body = report["body"]
    assert body.startswith("DRAFT: template report, not AI-generated.")
    assert "4 signed link claim(s) across banks A, B, C, from A, B, C." in body
    assert "1. start -> A -> B, confirmed by A" in body
    assert "2. A -> B -> C, confirmed by B" in body
    assert "3. B -> C -> A, confirmed by C" in body
    assert "4. C -> A -> end, confirmed by A" in body
    assert f"confirmed from B (digest {o1().chain_digest[:16]})" in body
    assert "not proven laundering" in body
    assert "links may be missing" not in body


def test_partial_case_report_states_uncertainty(store):
    partial = store.record(v1()[0])
    draft_report(store, store.get(partial), TemplateClient())
    body = store.reports(partial)[0]["body"]
    assert "Alert raised: not raised" in body
    assert "This case is partial: links may be missing or unconfirmed." in body


def test_template_needs_no_network(store, monkeypatch):
    def refuse(*args, **kwargs):
        raise AssertionError("network used")

    monkeypatch.setattr(socket, "socket", refuse)
    draft_report(store, store.get(case_id(store)), TemplateClient())


def test_review_happens_once(store):
    cid = case_id(store)
    report_id = draft_report(store, store.get(cid), TemplateClient())
    with pytest.raises(ValueError):
        store.review_report(cid, report_id, "draft")
    assert not store.review_report("other-case", report_id, "approved")
    assert store.review_report(cid, report_id, "approved")
    assert not store.review_report(cid, report_id, "rejected")
    assert store.reports(case_id(store))[0]["status"] == "approved"


def test_ui_generates_and_reviews_drafts(store, tmp_path):
    client = TestClient(
        create_app(tmp_path / "cases.db", "B", "http://x", public_keys(), None)
    )
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
