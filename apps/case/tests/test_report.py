import json
import threading
from http.server import ThreadingHTTPServer

import pytest
from fastapi.testclient import TestClient

from case.app import create_app
from case.report import report_input, summarizer
from sample_chains import chain, foreign, leaks, m1, message, o1, public_keys
from summary.app import Handler

DOWN = "http://127.0.0.1:9/"  # nothing listens here


@pytest.fixture
def summary_url():
    """The real summary service on a local port, so drafts travel over HTTP."""
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_port}/"
    server.shutdown()


@pytest.fixture
def case(store):
    return store.get(store.record(message(o1(), "B")))


def test_input_has_own_legs_tokens_and_no_signatures(case):
    data = report_input(case, "B")
    assert {g["payment_id"] for g in data["legs"]} == {"p10", "p11"}
    assert all("signature" not in link for link in data["links"])
    assert leaks(json.dumps(data), foreign("B")) == set()


def test_summary_service_drafts_over_http(case, summary_url):
    draft = summarizer(summary_url, "B")(case)
    assert draft.startswith(f"Case {o1().chain_digest[:16]} at bank B")
    assert "- p10: SGD 7,000.00 received into B-o1 from bank A." in draft
    assert "Bank B received a transfer from bank A, and its rule linked it" in draft


@pytest.mark.parametrize("summary", [DOWN, ""])
def test_summary_failure_means_no_draft(case, summary, capsys):
    assert summarizer(summary, "B", timeout=0.5)(case) is None
    if summary:
        assert json.loads(capsys.readouterr().out)["event"] == "no_draft"


def test_case_page_shows_the_draft_and_survives_an_outage(tmp_path, summary_url):
    url = str(tmp_path / "cases.db")
    up = TestClient(create_app(url, "B", public_keys, summarizer(summary_url, "B")))
    up.post("/results", json=chain(m1(), "B"))
    m1_id = m1().chain_digest
    assert "Low priority: only bank B" in up.get(f"/cases/{m1_id}").text
    down = TestClient(create_app(url, "B", public_keys, summarizer(DOWN, "B", 0.5)))
    page = down.get(f"/cases/{m1_id}")
    assert page.status_code == 200 and "No draft" in page.text
    # Detection doesn't wait on summaries: results are still stored.
    assert down.post("/results", json=chain(o1(), "B")).status_code == 200
