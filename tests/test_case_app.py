"""The analyst page, and the bulkhead around the summary service."""

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from itertools import pairwise

from conftest import hit, leg
from fastapi.testclient import TestClient

from mulechain import summary
from mulechain.case import app as case_app
from mulechain.case.app import create_app, summarizer
from mulechain.case.store import CaseStore
from mulechain.evaluate import settle


def test_an_alert_page_shows_own_payments_tokens_and_a_draft(nodes):
    settle(nodes)
    client = TestClient(
        create_app(nodes["B"].cases, lambda case: summary.handle(json.dumps(case))[1]["draft"])
    )
    listed = client.get("/api/cases").json()
    assert [c["tier"] for c in listed] == [2, 2, 1, 1, 1]
    loop = next(c for c in listed if c["payments"] == ["p10", "p11"])
    page = client.get(f"/cases/{loop['id']}").text
    assert "Alert</mark>" in page and "B-o1" in page  # this bank's own account
    assert "A-o0" not in page and "C-o2" not in page and "p12" not in page  # nobody else's
    assert "Alert: the rules at banks B and C each linked these payments" in page
    assert "one chain of 3 transfers" in page

    long = next(c for c in listed if len(c["payments"]) == 4)  # several overlapping partial chains
    path = client.get(f"/api/cases/{long['id']}").json()["links"]
    assert [link["signer"] for link in path] == list("ABCABCA")
    assert all(a["out_token"] == b["in_token"] for a, b in pairwise(path))

    client.post(f"/cases/{loop['id']}/reviewed", follow_redirects=False)
    assert client.get(f"/api/cases/{loop['id']}").json()["reviewed"] == 1


def test_the_node_posts_batches_and_a_resent_batch_is_harmless():
    client = TestClient(create_app(CaseStore(":memory:", "A"), summarizer("")))
    for _ in range(2):
        assert client.post("/messages", json=[hit("h1", leg("p1"))]).json() == {"stored": 1}
    assert [c["payments"] for c in client.get("/api/cases").json()] == [["p1"]]
    assert len(client.get("/api/cases/h1").json()["messages"]) == 1
    assert "No draft" in client.get("/cases/h1").text  # no summary service configured


def test_a_slow_or_broken_summary_service_only_costs_the_draft(monkeypatch):
    release = threading.Event()

    class Slow(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            release.wait(5)

        def log_message(self, *args) -> None:
            pass

    server = HTTPServer(("127.0.0.1", 0), Slow)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    monkeypatch.setattr(case_app, "SUMMARY_SECONDS", 0.2)
    case = {"id": "c1", "bank": "A"}
    try:
        assert summarizer(f"http://127.0.0.1:{server.server_port}")(case) is None  # timeout
        assert summarizer("http://127.0.0.1:1")(case) is None  # nothing listening
    finally:
        release.set()
        server.shutdown()
