"""Analyst UI and JSON API for one bank's cases, with the outbox dispatcher running inside.

    CASE_BANK=A CASE_DB=cases-a.db CASE_PUBLIC_KEYS=.secrets/public uv run python -m apps.case.app

Settings (environment): CASE_BANK, CASE_DB, CASE_PUBLIC_KEYS, CASE_INTAKE_URL, PORT.
Chain results arrive from the bank's privacy node on POST /results. Alerts and merges
are delivered in-process to this UI's inbox; reports go to the mock intake (intake.py).
"""

import os
import sys
import threading
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import UTC, datetime
from html import escape
from pathlib import Path
from typing import Annotated

# Direct script execution puts apps/case, rather than the repo root, on sys.path.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import uvicorn
from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from apps.case.case_store import DECISIONS, STATES, SqliteCaseStore, neighbours
from apps.case.evidence import load_public_keys, verify_result
from apps.case.outbox import LINK_FIELDS, route, run, webhook
from apps.case.report import TemplateClient, draft_report
from apps.privacy_node import privacy_node_pb2 as pb
from apps.security.security import check_peer_payload

ACTIONS = dict(
    zip(
        DECISIONS,
        (
            "Continue monitoring",
            "Suspend outgoing transactions",
            "Freeze accounts",
            "Report to authorities",
        ),
    )
)
# Everything a report may carry to the intake; checked before it leaves the bank.
# DEPENDS ON Role 4 (security and privacy): review of these intake report fields
# (security.md open item "Destination and inputs for the AI report"). Status: pending.
REPORT_FIELDS = {"event_id", "kind", "case_id", "state", "merged_into", "raised_at"}
REPORT_FIELDS |= {"links", "bank", *LINK_FIELDS}
STYLE = """<style>
body { font: 15px system-ui, sans-serif; max-width: 60rem; margin: 2rem auto; padding: 0 1rem }
table { border-collapse: collapse; width: 100% } td, th { border-bottom: 1px solid #ccc;
padding: .3rem .5rem; text-align: left } code { font-size: 12px } .muted { color: #666 }
</style>"""


def when(timestamp):
    if timestamp is None:
        return "-"
    return datetime.fromtimestamp(timestamp, UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


def short(token):
    """Show a token's version prefix and the start of its digest; "-" for none."""
    if not token:
        return "-"
    return f"<code title='{escape(token)}'>{escape(token[:24])}…</code>"


def page(bank, title, body):
    return HTMLResponse(
        f"<!doctype html><title>{escape(title)}</title>{STYLE}"
        f"<p class=muted><a href='/'>Bank {escape(bank)} cases</a></p>{body}"
    )


def create_app(db, bank, intake_url, public_keys, interval=1.0, client=None):
    """interval=None skips the background dispatcher (tests call dispatch directly)."""
    client = client or TemplateClient()

    def send_report(msg):
        msg = msg | {"bank": bank}
        check_peer_payload(msg, REPORT_FIELDS)
        webhook(intake_url)(msg)

    def senders(store):
        return route(
            {"alert": store.receive, "merged": store.receive, "report": send_report}
        )

    @asynccontextmanager
    async def lifespan(app):
        stop = threading.Event()

        def dispatcher():
            store = SqliteCaseStore(db)  # opened in the thread that uses it
            run(store, senders(store), stop, None, interval)

        if interval is not None:
            threading.Thread(target=dispatcher, daemon=True).start()
        yield
        stop.set()

    app = FastAPI(title=f"Bank {bank} cases", lifespan=lifespan)
    app.state.senders = senders

    def load(case_id):
        case = SqliteCaseStore(db).get(case_id)
        if case is None:
            raise HTTPException(404, "No such case")
        return case

    @app.get("/", response_class=HTMLResponse)
    def index():
        store = SqliteCaseStore(db)
        rows = "".join(
            f"<tr><td><a href='/cases/{c['case_id']}'><code>{c['case_id'][:16]}</code></a>"
            f"<td>{c['state']}<td>{'ALERT ' + when(c['raised_at']) if c['raised_at'] else '-'}"
            f"<td>{ACTIONS[d[-1][0]] if (d := store.get(c['case_id'])['decisions']) else '-'}"
            for c in reversed(store.cases())
        )
        inbox = "".join(
            f"<li>{when(n['received_at'])}: {escape(n['kind'])} "
            f"<a href='/cases/{escape(n['case_id'])}'><code>{escape(n['case_id'][:16])}</code></a>"
            for n in store.notifications()
        )
        pending = "".join(
            f"<li>{escape(e['event_id'][:24])}: {e['attempts']} attempts, {escape(e['error'] or 'waiting')}"
            for e in store.outbox()
            if e["acked_at"] is None
        )
        return page(
            bank,
            f"Bank {bank} cases",
            f"<h1>Bank {escape(bank)} cases</h1>"
            "<table><tr><th>Case<th>State<th>Alert<th>Latest decision</tr>"
            f"{rows or '<tr><td colspan=4>No cases yet'}</table>"
            f"<h2>Notifications</h2><ul>{inbox or '<li>None'}</ul>"
            f"<h2>Undelivered</h2><ul>{pending or '<li>None'}</ul>",
        )

    @app.get("/cases/{case_id}", response_class=HTMLResponse)
    def detail(case_id: str):
        case = load(case_id)
        links = "".join(
            f"<tr><td>{i}<td>{escape(start)} → <b>{escape(c.signer)}</b> → "
            f"{escape(end)}<td>{escape(c.signer)}"
            f"<td>{short(c.in_token)}<td>{short(c.out_token)}"
            for i, c in enumerate(case["claims"], 1)
            for start, end in [neighbours(c.signer, c.in_pair, c.out_pair)]
        )
        results = "".join(
            f"<li>{escape(s)} from {escape(sender)} · digest <code>{escape(d[:16])}</code>"
            for d, sender, s in case["results"]
        )
        decisions = "".join(
            f"<li>{when(t)}: {ACTIONS[a]}" for a, t in case["decisions"]
        )
        merged = case["merged_into"]
        reports = "".join(
            f"<h3>Report {r['report_id']} · {escape(r['source'])} · {escape(r['status'])}"
            f" · {when(r['created_at'])}</h3><pre>{escape(r['body'])}</pre>"
            + (
                f"<form method=post action='/cases/{escape(case_id)}/reports/"
                f"{r['report_id']}/review'><button name=status value=approved>Approve"
                "</button> <button name=status value=rejected>Reject</button></form>"
                if r["status"] == "draft"
                else ""
            )
            for r in SqliteCaseStore(db).reports(case_id)
        )
        buttons = "".join(
            f"<button name=action value={a}>{label}</button> "
            for a, label in ACTIONS.items()
        )
        return page(
            bank,
            f"Case {case_id[:16]}",
            f"<h1>Case <code>{escape(case_id[:16])}</code></h1>"
            + (
                f"<p>Merged into <a href='/cases/{escape(merged)}'>{escape(merged[:16])}</a>"
                if merged
                else ""
            )
            + f"<p>State: <b>{case['state']}</b> · Alert: {when(case['raised_at'])}"
            "<p class=muted>A rule flags suspicion, not proven laundering. Each link is "
            "confirmed by the signing bank; a signature doesn't prove its records are true."
            "<h2>Links</h2><table><tr><th>#<th>Banks (from → confirming → to)"
            f"<th>Confirmed by<th>Incoming token<th>Outgoing token</tr>{links}</table>"
            f"<h2>Chain results received</h2><ul>{results}</ul>"
            f"<h2>Decisions</h2><ul>{decisions or '<li>None yet'}</ul>"
            f"<form method=post action='/cases/{escape(case_id)}/decision'>{buttons}</form>"
            "<p class=muted>All actions are simulated."
            "<h2>Draft reports</h2><p class=muted>Template reports, not AI-generated;"
            " each needs analyst review.</p>"
            f"<form method=post action='/cases/{escape(case_id)}/report'>"
            f"<button>Generate draft</button></form>{reports}",
        )

    @app.post("/cases/{case_id}/decision")
    def decide(case_id: str, action: Annotated[str, Form()]):
        load(case_id)
        if action not in ACTIONS:
            raise HTTPException(422, "Unknown action")
        SqliteCaseStore(db).decide(case_id, action)
        return RedirectResponse(f"/cases/{case_id}", status_code=303)

    @app.post("/cases/{case_id}/report")
    def generate(case_id: str):
        load(case_id)
        draft_report(SqliteCaseStore(db), case_id, client)
        return RedirectResponse(f"/cases/{case_id}", status_code=303)

    @app.post("/cases/{case_id}/reports/{report_id}/review")
    def review(case_id: str, report_id: int, status: Annotated[str, Form()]):
        if status not in ("approved", "rejected"):
            raise HTTPException(422, "Unknown review")
        if not SqliteCaseStore(db).review_report(report_id, status):
            raise HTTPException(409, "Report is not a pending draft")
        return RedirectResponse(f"/cases/{case_id}", status_code=303)

    @app.post("/results")
    async def results(request: Request):
        """Take one serialized ChainResult from this bank's privacy node; returns its Ack.

        DEPENDS ON Role 3 (privacy node): posting every ChainResult it saves
        (store.save_chain, for its own searches and for Deliver) to this endpoint.
        Status: pending.
        """
        result = pb.ChainResult.FromString(await request.body())
        if result.state not in STATES or not verify_result(result, public_keys, bank):
            raise HTTPException(422, "Invalid chain result")
        SqliteCaseStore(db).record(result)
        return {"chain_digest": result.chain_digest}

    @app.post("/notifications", status_code=204)
    def notifications(message: dict):
        """Webhook receiver for another dispatcher; deduplicates on event_id."""
        SqliteCaseStore(db).receive(message)

    @app.get("/api/cases")
    def api_cases():
        return SqliteCaseStore(db).cases()

    @app.get("/api/cases/{case_id}")
    def api_case(case_id: str):
        case = load(case_id)
        return case | {"claims": [asdict(c) for c in case["claims"]]}

    return app


if __name__ == "__main__":
    uvicorn.run(
        create_app(
            os.environ.get("CASE_DB", "cases.db"),
            os.environ.get("CASE_BANK", "A"),
            os.environ.get("CASE_INTAKE_URL", "http://127.0.0.1:8001/reports"),
            load_public_keys(os.environ.get("CASE_PUBLIC_KEYS", "/public")),
        ),
        host="0.0.0.0",
        port=int(os.environ.get("PORT", "8000")),
    )
