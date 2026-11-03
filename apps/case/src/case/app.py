"""Analyst UI and JSON API for one bank's cases, with the report dispatcher running inside.

    CASE_BANK=A CASE_DB=cases-a.db CASE_PUBLIC_KEYS=.secrets/public uv run python -m case.app

Settings (environment): CASE_BANK, CASE_DB, CASE_PUBLIC_KEYS, CASE_INTAKE_URL, PORT.
Chain results arrive from the bank's privacy node on POST /results. Alerts and merges
go straight to this UI's inbox; reports go to the mock intake (intake.py).
"""

import json
import os
import threading
from contextlib import asynccontextmanager
from dataclasses import asdict
from html import escape
from typing import Annotated

import uvicorn
from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from google.protobuf.message import DecodeError

from case.case_store import SqliteCaseStore, neighbours
from case.enrichment import own_legs
from case.evidence import check_result, load_public_keys
from case.outbox import run, webhook
from case.report import TemplateClient, draft_report, when
from privacy_node import privacy_node_pb2 as pb

ACTIONS = {  # keys are case_store.DECISIONS
    "monitor": "Continue monitoring",
    "suspend": "Suspend outgoing transactions",
    "freeze": "Freeze accounts",
    "report": "Report to authorities",
}
STYLE = """<style>
body { font: 15px system-ui, sans-serif; max-width: 60rem; margin: 2rem auto; padding: 0 1rem }
table { border-collapse: collapse; width: 100% } td, th { border-bottom: 1px solid #ccc;
padding: .3rem .5rem; text-align: left } code { font-size: 12px } .muted { color: #666 }
</style>"""


def short(token):
    """Hex is only a display format; storage and peer messages retain raw bytes."""
    if not token:
        return "-"
    token = token.hex()
    return f"<code title='{escape(token)}'>{escape(token[:24])}…</code>"


def display_case(case):
    """Convert binary tokens only at the human-facing JSON boundary."""
    claims = [asdict(c) for c in case["claims"]]
    for claim in claims:
        for field in ("in_token", "out_token"):
            claim[field] = claim[field].hex()
    legs = [leg | {"token": leg["token"].hex()} for leg in case["own_legs"]]
    return case | {"claims": claims, "own_legs": legs}


def page(bank, title, body):
    return HTMLResponse(
        f"<!doctype html><title>{escape(title)}</title>{STYLE}"
        f"<p class=muted><a href='/'>Bank {escape(bank)} cases</a></p>{body}"
    )


def create_app(db, bank, intake_url, public_keys, interval=1.0, client=None):
    """interval=None skips the background dispatcher (tests call dispatch directly)."""
    client = client or TemplateClient()
    send = webhook(intake_url)
    local = threading.local()

    def store():
        """One connection per thread: a SQLite connection belongs to its thread."""
        if not hasattr(local, "store"):
            local.store = SqliteCaseStore(db)
        return local.store

    @asynccontextmanager
    async def lifespan(app):
        stop = threading.Event()

        def dispatcher():
            run(store(), send, bank, stop, interval)  # this thread's own connection

        if interval is not None:
            threading.Thread(target=dispatcher, daemon=True).start()
        yield
        stop.set()

    app = FastAPI(title=f"Bank {bank} cases", lifespan=lifespan)

    @app.exception_handler(ValueError)
    async def invalid(request, exc):  # the store rejects unknown actions and states
        return JSONResponse({"detail": str(exc)}, 422)

    def load(case_id):
        case = store().get(case_id)
        if case is None:
            raise HTTPException(404, "No such case")
        return case

    @app.get("/", response_class=HTMLResponse)
    def index():
        rows = "".join(
            f"<tr><td><a href='/cases/{c['case_id']}'><code>{c['case_id'][:16]}</code></a>"
            f"<td>{c['state']}<td>{'ALERT ' + when(c['raised_at']) if c['raised_at'] else '-'}"
            f"<td>{ACTIONS[c['decision']] if c['decision'] else '-'}"
            for c in reversed(store().cases())
        )
        inbox = "".join(
            f"<li>{when(n['created_at'])}: {escape(n['kind'])} "
            f"<a href='/cases/{escape(n['case_id'])}'><code>{escape(n['case_id'][:16])}</code></a>"
            for n in store().notifications()
        )
        pending = "".join(
            f"<li>{escape(e['event_id'][:24])}: {e['attempts']} attempts, {escape(e['error'] or 'waiting')}"
            for e in store().outbox(pending=True)
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
        details = "".join(
            f"<li>{escape(leg['direction'])}: payment {escape(leg['payment_id'])}, "
            f"account {escape(leg['account'])}, amount {escape(str(leg['amount']))}"
            for leg in case["own_legs"]
        )
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
            for r in store().reports(case_id)
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
            f"<h2>Own-bank details</h2><ul>{details or '<li>None supplied'}</ul>"
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
        store().decide(case_id, action)
        return RedirectResponse(f"/cases/{case_id}", status_code=303)

    @app.post("/cases/{case_id}/report")
    def generate(case_id: str):
        draft_report(store(), load(case_id), client, bank)
        return RedirectResponse(f"/cases/{case_id}", status_code=303)

    @app.post("/cases/{case_id}/reports/{report_id}/review")
    def review(case_id: str, report_id: int, status: Annotated[str, Form()]):
        if not store().review_report(case_id, report_id, status):
            raise HTTPException(409, "Report is not a pending draft")
        return RedirectResponse(f"/cases/{case_id}", status_code=303)

    @app.post("/results")
    async def results(request: Request):
        """Internal node delivery: protobuf, optionally with own-bank multipart details."""
        legs = []
        try:
            if request.headers.get("content-type", "").startswith(
                "multipart/form-data"
            ):
                async with request.form() as form:
                    payload = await form["result"].read()
                    legs = json.loads(form["own_legs"])
            else:
                payload = await request.body()
            result = pb.ChainResult.FromString(payload)
            keys = public_keys() if callable(public_keys) else public_keys
            result = check_result(result, keys, bank)
            details = own_legs(result, legs, bank)
        except (DecodeError, ValueError, KeyError, TypeError, AttributeError) as exc:
            raise HTTPException(
                422, "Invalid chain result or own-bank details"
            ) from exc
        store().record(result, details)
        return {"chain_digest": result.chain_digest}

    @app.get("/api/cases")
    def api_cases():
        return store().cases()

    @app.get("/api/cases/{case_id}")
    def api_case(case_id: str):
        case = load(case_id)
        return display_case(case)

    return app


if __name__ == "__main__":
    uvicorn.run(
        create_app(
            os.environ.get("CASE_DB", "cases.db"),
            os.environ.get("CASE_BANK", "A"),
            os.environ.get("CASE_INTAKE_URL", "http://127.0.0.1:8001/reports"),
            lambda: load_public_keys(os.environ.get("CASE_PUBLIC_KEYS", "/public")),
        ),
        host="0.0.0.0",
        port=int(os.environ.get("PORT", "8000")),
    )
