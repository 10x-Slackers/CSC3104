"""Analyst UI and JSON API for one bank's cases, with the report dispatcher running inside.

    CASE_BANK=A CASE_DB=cases-a.db CASE_KEY_DIRECTORY=.secrets uv run python -m case.app

Settings (environment): CASE_BANK, CASE_DB, CASE_KEY_DIRECTORY, CASE_INTAKE_URL, PORT.
Chain results arrive from the bank's privacy node on POST /results. Alerts and merges
go straight to this UI's inbox; reports go to the mock intake (intake.py).
"""

import os
import threading
from collections.abc import Callable
from contextlib import asynccontextmanager
from dataclasses import asdict
from html import escape
from typing import Annotated

import uvicorn
from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from google.protobuf.message import DecodeError

from case.case_store import Case, Decision, ReviewStatus, SqliteCaseStore, neighbours
from case.evidence import check_result, load_public_keys
from case.outbox import run, webhook
from case.report import ReportClient, TemplateClient, draft_report, when
from privacy_node import privacy_node_pb2 as pb
from security import PublicKeys

ACTIONS: dict[Decision, str] = {
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


def short(token: str) -> str:
    """Show the stored token hex; peer messages still use raw bytes."""
    if not token:
        return "-"
    return f"<code title='{escape(token)}'>{escape(token[:24])}…</code>"


def page(bank: str, title: str, body: str) -> HTMLResponse:
    return HTMLResponse(
        f"<!doctype html><title>{escape(title)}</title>{STYLE}"
        f"<p class=muted><a href='/'>Bank {escape(bank)} cases</a></p>{body}"
    )


def create_app(
    db: str | os.PathLike[str],
    bank: str,
    intake_url: str,
    public_keys: Callable[[], PublicKeys],
    interval: float | None = 1.0,
    client: ReportClient | None = None,
) -> FastAPI:
    """interval=None skips the background dispatcher (tests call dispatch directly)."""
    client = client or TemplateClient()
    send = webhook(intake_url)
    local = threading.local()

    def store() -> SqliteCaseStore:
        """One connection per thread: a SQLite connection belongs to its thread."""
        if not hasattr(local, "store"):
            local.store = SqliteCaseStore(db)
        return local.store

    @asynccontextmanager
    async def lifespan(app):
        stop, wait = threading.Event(), interval  # a local, so dispatcher sees a float
        if wait is not None:

            def dispatcher():
                run(store(), send, bank, stop, wait)  # this thread's own connection

            threading.Thread(target=dispatcher, daemon=True).start()
        yield
        stop.set()

    app = FastAPI(title=f"Bank {bank} cases", lifespan=lifespan)

    # The store rejects results in undeliverable states.
    @app.exception_handler(ValueError)
    async def invalid(request: Request, exc: ValueError) -> JSONResponse:
        return JSONResponse({"detail": str(exc)}, 422)

    def load(case_id: str) -> Case:
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
    def decide(case_id: str, action: Annotated[Decision, Form()]):
        load(case_id)
        store().decide(case_id, action)
        return RedirectResponse(f"/cases/{case_id}", status_code=303)

    @app.post("/cases/{case_id}/report")
    def generate(case_id: str):
        draft_report(store(), load(case_id), client)
        return RedirectResponse(f"/cases/{case_id}", status_code=303)

    @app.post("/cases/{case_id}/reports/{report_id}/review")
    def review(case_id: str, report_id: int, status: Annotated[ReviewStatus, Form()]):
        if not store().review_report(case_id, report_id, status):
            raise HTTPException(409, "Report is not a pending draft")
        return RedirectResponse(f"/cases/{case_id}", status_code=303)

    @app.post("/results")
    async def results(request: Request):
        """Verify one bank-internal protobuf result before storing its normalized copy."""
        try:
            result = pb.ChainResult.FromString(await request.body())
            keys = public_keys()
            result = check_result(result, keys, bank)
        except (DecodeError, ValueError) as exc:
            raise HTTPException(422, "Invalid chain result") from exc
        store().record(result)
        return {"chain_digest": result.chain_digest}

    @app.get("/api/cases")
    def api_cases():
        return store().cases()

    @app.get("/api/cases/{case_id}")
    def api_case(case_id: str):
        case = load(case_id)
        return case | {"claims": [asdict(c) for c in case["claims"]]}

    return app


if __name__ == "__main__":
    bank = os.environ.get("CASE_BANK", "A")
    uvicorn.run(
        create_app(
            os.environ.get("CASE_DB", "cases.db"),
            bank,
            os.environ.get("CASE_INTAKE_URL", "http://127.0.0.1:8001/reports"),
            lambda: load_public_keys(
                os.environ.get("CASE_KEY_DIRECTORY", ".secrets"), bank
            ),
        ),
        host="0.0.0.0",
        port=int(os.environ.get("PORT", "8000")),
    )
