"""Case service: one bank's analyst page and JSON API over its case store.

    BANK=A DATABASE_URL=cases.db python -m mulechain.case.app

The bank's privacy node posts its messages to POST /messages. Analysts can read a
case, bookmark it and mark it reviewed. What the bank then does about a case is its
own process, so there is no free text and no decision here.
"""

import json
import os
import urllib.request
from collections.abc import Callable
from datetime import UTC, datetime
from html import escape
from typing import Literal

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse

from mulechain import log
from mulechain.case.store import CaseStore
from mulechain.serverless import request_headers

SUMMARY_SECONDS = 2
STYLE = """<style>
body { font: 15px system-ui, sans-serif; max-width: 60rem; margin: 2rem auto; padding: 0 1rem }
table { border-collapse: collapse; width: 100% } td, th { border-bottom: 1px solid #ccc;
padding: .3rem .5rem; text-align: left } code { font-size: 12px } .muted { color: #666 }
form { display: inline } .tag { padding: 0 .4rem; border-radius: 3px; font-size: 13px }
.t2 { background: #c62828; color: #fff; font-weight: 600 } .t1 { background: #eee; color: #555 }
.wait { background: #fff3cd; color: #7a5b00 }
</style>"""
TIERS = {2: "ALERT", 1: "low priority", 0: "no tier"}


def summarizer(url: str) -> Callable[[dict], str | None]:
    """Ask the summary service for a draft; any failure or a slow answer means "no draft"."""

    def summarize(case: dict) -> str | None:
        if not url:
            return None
        body = json.dumps(case).encode()
        try:
            request = urllib.request.Request(url, body, request_headers(url, body))
            with urllib.request.urlopen(request, timeout=SUMMARY_SECONDS) as reply:
                return json.load(reply)["draft"]
        except Exception as error:  # noqa: BLE001 - the bulkhead: a summary outage stops here
            failure = type(error).__name__
            log("case", bank=case["bank"], event="no_draft", case_id=case["id"], error=failure)
            return None

    return summarize


def tag(case: dict) -> str:
    return f"<span class='tag t{case['tier']}'>{TIERS[case['tier']]}</span>"


def state(case: dict) -> str:
    if case["state"] == "pending" and case["waiting"]:
        return f"<span class='tag wait'>Waiting for bank {escape(case['waiting'])}</span>"
    return escape(case["state"])


def short(token: str) -> str:
    return f"<code title='{escape(token)}'>{escape(token[:12])}…</code>" if token else "-"


def when(timestamp: float | None) -> str:
    if not timestamp:
        return "-"
    return f"{datetime.fromtimestamp(timestamp, UTC):%Y-%m-%d %H:%M:%S UTC}"


def table(headings: str, rows: list[str]) -> str:
    head = "".join(f"<th>{h}" for h in headings.split("|"))
    return f"<table><tr>{head}</tr>{''.join(rows) or '<tr><td>None'}</table>"


def create_app(store: CaseStore, summarize: Callable[[dict], str | None]) -> FastAPI:
    bank = store.bank
    app = FastAPI(title=f"Bank {bank} cases")

    def page(title: str, body: str, head: str = "") -> HTMLResponse:
        return HTMLResponse(
            f"<!doctype html><title>{escape(title)}</title>{head}{STYLE}"
            f"<p class=muted><a href='/'>Bank {bank} cases</a></p>{body}"
        )

    def load(case_id: str) -> dict:
        case = store.get(case_id)
        if case is None:
            raise HTTPException(404, "No such case")
        return case

    @app.get("/healthz")
    def healthz():
        return {"ok": True}

    @app.post("/messages")
    def messages(batch: list[dict]):
        """Store a batch from the privacy node; it sends the batch again unless this replies."""
        for message, case in zip(batch, store.record(batch), strict=True):
            log(
                "case",
                bank=bank,
                event="stored",
                key=message["key"],
                kind=message["kind"],
                case_id=case["id"],
                tier=case["tier"],
                alerted_at=case["alerted_at"],
            )
        return {"stored": len(batch)}

    @app.get("/api/cases")
    def api_cases():
        payments = store.payments()
        return [case | {"payments": payments.get(case["id"], [])} for case in store.cases()]

    @app.get("/api/cases/{case_id}")
    def api_case(case_id: str):
        return load(case_id)

    @app.post("/cases/{case_id}/{flag}")
    def toggle(case_id: str, flag: Literal["bookmarked", "reviewed"]):
        store.toggle(case_id, flag)
        return RedirectResponse(f"/cases/{case_id}", status_code=303)

    @app.get("/", response_class=HTMLResponse)
    def index():
        rows = [
            f"<tr><td><a href='/cases/{c['id']}'><code>{c['id'][:16]}</code></a><td>{tag(c)}"
            f"<td>{state(c)}<td>{c['rules']}<td>{c['amount']:,.2f}<td>{when(c['alerted_at'])}"
            f"<td>{'★' if c['bookmarked'] else ''}<td>{'✓' if c['reviewed'] else ''}"
            for c in store.cases(limit=200)
        ]
        return page(
            f"Bank {bank} cases",
            f"<h1>Bank {bank} cases</h1><p class=muted>The 200 most urgent, alerts first.</p>"
            + table("Case|Tier|State|Rules|Amount (SGD)|Alert|Bookmark|Reviewed", rows),
            "<meta http-equiv=refresh content=5>",  # new cases appear during a replay
        )

    @app.get("/cases/{case_id}", response_class=HTMLResponse)
    def detail(case_id: str):
        case = load(case_id)
        legs = [
            f"<tr><td>{escape(leg['payment_id'])}<td>{leg['direction']}<td>{escape(leg['account'])}"
            f"<td>{leg['amount']:,.2f}<td>{leg['peer'] or 'this bank'}"
            for leg in case["legs"]
        ]
        links = [
            f"<tr><td>{i}<td>{link['in_from'] or 'start'} → <b>{link['signer']}</b> → "
            f"{link['out_to'] or 'end'}<td>{short(link['in_token'])}<td>{short(link['out_token'])}"
            for i, link in enumerate(case["links"], 1)
        ]
        received = "".join(
            f"<li>{m['kind']} {escape(m['rule'] or '')} {escape(m['waiting'] or '')}"
            f" · <code>{m['key'][:16]}</code> · {when(m['received_at'])}"
            for m in case["messages"]
        )
        labels = {
            "bookmarked": ("Bookmark", "Remove bookmark"),
            "reviewed": ("Mark reviewed", "Mark not reviewed"),
        }
        toggles = "".join(
            f"<form method=post action='/cases/{case_id}/{flag}'><button>{label[case[flag]]}"
            "</button></form> "
            for flag, label in labels.items()
        )
        merged = case["merged_into"]
        draft = summarize(case)
        return page(
            f"Case {case_id[:16]}",
            f"<h1>Case <code>{case_id[:16]}</code></h1>"
            + (f"<p>Merged into <a href='/cases/{merged}'>{merged[:16]}</a>" if merged else "")
            + f"<p>{tag(case)} · State: <b>{state(case)}</b> · Alert: {when(case['alerted_at'])}"
            f"<p>{toggles}<p class=muted>A rule flags suspicion, not proven laundering. Each link"
            " is signed by the bank that states it; a signature doesn't prove its records are true."
            "<h2>This bank's payments</h2>"
            + table("Payment|Direction|Account|Amount (SGD)|Other bank", legs)
            + "<h2>Chain</h2><p class=muted>Other banks' transfers are tokens only.</p>"
            + table("#|Banks (from → signer → to)|Incoming token|Outgoing token", links)
            + f"<h2>From the privacy node</h2><ul>{received}</ul><h2>Draft summary</h2>"
            + (
                f"<pre>{escape(draft)}</pre>"
                if draft
                else "<p>No draft: the summary service is unavailable."
            ),
        )

    return app


if __name__ == "__main__":
    bank = os.environ["BANK"]
    uvicorn.run(
        create_app(
            CaseStore(os.environ["DATABASE_URL"], bank),
            summarizer(os.environ.get("SUMMARY_URL", "")),
        ),
        host="0.0.0.0",
        port=int(os.environ.get("PORT", "8000")),
        access_log=False,
        log_config=None,
    )
