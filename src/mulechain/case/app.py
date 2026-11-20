"""Case service: one bank's analyst page and JSON API over its case store.

    BANK=A DATABASE_URL=cases.db python -m mulechain.case.app

The bank's privacy node posts its messages to POST /messages. Analysts can read a
case, bookmark it and mark it reviewed. What the bank then does about a case is its
own process, so there is no free text and no decision here.

The bulkhead pattern (docs/patterns.md): the summary service gets two seconds
(`summarizer`), and a slow or broken one costs the page only its draft.
"""

import json
import os
import urllib.request
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from mulechain import log
from mulechain.case.store import CaseStore
from mulechain.serverless import request_headers

SUMMARY_SECONDS = 2
TIERS = {2: "Alert", 1: "Low priority", 0: "No tier"}
STATES = {
    "confirmed": "Confirmed chain",
    "partial": "Partial chain",
    "pending": "Search pending",
    "local": "Inside this bank",
    "hit": "Rule hit",
}


def when(timestamp: float | None) -> str:
    return f"{datetime.fromtimestamp(timestamp, UTC):%d %b %Y, %H:%M UTC}" if timestamp else ""


# The pages are the Jinja templates next to this file; values are HTML-escaped by default.
TEMPLATES = Jinja2Templates(Path(__file__).parent / "templates")
TEMPLATES.env.filters["when"] = when
TEMPLATES.env.globals.update(tiers=TIERS, states=STATES)


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


def create_app(store: CaseStore, summarize: Callable[[dict], str | None]) -> FastAPI:
    bank = store.bank
    app = FastAPI(title=f"Bank {bank} cases")

    def page(request: Request, template: str, **context) -> HTMLResponse:
        return TEMPLATES.TemplateResponse(request, template, {"bank": bank, **context})

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
                payments=sorted(leg["payment_id"] for leg in message["legs"]),
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

    @app.get("/")
    def index(request: Request):
        return page(request, "cases.html", cases=store.cases(limit=200))

    @app.get("/cases/{case_id}")
    def detail(request: Request, case_id: str):
        case = load(case_id)
        if case["merged_into"]:  # joined another case: show that one
            return RedirectResponse(f"/cases/{case['merged_into']}")
        return page(request, "case.html", case=case, draft=summarize(case))

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
