"""Read-only analyst UI and JSON API for one bank's cases.

    CASE_BANK=A CASE_DATABASE_URL=cases-a.db CASE_KEY_DIRECTORY=.secrets uv run python -m case.app

Settings (environment): CASE_BANK, CASE_DATABASE_URL (a postgresql:// URL, or a SQLite
file), CASE_KEY_DIRECTORY, CASE_SUMMARY_URL, CASE_SUMMARY_TIMEOUT, CASE_SUMMARY_REGION,
PORT. The bank's privacy node posts each message to POST /results. Analysts can only
view, bookmark and mark cases reviewed: what the bank does about a case is its own
process, so there is no free text and no decision.
"""

import os
import threading
from collections.abc import Callable
from dataclasses import asdict
from datetime import UTC, datetime
from html import escape
from typing import Literal

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from case import log
from case.case_store import Case, CaseStore, neighbours
from case.evidence import load_public_keys, parse
from case.report import summarizer
from security import PublicKeys

TIERS = {2: "ALERT", 1: "low priority"}
STYLE = """<style>
body { font: 15px system-ui, sans-serif; max-width: 60rem; margin: 2rem auto; padding: 0 1rem }
table { border-collapse: collapse; width: 100% } td, th { border-bottom: 1px solid #ccc;
padding: .3rem .5rem; text-align: left } code { font-size: 12px } .muted { color: #666 }
form { display: inline } .tag { padding: 0 .4rem; border-radius: 3px; font-size: 13px }
.t2 { background: #c62828; color: #fff; font-weight: 600 } .t1 { background: #eee; color: #555 }
.wait { background: #fff3cd; color: #7a5b00 }
</style>"""


def tier(t: int) -> str:
    """Alerts in red, low priority in grey."""
    return f"<span class='tag t{t}'>{TIERS.get(t, 'no tier')}</span>"


def state(c: dict) -> str:
    if c["state"] == "pending" and c.get("waiting"):
        return f"<span class='tag wait'>Waiting for bank {escape(c['waiting'])}</span>"
    return escape(c["state"])


def short(token: str) -> str:
    if not token:
        return "-"
    return f"<code title='{escape(token)}'>{escape(token[:24])}…</code>"


def when(timestamp: float | None) -> str:
    if timestamp is None:
        return "-"
    return datetime.fromtimestamp(timestamp, UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


def page(bank: str, title: str, body: str, head: str = "") -> HTMLResponse:
    return HTMLResponse(
        f"<!doctype html><title>{escape(title)}</title>{head}{STYLE}"
        f"<p class=muted><a href='/'>Bank {escape(bank)} cases</a></p>{body}"
    )


def create_app(
    db: str | os.PathLike[str],
    bank: str,
    public_keys: Callable[[], PublicKeys],
    summarize: Callable[[Case], str | None] = lambda case: None,
) -> FastAPI:
    local = threading.local()

    def store() -> CaseStore:
        """One connection per thread: a connection runs one transaction at a time."""
        if not hasattr(local, "store"):
            local.store = CaseStore(str(db))
        return local.store

    store()  # create the schema before the first request
    app = FastAPI(title=f"Bank {bank} cases")

    def load(case_id: str) -> Case:
        case = store().get(case_id)
        if case is None:
            raise HTTPException(404, "No such case")
        return case

    @app.get("/healthz")
    def healthz():
        return {"ok": True}

    @app.get("/", response_class=HTMLResponse)
    def index():
        rows = "".join(
            f"<tr><td><a href='/cases/{c['case_id']}'><code>{c['case_id'][:16]}</code></a>"
            f"<td>{tier(c['tier'])}<td>{state(c)}<td>{when(c['raised_at'])}"
            f"<td>{'★' if c['bookmarked'] else ''}<td>{'✓' if c['reviewed'] else ''}"
            for c in store().cases()
        )
        inbox = "".join(
            f"<li>{when(n['created_at'])}: {escape(n['kind'])} "
            f"<a href='/cases/{escape(n['case_id'])}'><code>{escape(n['case_id'][:16])}</code></a>"
            for n in store().notifications()
        )
        return page(
            bank,
            f"Bank {bank} cases",
            f"<h1>Bank {escape(bank)} cases</h1>"
            "<table><tr><th>Case<th>Tier<th>State<th>Alert<th>Bookmark<th>Reviewed</tr>"
            f"{rows or '<tr><td colspan=6>No cases yet'}</table>"
            f"<h2>Alerts</h2><ul>{inbox or '<li>None'}</ul>",
            "<meta http-equiv=refresh content=5>",  # new cases appear during a replay
        )

    @app.get("/cases/{case_id}", response_class=HTMLResponse)
    def detail(case_id: str):
        case = load(case_id)
        links = "".join(
            f"<tr><td>{i}<td>{escape(start)} → <b>{escape(c.signer)}</b> → "
            f"{escape(end)}<td>{short(c.in_token)}<td>{short(c.out_token)}"
            for i, c in enumerate(case["claims"], 1)
            for start, end in [neighbours(c.signer, c.in_pair, c.out_pair)]
        )
        legs = "".join(
            f"<tr><td>{escape(leg['payment_id'])}<td>{escape(leg['direction'])}"
            f"<td>{escape(leg['account'])}<td>{leg['amount']:,.2f}"
            f"<td>{escape(leg['pair'] or 'intra-bank')}"
            for leg in case["legs"]
        )
        messages = "".join(
            f"<li>{escape(m['kind'])}{' ' + escape(m['state']) if m['state'] != m['kind'] else ''}"
            f"{' · ' + escape(m['detail']) if m['detail'] else ''}"
            f" · <code>{escape(m['key'][:16])}</code> · {when(m['received_at'])}"
            for m in case["messages"]
        )
        draft = summarize(case)
        waiting = next(
            (m["detail"] for m in case["messages"] if m["kind"] == "pending"), ""
        )
        merged = case["merged_into"]
        toggles = "".join(
            f"<form method=post action='/cases/{escape(case_id)}/{flag}'>"
            f"<button>{label[case[flag]]}</button></form> "
            for flag, label in (
                ("bookmarked", {False: "Bookmark", True: "Remove bookmark"}),
                ("reviewed", {False: "Mark reviewed", True: "Mark not reviewed"}),
            )
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
            + f"<p>{tier(case['tier'])} · State: <b>{state(case | {'waiting': waiting})}</b>"
            f" · Alert: {when(case['raised_at'])}<p>{toggles}"
            "<p class=muted>A rule flags suspicion, not proven laundering. Each link is "
            "confirmed by the signing bank; a signature doesn't prove its records are true."
            "<h2>This bank's legs</h2><table><tr><th>Payment<th>Direction<th>Account"
            f"<th>Amount (SGD)<th>Pair</tr>{legs or '<tr><td colspan=5>None'}</table>"
            "<h2>Links</h2><p class=muted>Other banks' legs are tokens only.</p>"
            "<table><tr><th>#<th>Banks (from → confirming → to)"
            f"<th>Incoming token<th>Outgoing token</tr>{links or '<tr><td colspan=4>None'}</table>"
            f"<h2>From the privacy node</h2><ul>{messages}</ul>"
            "<h2>Draft summary</h2><p class=muted>Drafted from a fixed template for you to review.</p>"
            + (
                f"<pre>{escape(draft)}</pre>"
                if draft
                else "<p>No draft: the summary service is unavailable."
            ),
        )

    @app.post("/cases/{case_id}/{flag}")
    def toggle(case_id: str, flag: Literal["bookmarked", "reviewed"]):
        load(case_id)
        store().flag(case_id, flag)
        return RedirectResponse(f"/cases/{case_id}", status_code=303)

    @app.post("/results")
    async def results(request: Request):
        """Check and store one node message (JSON); reply only once it is stored."""
        try:
            message = parse(await request.json(), public_keys(), bank)
        except (KeyError, TypeError, ValueError) as exc:
            log(event="rejected", bank=bank, error=type(exc).__name__)
            raise HTTPException(422, "Invalid message") from exc
        case = load(store().record(message))
        # The evaluation joins chain_digest (key) to raised_at, on one clock.
        log(
            event="stored", bank=bank, key=message.key, kind=message.kind,
            case_id=case["case_id"], tier=case["tier"], raised_at=case["raised_at"],
        )  # fmt: skip
        return {"key": message.key}

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
    summarize = summarizer(
        os.environ.get("CASE_SUMMARY_URL", ""),
        bank,
        float(os.environ.get("CASE_SUMMARY_TIMEOUT", "2")),
        os.environ.get("CASE_SUMMARY_REGION", ""),
    )
    keys = os.environ.get("CASE_KEY_DIRECTORY", ".secrets")
    uvicorn.run(
        create_app(
            os.environ.get("CASE_DATABASE_URL", "cases.db"),
            bank,
            lambda: load_public_keys(keys, bank),
            summarize,
        ),
        host="0.0.0.0",
        port=int(os.environ.get("PORT", "8000")),
        access_log=False,
        log_config=None,
    )
