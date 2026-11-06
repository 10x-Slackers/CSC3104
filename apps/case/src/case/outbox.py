"""Deliver queued reports to the mock intake at least once, with bounded retries.

A delivery counts as acknowledged when send() returns, e.g. a 2xx webhook response.
A crash between send and ack resends it, so the intake deduplicates on event_id.
Reports carry the case's tokens and signing banks; never accounts or amounts.
"""

import json
import os
import threading
import time
import urllib.request
from collections.abc import Callable
from typing import Final

from case.case_store import SqliteCaseStore
from case.report import report_input

# Role 5 delivery timings (case.md); no other role sets them.
MAX_ATTEMPTS = int(os.environ.get("CASE_NOTIFY_MAX_ATTEMPTS", "10"))
BACKOFF = 1.0  # seconds before the first retry, doubling each attempt
MAX_BACKOFF = 60.0
MESSAGE_FIELDS: Final = ("case_id", "state", "merged_into", "raised_at", "links")


def message(store: SqliteCaseStore, event: dict, bank: str) -> dict:
    """The report event: the case's allow-listed evidence, as of sending."""
    case = store.get(event["case_id"])
    assert case is not None  # merges keep the old case row, so it always exists
    data = report_input(case)
    return {"event_id": event["event_id"], "bank": bank} | {
        f: data[f] for f in MESSAGE_FIELDS
    }


def dispatch(
    store: SqliteCaseStore,
    send: Callable[[dict], object],
    bank: str,
    now: Callable[[], float] = time.time,
) -> int:
    """Send every due report once; return how many were acknowledged."""
    started, sent = now(), 0
    for event in store.due(started, MAX_ATTEMPTS):
        try:
            send(message(store, event, bank))
        except Exception as exc:  # noqa: BLE001 - any failure is retried; the class is the category
            delay = min(BACKOFF * 2 ** event["attempts"], MAX_BACKOFF)
            store.failed(event["event_id"], started + delay, type(exc).__name__)
            continue
        store.ack(event["event_id"])
        sent += 1
    return sent


def run(
    store: SqliteCaseStore,
    send: Callable[[dict], object],
    bank: str,
    stop: threading.Event,
    interval: float = 1.0,
) -> None:
    """Housekeeping loop for a background thread; set stop to end it."""
    while not stop.wait(interval):
        dispatch(store, send, bank)


def webhook(url: str, timeout: float = 10) -> Callable[[dict], None]:
    """POST each message as JSON; non-2xx responses raise and are retried."""

    def send(msg):
        request = urllib.request.Request(
            url, json.dumps(msg).encode(), {"Content-Type": "application/json"}
        )
        urllib.request.urlopen(request, timeout=timeout).close()

    return send
