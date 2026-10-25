"""Deliver outbox events at least once, with bounded retries and acknowledgements.

    uv run apps/case/outbox.py cases.db --url http://analyst-ui:8000/notifications

A delivery counts as acknowledged when send() returns, e.g. a 2xx webhook response.
A crash between send and ack resends after the lease, so receivers must deduplicate
on event_id. Notifications carry only the event, case ID and state; reports to the
mock intake add the chain's tokens and signing banks. Never accounts or amounts.
"""

import argparse
import json
import os
import sys
import threading
import time
import urllib.request
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class RetryPolicy:
    """Role 5 delivery timings (case.md); no other role sets them."""

    max_attempts: int = int(os.environ.get("CASE_NOTIFY_MAX_ATTEMPTS", "10"))
    backoff: float = 1.0  # seconds before the first retry, doubling each attempt
    max_backoff: float = 60.0
    lease: float = 30.0  # how long a send may take before another dispatcher retries


LINK_FIELDS = ("signer", "in_pair", "in_token", "out_pair", "out_token")


def message(store, event):
    """Notifications name the case only; a report also carries the chain evidence."""
    case = store.get(event["case_id"])
    msg = {
        "event_id": event["event_id"],
        "kind": event["kind"],
        "case_id": event["case_id"],
        "state": case["state"],
        "merged_into": case["merged_into"],
    }
    if event["kind"] == "report":
        links = [{f: getattr(c, f) for f in LINK_FIELDS} for c in case["claims"]]
        msg |= {"raised_at": case["raised_at"], "links": links}
    return msg


def route(senders):
    """Send each event kind to its own receiver."""
    return lambda msg: senders[msg["kind"]](msg)


def dispatch(store, send, policy=None, now=time.time):
    """Send every due event once; return how many were acknowledged."""
    policy = policy or RetryPolicy()
    started, sent = now(), 0
    for event in store.lease_due(started, policy.lease, policy.max_attempts):
        try:
            send(message(store, event))
        except OSError as exc:  # network and HTTP errors; the class is the category
            delay = min(policy.backoff * 2 ** event["attempts"], policy.max_backoff)
            store.retry(event["event_id"], started + delay, type(exc).__name__)
            continue
        store.ack(event["event_id"])
        sent += 1
    return sent


def run(store, send, stop, policy=None, interval=1.0):
    """Housekeeping loop for a background thread; set stop to end it."""
    while not stop.wait(interval):
        dispatch(store, send, policy)


def webhook(url, timeout=10):
    """POST each message as JSON; non-2xx responses raise and are retried."""

    def send(msg):
        request = urllib.request.Request(
            url, json.dumps(msg).encode(), {"Content-Type": "application/json"}
        )
        urllib.request.urlopen(request, timeout=timeout).close()

    return send


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("db", type=Path, help="case store SQLite file")
    parser.add_argument("--url", required=True, help="webhook receiving events")
    parser.add_argument("--interval", type=float, default=1.0, help="seconds")
    args = parser.parse_args()
    # Direct script execution puts apps/case, rather than the repo root, on sys.path.
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from apps.case.case_store import SqliteCaseStore

    try:
        run(
            SqliteCaseStore(args.db),
            webhook(args.url),
            threading.Event(),
            None,
            args.interval,
        )
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
