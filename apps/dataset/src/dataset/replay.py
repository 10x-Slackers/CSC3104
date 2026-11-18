"""Replay a workload to each bank's ingestor as pacs.008 batches over HTTP(S).

One process and one clock feed all three banks. Each GIRO or MEPS+ payment goes to
every bank it involves (once if intra-bank); other payment types are never sent.
An https:// URL is a Lambda function URL with AWS_IAM: each request is signed with
SigV4 as whatever role botocore finds (on EKS, LabRole from the node).
Contract: docs/contracts/data.md (Replay, Ingestor).

    uv run python -m dataset.replay apps/dataset/fixtures/tiny --url B=http://localhost:8080 --speed 0
    uv run python -m dataset.replay apps/dataset/out/slice-6h --url A=<url> --url B=<url> --url C=<url>
    uv run python -m dataset.replay apps/dataset/out/slice-6h --url A=<url> --url B=<url> --url C=<url> --rate 2000
"""

import argparse
import heapq
import http.client
import itertools
import json
import queue
import random
import threading
import time
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from urllib.parse import urlsplit

import botocore.session
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest

from ingestor import BICS, Event, read_events

BATCH = 100  # payments per pacs.008 message, at most
BACKLOG = 1000  # --speed 0: payments queued per bank, at most (10 batches)


def payments(workload: Path) -> Iterator[Event]:
    # Each transfer once, from its `out` event, in event-time order across the banks.
    banks = [read_events(workload / f"bank_{b}.csv") for b in "ABC"]
    for e in heapq.merge(*banks, key=lambda e: e["event_time"]):
        if e["direction"] == "out" and e["payment_format"] in ("GIRO", "MEPS+"):
            yield e


def tx(e: Event) -> dict:
    return {
        "PmtId": {"EndToEndId": e["payment_id"]},
        "PmtTpInf": {"LclInstrm": {"Prtry": e["payment_format"]}},
        "IntrBkSttlmAmt": {"Ccy": "SGD", "Amt": f"{e['amount']:.2f}"},
        "AccptncDtTm": e["event_time"],
        "DbtrAcct": {"Id": {"Othr": {"Id": e["account"]}}},
        "DbtrAgt": {"FinInstnId": {"BICFI": BICS[e["bank"]]}},
        "CdtrAcct": {"Id": {"Othr": {"Id": e["counterparty_account"]}}},
        "CdtrAgt": {"FinInstnId": {"BICFI": BICS[e["counterparty_bank"]]}},
    }


def sigv4(url: str) -> SigV4Auth | None:
    """A signer for an https function URL (<url-id>.lambda-url.<region>.on.aws), else None."""
    u = urlsplit(url)
    if u.scheme != "https":
        return None
    creds = botocore.session.get_session().get_credentials()
    if creds is None:
        raise SystemExit(f"no AWS credentials to sign requests to {url}")
    return SigV4Auth(creds, "lambda", u.hostname.split(".")[2])


def headers(url: str, body: bytes, auth: SigV4Auth | None) -> dict:
    h = {"Content-Type": "application/json"}
    if auth:  # signed per attempt: a signature is only valid for 5 minutes
        req = AWSRequest("POST", url, data=body, headers=h)
        auth.add_auth(req)
        h = dict(req.headers)
    return h


def post(
    conn: http.client.HTTPConnection, url: str, body: bytes, auth: SigV4Auth | None
) -> tuple[int | None, str]:
    # One POST -> (status, reply); status None on any error (network, signing), then
    # conn reconnects.
    try:
        path = urlsplit(url).path or "/"
        conn.request("POST", path, body, headers(url, body, auth))
        r = conn.getresponse()
        return r.status, r.read().decode()
    except Exception as e:  # noqa: BLE001 - a dead sender would hang --speed 0
        conn.close()
        return None, f"{type(e).__name__}: {e}"


def send(
    bank: str, url: str, auth: SigV4Auth | None, q: queue.Queue, failed: threading.Event
) -> None:
    """POST this bank's queue in batches, one at a time, so its events keep their order.

    A batch is whatever became due while the last one was sent, at most BATCH payments.
    5xx, 429, network and signing errors resend the same batch with full jitter, 1 s
    doubling to 30 s; a 400 can't succeed on resend, so it stops replay.
    """
    u = urlsplit(url)
    https = u.scheme == "https"
    connect = http.client.HTTPSConnection if https else http.client.HTTPConnection
    conn = connect(u.netloc, timeout=35)  # ingestor gives up at 25s
    for seq in itertools.count(1):
        txs = [q.get()]
        while txs[-1] and len(txs) < BATCH and not q.empty():
            txs.append(q.get())
        done = txs[-1] is None  # the end of the workload
        if done:
            txs = txs[:-1]
        if not txs or failed.is_set():
            return
        msg_id = f"replay-{bank}-{seq:06d}"
        hdr = {"MsgId": msg_id, "CreDtTm": txs[-1]["AccptncDtTm"]}
        hdr["NbOfTxs"] = str(len(txs))
        batch = {"FIToFICstmrCdtTrf": {"GrpHdr": hdr, "CdtTrfTxInf": txs}}
        body = json.dumps(batch).encode()
        for attempt in itertools.count():
            ts = time.time()
            status, reply = post(conn, url, body, auth)
            ms = round((time.time() - ts) * 1000, 1)
            log = {"ts": ts, "service": "replay", "bank": bank, "msg_id": msg_id}
            log |= {"status": status, "attempt": attempt, "payments": len(txs)}
            log |= {"ms": ms, "queued": q.qsize()}
            if status != 200:
                log["reply"] = reply
            print(json.dumps(log), flush=True)
            if status == 200:
                break
            if status == 400:
                failed.set()
                return
            time.sleep(random.uniform(0, min(30, 2**attempt)))
        if done:
            return


def main(argv=None):
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("workload", type=Path, help="directory written by prepare.py")
    p.add_argument(
        "--url",
        action="append",
        required=True,
        metavar="BANK=URL",
        help="a bank's ingestor, e.g. B=http://localhost:8080 or an https function URL;"
        " banks without one are skipped",
    )
    p.add_argument(
        "--speed", type=float, default=60, help="simulated s per real s; 0 = no pacing"
    )
    p.add_argument(
        "--rate",
        type=float,
        help="events/s, open-loop, ignoring event times (load tests)",
    )
    a = p.parse_args(argv)
    urls = dict(u.split("=", 1) for u in a.url)

    meta = json.loads((a.workload / "workload.json").read_text())
    t0 = datetime.fromisoformat(meta["t0"])
    # one queue and sender per bank: with --speed N or --rate, a bank that's down
    # delays only itself
    queues = {b: queue.Queue() for b in urls}
    failed = threading.Event()
    senders = [
        threading.Thread(target=send, args=(b, u, sigv4(u), queues[b], failed))
        for b, u in urls.items()
    ]
    for s in senders:
        s.start()
    start = time.time()
    n = 0
    for n, e in enumerate(payments(a.workload), 1):
        if a.rate:  # each payment makes two events, `out` and `in`
            due = start + (n - 1) * 2 / a.rate
        elif a.speed:
            event = datetime.fromisoformat(e["event_time"])
            due = start + (event - t0).total_seconds() / a.speed
        else:  # --speed 0: as fast as the slowest ingestor, so memory stays flat
            while max(q.qsize() for q in queues.values()) >= BACKLOG:
                if failed.wait(0.01):
                    break
            due = start
        time.sleep(max(0, due - time.time()))  # open loop: never waits for a reply
        if failed.is_set():
            break
        t = tx(e)
        for b in {e["bank"], e["counterparty_bank"]} & urls.keys():
            queues[b].put(t)
    for q in queues.values():
        q.put(None)
    for s in senders:
        s.join()
    took = round(time.time() - start, 1)
    print(json.dumps({"service": "replay", "payments": n, "seconds": took}))
    if failed.is_set():
        raise SystemExit("replay stopped: an ingestor rejected a batch (400)")


if __name__ == "__main__":
    main()
