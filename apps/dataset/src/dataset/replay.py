"""Replay a workload to each bank's ingestor as pacs.008 batches over HTTP.

One process and one clock feed all three banks. Each GIRO or MEPS+ payment goes to
every bank it involves (once if intra-bank); other payment types are never sent.
Contract: docs/contracts/data.md (Replay, Ingestor).

    uv run python -m dataset.replay apps/dataset/fixtures/tiny --url B=http://localhost:8080 --speed 0
    uv run python -m dataset.replay apps/dataset/out/slice-6h --url A=<url> --url B=<url> --url C=<url>
    uv run python -m dataset.replay apps/dataset/out/slice-6h --url A=<url> --url B=<url> --url C=<url> --rate 2000
"""

import argparse
import csv
import heapq
import http.client
import itertools
import json
import queue
import random
import threading
import time
from collections.abc import Iterator
from contextlib import ExitStack
from datetime import datetime
from pathlib import Path
from urllib.parse import urlsplit

from ingestor import BICS

BATCH = 100  # payments per pacs.008 message, at most


def payments(workload: Path) -> Iterator[dict]:
    # Each transfer once, from its `out` event, in event-time order across the banks.
    with ExitStack() as files:
        banks = [
            csv.DictReader(files.enter_context(open(workload / f"bank_{b}.csv")))
            for b in "ABC"
        ]
        for e in heapq.merge(*banks, key=lambda e: e["event_time"]):
            if e["direction"] == "out" and e["payment_format"] in ("GIRO", "MEPS+"):
                yield e


def tx(e: dict) -> dict:
    return {
        "PmtId": {"EndToEndId": e["payment_id"]},
        "PmtTpInf": {"LclInstrm": {"Prtry": e["payment_format"]}},
        "IntrBkSttlmAmt": {"Ccy": "SGD", "Amt": f"{float(e['amount']):.2f}"},
        "AccptncDtTm": e["event_time"],
        "DbtrAcct": {"Id": {"Othr": {"Id": e["account"]}}},
        "DbtrAgt": {"FinInstnId": {"BICFI": BICS[e["bank"]]}},
        "CdtrAcct": {"Id": {"Othr": {"Id": e["counterparty_account"]}}},
        "CdtrAgt": {"FinInstnId": {"BICFI": BICS[e["counterparty_bank"]]}},
    }


def post(
    conn: http.client.HTTPConnection, path: str, body: str
) -> tuple[int | None, str]:
    # One POST -> (status, reply); status None on a network error, then conn reconnects.
    try:
        conn.request("POST", path, body, {"Content-Type": "application/json"})
        r = conn.getresponse()
        return r.status, r.read().decode()
    except (OSError, http.client.HTTPException) as e:
        conn.close()
        return None, f"{type(e).__name__}: {e}"


def send(bank: str, url: str, q: queue.Queue, failed: threading.Event) -> None:
    """POST this bank's queue in batches, one at a time, so its events keep their order.

    A batch is whatever became due while the last one was sent, at most BATCH payments.
    5xx, 429 and network errors resend the same batch with full jitter, 1 s doubling
    to 30 s; a 400 can't succeed on resend, so it stops replay.
    """
    u = urlsplit(url)
    # plain HTTP; HTTPS and SigV4 come with the EKS function URLs
    conn = http.client.HTTPConnection(u.netloc, timeout=35)  # ingestor gives up at 25s
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
        body = json.dumps({"FIToFICstmrCdtTrf": {"GrpHdr": hdr, "CdtTrfTxInf": txs}})
        for attempt in itertools.count():
            ts = time.time()
            status, reply = post(conn, u.path or "/", body)
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
        help="a bank's ingestor, e.g. B=http://localhost:8080; banks without one are skipped",
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
    # one queue and sender per bank: a bank that's down delays only itself
    queues = {b: queue.Queue() for b in urls}
    failed = threading.Event()
    senders = [
        threading.Thread(target=send, args=(b, urls[b], queues[b], failed))
        for b in urls
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
        else:
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
