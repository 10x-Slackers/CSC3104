"""Replay: send a workload to each bank's ingestor as pacs.008 batches, like a payment network.

    python -m mulechain.replay workloads/tiny --url A=http://localhost:8080 --url B=... --url C=...
    python -m mulechain.replay workloads/slice-6h --url ... --speed 0     # as fast as it is taken
    python -m mulechain.replay workloads/slice-6h --url ... --rate 2000   # a fixed events/s

One process feeds all three banks. Every transfer goes to each bank on it, once.

Patterns (docs/patterns.md): each bank has its own queue and sender, so a slow bank
delays only itself; a failed batch is sent again with capped exponential backoff and
full jitter; and with --speed 0 the queues are bounded, which is back-pressure.

A Lambda function URL works too: those requests are signed (mulechain/serverless.py).
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

from mulechain import BANKS, log, read_events
from mulechain.ingestor import BICS
from mulechain.serverless import request_headers

BATCH = 100  # transfers per pacs.008 message, at most
BACKLOG = 1000  # --speed 0: transfers queued per bank, at most


def transfers(workload: Path) -> Iterator[dict]:
    """Each bank transfer once, from its `out` event, in time order across the banks."""
    banks = [read_events(workload / f"bank_{bank}.csv") for bank in BANKS]
    for e in heapq.merge(*banks, key=lambda e: e["event_time"]):
        if e["direction"] == "out":
            yield e


def pacs008(e: dict) -> dict:
    """One transfer as a pacs.008 credit transfer."""
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


def message(message_id: str, batch: list[dict]) -> dict:
    """A pacs.008 message: a header and its transfers."""
    header = {"MsgId": message_id, "CreDtTm": batch[-1]["AccptncDtTm"], "NbOfTxs": str(len(batch))}
    return {"FIToFICstmrCdtTrf": {"GrpHdr": header, "CdtTrfTxInf": batch}}


def next_batch(todo: queue.Queue) -> tuple[list[dict], bool]:
    """Whatever became due while the last batch was being sent; True at the workload's end."""
    batch = [todo.get()]
    while batch[-1] and len(batch) < BATCH and not todo.empty():
        batch.append(todo.get())
    return [t for t in batch if t], batch[-1] is None


def send(bank: str, url: str, todo: queue.Queue, failed: threading.Event) -> None:
    """POST one bank's queue in batches, one at a time, so its events keep their order.

    Anything but a 200 or a 400 sends the same batch again, after
    random(0, min(30 s, 2^attempt s)). A 400 can never succeed, so it stops the replay.
    """
    target = urlsplit(url)
    https = target.scheme == "https"
    connect = http.client.HTTPSConnection if https else http.client.HTTPConnection
    connection = connect(target.netloc, timeout=35)
    for number in itertools.count(1):
        batch, last = next_batch(todo)
        if not batch or failed.is_set():
            return
        message_id = f"replay-{bank}-{number:06d}"
        body = json.dumps(message(message_id, batch)).encode()
        for attempt in itertools.count():
            start = time.monotonic()
            try:
                connection.request("POST", target.path or "/", body, request_headers(url, body))
                reply = connection.getresponse()
                status, text = reply.status, reply.read().decode()
            except Exception as error:  # noqa: BLE001 - any network or signing error: retry
                connection.close()
                status, text = None, repr(error)
            ms = round((time.monotonic() - start) * 1000, 1)
            fields = {"status": status, "attempt": attempt, "transfers": len(batch), "ms": ms}
            log("replay", bank=bank, message=message_id, queued=todo.qsize(), **fields)
            if status == 200:
                break
            if status == 400:
                log("replay", bank=bank, message=message_id, rejected=text)
                failed.set()
                return
            time.sleep(random.uniform(0, min(30, 2**attempt)))
        if last:
            return


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("workload", type=Path, help="a directory under workloads/")
    parser.add_argument("--url", action="append", required=True, metavar="BANK=URL")
    parser.add_argument(
        "--speed", type=float, default=60, help="simulated seconds per second; 0: unpaced"
    )
    parser.add_argument("--rate", type=float, help="events per second, ignoring event times")
    args = parser.parse_args()
    urls = dict(pair.split("=", 1) for pair in args.url)
    t0 = datetime.fromisoformat(json.loads((args.workload / "workload.json").read_text())["t0"])

    queues = {bank: queue.Queue() for bank in urls}
    failed = threading.Event()
    senders = [threading.Thread(target=send, args=(b, urls[b], queues[b], failed)) for b in urls]
    for sender in senders:
        sender.start()
    start, count = time.time(), 0
    for count, e in enumerate(transfers(args.workload), 1):
        due = start
        if args.rate:  # open loop; each transfer makes two events, `out` and `in`
            due += (count - 1) * 2 / args.rate
        elif args.speed:  # open loop, at the workload's own pace
            due += (datetime.fromisoformat(e["event_time"]) - t0).total_seconds() / args.speed
        else:  # back-pressure: wait while any bank's queue is full
            while max(q.qsize() for q in queues.values()) >= BACKLOG and not failed.wait(0.01):
                pass
        time.sleep(max(0, due - time.time()))
        if failed.is_set():
            break
        for bank in {e["bank"], e["counterparty_bank"]} & urls.keys():
            queues[bank].put(pacs008(e))
    for q in queues.values():
        q.put(None)
    for sender in senders:
        sender.join()
    log("replay", transfers=count, seconds=round(time.time() - start, 1))
    if failed.is_set():
        raise SystemExit("replay stopped: an ingestor rejected a batch (400)")


if __name__ == "__main__":
    main()
