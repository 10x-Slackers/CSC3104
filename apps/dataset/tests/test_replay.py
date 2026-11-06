import csv
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from dataset import replay
from ingestor import records

TINY = Path(__file__).parents[1] / "fixtures/tiny"


def ingestor(bank, replies=()):
    """A stand-in for one bank's ingestor: maps each batch with the real records(),
    answers with `replies` first (e.g. 503), then 200, and 400 if records() rejects it."""
    got, replies = [], list(replies)

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"  # replay keeps one connection per bank

        def do_POST(self):
            batch = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            try:
                events = records(batch, bank)
                status = replies.pop(0) if replies else 200
            except (KeyError, TypeError, ValueError):
                events, status = [], 400
            got.append((status, batch, events if status == 200 else []))
            self.send_response(status)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return f"http://127.0.0.1:{server.server_port}", got


def test_every_bank_gets_its_bank_transfers_once(monkeypatch):
    monkeypatch.setattr(replay.random, "uniform", lambda a, b: 0)  # no backoff wait
    banks = {b: ingestor(b, [503] if b == "B" else []) for b in "ABC"}
    replay.main(
        [str(TINY), "--speed", "0", *(f"--url={b}={u}" for b, (u, _) in banks.items())]
    )
    for b, (_, got) in banks.items():
        # the ingestor's events are this bank's file without cheque and card (p01, p04)
        produced = [r["value"] for _, _, events in got for r in events]
        with open(TINY / f"bank_{b}.csv") as f:
            expected = [
                e | {"amount": float(e["amount"])}
                for e in csv.DictReader(f)
                if e["payment_format"] in ("GIRO", "MEPS+")
            ]
        assert sorted(produced, key=str) == sorted(expected, key=str)
    # B's first batch got a 503 and was resent unchanged, with the same MsgId
    (first, sent), (again, resent) = [(s, batch) for s, batch, _ in banks["B"][1][:2]]
    assert (first, again) == (503, 200) and sent == resent


def test_a_rejected_batch_stops_replay():
    url, got = ingestor("A", [400])
    with pytest.raises(SystemExit, match="400"):
        replay.main([str(TINY), "--speed", "0", f"--url=A={url}"])
    assert [status for status, _, _ in got] == [400]  # never resent
