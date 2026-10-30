"""Replay one bank's payment events in time order, paced by event time.

Prints one JSON event per line for that bank's ingestor (Role 2), which publishes
them to the bank-local Kafka. Run one replay per bank with the same --start-at so
all three stay in step. Contract: docs/contracts/data.md.

    uv run python -m dataset.replay apps/dataset/fixtures/tiny A --speed 0
    uv run python -m dataset.replay apps/dataset/out/dev-small A --speed 60 --start-at 1791500000 | <ingestor>
"""

import argparse
import csv
import json
import time
from datetime import datetime
from pathlib import Path

p = argparse.ArgumentParser(
    description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
)
p.add_argument("workload", type=Path, help="directory written by prepare.py")
p.add_argument("bank", choices="ABC")
p.add_argument(
    "--speed", type=float, default=60, help="simulated s per real s; 0 = no pacing"
)
p.add_argument(
    "--start-at",
    type=float,
    help="epoch seconds that workload t0 maps to (default: now)",
)
a = p.parse_args()

t0 = datetime.fromisoformat(
    json.loads((a.workload / "workload.json").read_text())["t0"]
)
start = a.start_at or time.time()

with open(a.workload / f"bank_{a.bank}.csv") as f:
    for e in csv.DictReader(f):
        if a.speed:
            due = (
                start
                + (datetime.fromisoformat(e["event_time"]) - t0).total_seconds()
                / a.speed
            )
            time.sleep(max(0, due - time.time()))
        print(json.dumps(e | {"amount": float(e["amount"])}), flush=True)
