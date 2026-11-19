"""Privacy-preserving detection of cross-bank money-mule chains.

One module per box in docs/architecture.drawio.png:

    replay -> ingestor -> Kafka -> scorer -> Valkey -> node <-> other banks' nodes
                                                        |
                                             PostgreSQL + case -> summary

Every bank runs the same code. `BANK` (A, B or C) tells a process which bank it is.
"""

import csv
import json
import sys
import time
from collections.abc import Iterator
from pathlib import Path

BANKS = ("A", "B", "C")
BANK_TRANSFERS = ("GIRO", "MEPS+")  # the only payment types replayed, ingested and scored


def log(service: str, **fields) -> None:
    """One JSON line to stdout; the evaluation joins these lines on payment and chain IDs."""
    sys.stdout.write(json.dumps({"ts": time.time(), "service": service} | fields) + "\n")
    sys.stdout.flush()


def read_events(path: str | Path) -> Iterator[dict]:
    """A workload's bank file as the events the ingestor would write to Kafka."""
    with Path(path).open() as source:
        for row in csv.DictReader(source):
            yield row | {"amount": float(row["amount"])}
