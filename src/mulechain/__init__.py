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
BANK_TRANSFERS = ("GIRO", "MEPS+")  # the only payment types replayed and ingested


def log(service: str, **fields) -> None:
    """One JSON line to stdout; the lines of different services share payment IDs."""
    sys.stdout.write(json.dumps({"ts": time.time(), "service": service} | fields) + "\n")
    sys.stdout.flush()


def read_events(path: str | Path) -> Iterator[dict]:
    """The bank transfers in a workload's bank file, as the ingestor would write them to Kafka."""
    with Path(path).open() as source:
        for row in csv.DictReader(source):
            if row["payment_format"] in BANK_TRANSFERS:
                yield row | {"amount": float(row["amount"])}
