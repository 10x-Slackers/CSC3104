"""Evaluate: run all three banks in one process and score their cases against the labels.

    python -m mulechain.evaluate workloads/altman-small --split dev
    python -m mulechain.evaluate workloads/tiny --cases          # list the cases instead

This is the cluster's own code with the infrastructure swapped out: fakeredis for
Valkey, SQLite for PostgreSQL, and a direct function call where the cluster has gRPC
over mTLS. Each bank still has its own keys, state, privacy node and case store, and
the nodes still only exchange protocol messages. Because detection uses event time,
the cases are the ones a cluster replay of the same workload ends with.

The services' JSON log lines go to stderr; the report is the only thing on stdout.
"""

import argparse
import csv
import json
import os
import sys
import tempfile
import time
from collections import Counter, defaultdict
from contextlib import redirect_stdout
from dataclasses import dataclass
from pathlib import Path
from typing import NamedTuple

import fakeredis
import grpc

from mulechain import BANK_TRANSFERS, BANKS, read_events
from mulechain.case.store import CaseStore
from mulechain.node import node as node_module
from mulechain.node.bank import Bank
from mulechain.node.node import Node, identity
from mulechain.node.store import Store
from mulechain.scorer import PairKeys, Scorer

SPLITS = {"dev": ("2022-09-01", "2022-09-05"), "eval": ("2022-09-06", "2022-09-10")}
ORDINARY_END = "2022-09-10"  # the generator only finishes its laundering patterns after this


class Refused(grpc.RpcError):
    """A peer aborted an in-process call, as a gRPC status."""

    def code(self) -> grpc.StatusCode:
        return self.args[0]


class Context:
    """What gRPC tells a node about a call: who is calling and how long they will wait."""

    def __init__(self, caller: str, timeout: float) -> None:
        self.caller, self.deadline = caller, time.monotonic() + timeout

    def auth_context(self) -> dict:
        return {"x509_subject_alternative_name": [identity(self.caller).encode()]}

    def time_remaining(self) -> float:
        return self.deadline - time.monotonic()

    def abort(self, code: grpc.StatusCode, details: str):
        raise Refused(code, details)


def start(workload: str | Path, key_dir: str | Path) -> dict[str, Node]:
    """Give each bank keys, a Valkey, a node and a case store; then score its events."""
    nodes: dict[str, Node] = {}
    for bank in BANKS:
        (Path(key_dir) / bank).mkdir()
        (Path(key_dir) / bank / "1").write_bytes(os.urandom(64))  # the bank's own key
        cases = CaseStore(":memory:", bank)

        def call(peer: str, method: str, request, timeout: float, caller: str = bank):
            return getattr(nodes[peer], method)(request, Context(caller, timeout))

        def send_cases(messages: list[dict], cases: CaseStore = cases) -> None:
            for message in messages:
                cases.record(message)

        db = fakeredis.FakeValkey(decode_responses=True)
        nodes[bank] = Node(Bank(bank, db), Store(), Path(key_dir) / bank, call, send_cases)
        nodes[bank].cases = cases
    for node in nodes.values():
        node.refresh_keys()  # key agreement: fetch the peers' public keys
    for bank, node in nodes.items():
        scorer = Scorer(bank, node.bank.db, PairKeys(bank, node.key_dir, node.bank.db))
        for event in read_events(Path(workload) / f"bank_{bank}.csv"):
            scorer.process(event)
    return nodes


@dataclass(eq=False)
class Case:
    bank: str
    tier: int
    ids: set[str]  # the bank's own payment IDs on the case


def run(workload: str | Path) -> tuple[list[Case], dict[str, int]]:
    """Everything a cluster does for a replay; returns (cases, searches by final state)."""
    with tempfile.TemporaryDirectory() as key_dir:
        nodes = start(workload, key_dir)
        while sum(node.work_once() for node in nodes.values()):
            pass  # until no bank has a hit to read, a search to run or a message to send
    cases, searches = [], Counter()
    for bank, node in nodes.items():
        payments = node.cases.payments()
        cases += [Case(bank, c["tier"], set(payments[c["id"]])) for c in node.cases.cases()]
        searches.update(row["state"] for row in node.store.db.all("SELECT state FROM searches"))
    return cases, dict(sorted(searches.items()))


class Transfer(NamedTuple):
    day: str
    bank: str
    counterparty_bank: str
    payment_format: str


def load(workload: str | Path) -> tuple[dict[str, Transfer], dict[str, dict]]:
    """({payment ID: Transfer}, {payment ID: label row})."""
    transfers = {}
    for path in sorted(Path(workload).glob("bank_*.csv")):
        with path.open() as source:
            for e in csv.DictReader(source):
                if e["direction"] == "out":
                    transfers[e["payment_id"]] = Transfer(
                        e["event_time"][:10], e["bank"], e["counterparty_bank"], e["payment_format"]
                    )
    with (Path(workload) / "labels.csv").open() as source:
        labels = {row["payment_id"]: row for row in csv.DictReader(source)}
    return transfers, labels


def fraction(part, whole) -> float | None:
    return len(part) / len(whole) if whole else None


def score(
    cases: list[Case], transfers: dict[str, Transfer], labels: dict[str, dict], split: str
) -> dict:
    """Detection quality for one split. Laundering counts by the day its attempt started."""
    first, last = SPLITS[split]

    def day(p: str) -> str:
        return transfers[p].day

    def in_split(d: str) -> bool:
        return first <= d <= last

    attempts = defaultdict(list)
    for p, row in labels.items():
        if row["attempt_id"]:
            attempts[row["attempt_id"]].append(p)
    started = {}
    for ids in attempts.values():
        started.update(dict.fromkeys(ids, min(map(day, ids))))

    laundering = {p for p, row in labels.items() if row["altman_laundering"] == "1"}
    counted = [p for p in laundering if in_split(started.get(p, day(p)))]

    def scope(p: str) -> str:
        """Why a laundering payment is or isn't one this system could flag."""
        t = transfers[p]
        if t.bank == t.counterparty_bank:
            return "same_bank"
        if t.payment_format not in BANK_TRANSFERS:
            return t.payment_format
        return "after_ordinary_activity" if day(p) > ORDINARY_END else "in_scope"

    flagged = set().union(*(c.ids for c in cases))
    tier2 = set().union(*(c.ids for c in cases if c.tier == 2))

    patterns = defaultdict(Counter)
    for ids in attempts.values():
        if in_split(started[ids[0]]):
            row = patterns[labels[ids[0]]["altman_pattern"]]
            caught = not flagged.isdisjoint(ids)
            reachable = any(scope(p) == "in_scope" for p in ids)
            row["attempts"] += 1
            row["caught_any_tier"] += caught
            row["caught_tier2"] += not tier2.isdisjoint(ids)
            row["attempts_in_scope"] += reachable
            row["caught_in_scope"] += reachable and caught

    in_scope = [p for p in counted if scope(p) == "in_scope"]
    own_days = {p for p in transfers if in_split(day(p))}
    own_flagged = own_days & flagged
    per_bank_tier = Counter(
        f"{c.bank} tier {c.tier}" for c in cases if in_split(max(map(day, c.ids)))
    )
    days = len({day(p) for p in own_days})
    return {
        "patterns": {name: dict(row) for name, row in sorted(patterns.items())},
        "payment_recall": fraction(flagged.intersection(in_scope), in_scope),
        "laundering_by_scope": dict(sorted(Counter(map(scope, counted)).items())),
        "precision": fraction(own_flagged & laundering, own_flagged),
        "false_positive_rate": fraction(own_flagged - laundering, own_days),
        "cases_per_bank_per_day": {k: n / days for k, n in sorted(per_bank_tier.items())},
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("workload", help="a directory under workloads/, with labels.csv")
    parser.add_argument("--split", choices=SPLITS, default="dev")
    parser.add_argument("--cases", action="store_true", help="print the cases, not their score")
    parser.add_argument("--hop-limit", type=int, default=node_module.HOP_LIMIT)
    args = parser.parse_args()
    node_module.HOP_LIMIT = args.hop_limit
    with redirect_stdout(sys.stderr):
        cases, searches = run(args.workload)
    if args.cases:
        listed = sorted((c.bank, c.tier, " ".join(sorted(c.ids))) for c in cases)
        report = {"searches": searches, "cases": listed}
    else:
        report = {"split": args.split, "searches": searches}
        report |= score(cases, *load(args.workload), args.split)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
