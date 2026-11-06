"""Score cases against Altman's labels for the dev or eval split."""

import csv
from collections import Counter, defaultdict
from collections.abc import Sized
from dataclasses import dataclass
from pathlib import Path
from typing import NamedTuple

from scorer.score import BANK_TRANSFERS

SPLITS = {"dev": ("2022-09-01", "2022-09-05"), "eval": ("2022-09-06", "2022-09-10")}
# altman-small; the generator only finishes its patterns after (contracts/data.md)
ORDINARY_END = "2022-09-10"


@dataclass(eq=False)
class Case:
    bank: str
    tier: int
    ids: set[str]


class Transfer(NamedTuple):
    day: str
    bank: str
    counterparty_bank: str
    payment_format: str


def load(workload: str | Path) -> tuple[dict[str, Transfer], dict[str, dict]]:
    """Return ({payment_id: Transfer}, {payment_id: label row})."""
    transfers = {}
    for path in sorted(Path(workload).glob("bank_*.csv")):
        with path.open() as source:
            for event in csv.DictReader(source):
                if event["direction"] == "out":
                    transfers[event["payment_id"]] = Transfer(
                        event["event_time"][:10],
                        event["bank"],
                        event["counterparty_bank"],
                        event["payment_format"],
                    )
    with (Path(workload) / "labels.csv").open() as source:
        labels = {row["payment_id"]: row for row in csv.DictReader(source)}
    return transfers, labels


def fraction(part: Sized, whole: Sized) -> float | None:
    return len(part) / len(whole) if whole else None


def score(
    cases: list[Case],
    transfers: dict[str, Transfer],
    labels: dict[str, dict],
    split: str,
) -> dict:
    """Detection quality for one split; laundering counts by its attempt's start day."""
    first, last = SPLITS[split]

    def day(p: str) -> str:
        return transfers[p].day

    def in_split(d: str) -> bool:
        return first <= d <= last

    attempts = defaultdict(list)
    for p, row in labels.items():
        if row["attempt_id"]:
            attempts[row["attempt_id"]].append(p)
    start = {}
    for ids in attempts.values():
        start.update(dict.fromkeys(ids, min(map(day, ids))))

    laundering = {p for p, row in labels.items() if row["altman_laundering"] == "1"}
    counted = [p for p in laundering if in_split(start.get(p, day(p)))]

    def scope(p: str) -> str:
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
        if in_split(start[ids[0]]):
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
        "cases_per_bank_per_day": {
            k: n / days for k, n in sorted(per_bank_tier.items())
        },
    }
