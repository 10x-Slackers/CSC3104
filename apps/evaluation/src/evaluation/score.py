"""Score cases against Altman's labels for the dev or eval split."""

import csv
from collections import Counter, defaultdict
from pathlib import Path

from scorer.score import BANK_TRANSFERS

SPLITS = {"dev": ("2022-09-01", "2022-09-05"), "eval": ("2022-09-06", "2022-09-10")}


def load(workload):
    """Return ({payment_id: outgoing event}, {payment_id: label row})."""
    transfers = {}
    for path in sorted(Path(workload).glob("bank_*.csv")):
        with path.open() as source:
            for event in csv.DictReader(source):
                if event["direction"] == "out":
                    transfers[event["payment_id"]] = event
    with (Path(workload) / "labels.csv").open() as source:
        labels = {row["payment_id"]: row for row in csv.DictReader(source)}
    return transfers, labels


def fraction(part, whole):
    return len(part) / len(whole) if whole else None


def score(cases, transfers, labels, split):
    """Detection quality for one split; laundering counts by its attempt's start day."""
    first, last = SPLITS[split]

    def day(p):
        return transfers[p]["event_time"][:10]

    def in_split(d):
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
    ordinary_end = max(day(p) for p in transfers if p not in laundering)

    def scope(p):
        t = transfers[p]
        if t["bank"] == t["counterparty_bank"]:
            return "same_bank"
        if t["payment_format"] not in BANK_TRANSFERS:
            return t["payment_format"]
        return "after_ordinary_activity" if day(p) > ordinary_end else "in_scope"

    flagged = set().union(*(c.ids for c in cases))
    tier2 = set().union(*(c.ids for c in cases if c.tier == 2))

    patterns = defaultdict(Counter)
    for ids in attempts.values():
        if in_split(start[ids[0]]):
            row = patterns[labels[ids[0]]["altman_pattern"]]
            row["attempts"] += 1
            row["caught_any_tier"] += not flagged.isdisjoint(ids)
            row["caught_tier2"] += not tier2.isdisjoint(ids)

    in_scope = [p for p in counted if scope(p) == "in_scope"]
    own_days = [p for p in transfers if in_split(day(p))]
    own_flagged = [p for p in own_days if p in flagged]
    load = Counter(
        f"{c.bank} tier {c.tier}" for c in cases if in_split(max(map(day, c.ids)))
    )
    days = len({day(p) for p in own_days})

    return {
        "patterns": {name: dict(row) for name, row in sorted(patterns.items())},
        "payment_recall": fraction([p for p in in_scope if p in flagged], in_scope),
        "laundering_by_scope": dict(sorted(Counter(map(scope, counted)).items())),
        "precision": fraction([p for p in own_flagged if p in laundering], own_flagged),
        "false_positive_rate": fraction(
            [p for p in own_flagged if p not in laundering], own_days
        ),
        "cases_per_bank_per_day": {k: n / days for k, n in sorted(load.items())},
    }
