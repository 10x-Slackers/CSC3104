"""Build a workload: per-bank event files for Banks A, B and C plus held-out labels.

Each transfer becomes an `out` event at the sender's bank and an `in` event at the
receiver's bank, sharing one payment_id. Labels never go into bank files.
Contract: docs/contracts/data.md. The first run downloads HI-Small (~476 MB, no Kaggle
login) into ~/.cache/kagglehub.

    uv run apps/dataset/prepare.py apps/dataset/fixtures/tiny --transfers apps/dataset/fixtures/tiny/transfers.csv
    uv run apps/dataset/prepare.py apps/dataset/out/dev-small --start 2022-09-05 --hours 6 --seed 1
    uv run apps/dataset/prepare.py apps/dataset/out/eval-small --start 2022-09-07 --hours 6 --seed 2
"""

import argparse
import hashlib
import json
import random
from pathlib import Path

import kagglehub
import pandas as pd

DATASET = "ealtman2019/ibm-transactions-for-anti-money-laundering-aml/versions/8"
# Text, not int: bank IDs are zero-padded ("010"); the second "Account" header becomes "Account.1".
ID_COLS = {"From Bank": str, "To Bank": str, "Account": str, "Account.1": str}
SCENARIOS = ("mule_chain", "lookalike", "loop", "absent_link", "over_limit")
# Draft: most hops in a mule chain, pending testing of longer chains.
HOP_LIMIT = 4
# Share of the amount forwarded to the next hop.
FORWARD = (0.90, 0.98)
# Benign lookalikes follow the same shape as mule chains but must not be flagged.
LOOKALIKE_FORWARD = (0.20, 0.50)
DELAY_MIN = (1, 30)  # minutes between hops
EVENT_COLS = ["payment_id", "event_time", "bank", "direction", "account", "counterparty_bank", "amount", "payment_format"]  # fmt: skip
LABEL_COLS = ["payment_id", "altman_laundering", "chain_id", "hop", "scenario"]


def pid(seed):
    return hashlib.sha256(seed.encode()).hexdigest()[:16]


def altman(start, hours):
    # Slice HI-Small to USD transfers between different accounts, mapped onto A/B/C.
    df = pd.read_csv(
        kagglehub.dataset_download(DATASET, path="HI-Small_Trans.csv"), dtype=ID_COLS
    )
    df["time"] = pd.to_datetime(df["Timestamp"], format="%Y/%m/%d %H:%M")
    start = pd.Timestamp(start)
    fb, tb = df["From Bank"].astype(int), df["To Bank"].astype(int)  # "010" == "0010"
    keep = (
        (df.time >= start)
        & (df.time < start + pd.Timedelta(hours=hours))
        & (df["Payment Currency"] == "US Dollar")
        & (df["Receiving Currency"] == "US Dollar")
        & ~((fb == tb) & (df["Account"] == df["Account.1"]))  # self-transfers
    )
    df, fb, tb = df[keep], fb[keep], tb[keep]
    sim = dict(enumerate("ABC"))
    return pd.DataFrame(
        {
            # source row number: the same transfer keeps its ID in every slice
            "payment_id": [pid(f"altman:{i}") for i in df.index],
            "time": df.time,
            "from_bank": (fb % 3).map(sim),
            # account IDs are only unique per source bank
            "from_account": fb.astype(str) + "-" + df["Account"],
            "to_bank": (tb % 3).map(sim),
            "to_account": tb.astype(str) + "-" + df["Account.1"],
            "amount": df["Amount Paid"],
            "payment_format": df["Payment Format"],
            "altman_laundering": df["Is Laundering"],
        }
    )


def plant(t, n, seed):
    # Add n chains per scenario between existing accounts, in the first half of the slice.
    rng = random.Random(seed)
    accounts = {
        b: sorted(
            set(t.from_account[t.from_bank == b]) | set(t.to_account[t.to_bank == b])
        )
        for b in "ABC"
    }
    t0, half = t.time.min(), (t.time.max() - t.time.min()) / 2
    fixed_hops = {"over_limit": HOP_LIMIT + 2, "loop": 3, "absent_link": 3}
    rows = []
    for scenario in SCENARIOS:
        for k in range(n):
            chain = f"{scenario}-{seed}-{k}"
            hops = fixed_hops.get(scenario) or rng.randint(2, HOP_LIMIT)
            if scenario == "loop":
                banks = rng.sample("ABC", 3)
            else:
                banks = [rng.choice("ABC")]
                for _ in range(hops):
                    banks.append(rng.choice([b for b in "ABC" if b != banks[-1]]))
            accts = [rng.choice(accounts[b]) for b in banks]
            if scenario == "loop":  # last hop returns to the first account
                banks, accts = banks + banks[:1], accts + accts[:1]
            path = list(zip(banks, accts))
            amount = round(rng.uniform(5_000, 50_000), 2)
            time = (t0 + half * rng.random()).floor("min")
            fmt = rng.choice(["ACH", "Wire"])
            share = LOOKALIKE_FORWARD if scenario == "lookalike" else FORWARD
            for h in range(hops):
                if not (scenario == "absent_link" and h == 1):  # the missing link
                    src, dst = path[h], path[h + 1]  # (bank, account)
                    row = [pid(f"{chain}:{h}"), time, *src, *dst, amount, fmt]
                    rows.append(row + [0, chain, h, scenario])
                amount = round(amount * rng.uniform(*share), 2)
                time += pd.Timedelta(minutes=rng.randint(*DELAY_MIN))
    planted = pd.DataFrame(rows, columns=[*t.columns, "chain_id", "hop", "scenario"])
    return pd.concat([t, planted], ignore_index=True).astype({"hop": "Int64"})


def split(t):
    # Transfers -> ({bank: events}, labels).
    sides = {"out": ("from", "to"), "in": ("to", "from")}
    ev = pd.concat(
        t.assign(
            direction=d,
            bank=t[f"{me}_bank"],
            account=t[f"{me}_account"],
            counterparty_bank=t[f"{other}_bank"],
        )
        for d, (me, other) in sides.items()
    )
    # an intra-bank transfer's `out` stays ahead of its `in`
    ev = ev.sort_values(["time", "payment_id"])
    ev["event_time"] = ev.time.dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    labels = t.loc[(t.altman_laundering == 1) | t.chain_id.notna(), LABEL_COLS]
    return {b: ev.loc[ev.bank == b, EVENT_COLS] for b in "ABC"}, labels


def read_transfers(path):
    return pd.read_csv(
        path, parse_dates=["time"], dtype={"payment_id": str, "hop": "Int64"}
    )


if __name__ == "__main__":
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("out", type=Path, help="output directory")
    p.add_argument(
        "--transfers", help="transfers CSV (e.g. the fixture) instead of Altman"
    )
    p.add_argument("--start", default="2022-09-05", help="slice start (Altman time)")
    p.add_argument("--hours", type=float, default=6, help="slice length")
    p.add_argument("--chains", type=int, default=10, help="planted chains per scenario")
    p.add_argument("--seed", type=int, default=1)
    a = p.parse_args()

    if a.transfers:
        t = read_transfers(a.transfers)
    else:
        t = plant(altman(a.start, a.hours), a.chains, a.seed)
    banks, labels = split(t)
    a.out.mkdir(parents=True, exist_ok=True)
    for b, ev in banks.items():
        ev.to_csv(a.out / f"bank_{b}.csv", index=False)
    labels.to_csv(a.out / "labels.csv", index=False)
    meta = {
        "t0": t.time.min().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": a.transfers or DATASET,
    }
    if not a.transfers:
        meta |= {"start": a.start, "hours": a.hours, "chains": a.chains, "seed": a.seed}
    meta |= {"transfers": len(t), "events": {b: len(ev) for b, ev in banks.items()}}
    (a.out / "workload.json").write_text(json.dumps(meta, indent=2) + "\n")
    print(json.dumps(meta))
