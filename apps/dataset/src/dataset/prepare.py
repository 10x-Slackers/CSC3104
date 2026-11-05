"""Build a workload: per-bank event files for Banks A, B and C plus held-out labels.

Each transfer becomes an `out` event at the sender's bank and an `in` event at the
receiver's bank, sharing one payment_id. Labels never go into bank files.
Contract: docs/contracts/data.md. The first run downloads the Altman files (HI-Small
~476 MB, HI-Medium ~3 GB, no Kaggle login) into ~/.cache/kagglehub.

    uv run python -m dataset.prepare apps/dataset/fixtures/tiny --transfers apps/dataset/fixtures/tiny/transfers.csv
    uv run python -m dataset.prepare apps/dataset/out/slice-6h --start 2022-09-05 --hours 6
    uv run python -m dataset.prepare apps/dataset/out/altman-small
    uv run python -m dataset.prepare apps/dataset/out/altman-medium --altman HI-Medium
"""

import argparse
import hashlib
import json
from pathlib import Path

import kagglehub
import pandas as pd

DATASET = "ealtman2019/ibm-transactions-for-anti-money-laundering-aml/versions/8"
# Account IDs are hex text; the second "Account" header becomes "Account.1".
ID_COLS = {"Account": str, "Account.1": str}
# Few distinct values: categories keep HI-Medium (32M rows) in memory.
CATEGORIES = ["Timestamp", "Receiving Currency", "Payment Currency", "Payment Format"]
TIME = "%Y-%m-%dT%H:%M:%SZ"
SGD_PER_USD = 1.4136  # MAS average, September 2022
FORMATS = {"ACH": "GIRO", "Wire": "MEPS+"}  # Singapore's bank transfer systems
EVENT_COLS = ["payment_id", "event_time", "bank", "direction", "account", "counterparty_account", "counterparty_bank", "amount", "payment_format"]  # fmt: skip
LABEL_COLS = ["payment_id", "altman_laundering", "attempt_id", "altman_pattern", "chain_id", "hop", "scenario"]  # fmt: skip


def pid(seed):
    return hashlib.sha256(seed.encode()).hexdigest()[:16]


def altman(name, start=None, hours=6):
    # One Altman file (whole, or a slice from start) mapped onto A/B/C.
    def get(f):
        return kagglehub.dataset_download(DATASET, path=f"{name}_{f}")

    df = pd.read_csv(
        get("Trans.csv"), dtype=ID_COLS | dict.fromkeys(CATEGORIES, "category")
    )
    # Patterns file: a BEGIN line naming each attempt's type, its transfers, an END line.
    p = pd.read_csv(get("Patterns.txt"), names=df.columns, dtype=ID_COLS)
    kind = p.Timestamp.str.extract(r"^BEGIN LAUNDERING ATTEMPT - ([A-Z-]+)")[0]
    p["attempt_id"], p["altman_pattern"] = kind.notna().cumsum() - 1, kind.ffill()
    p = p[p["Amount Paid"].notna()].astype({"From Bank": int, "To Bank": int})
    # A transfer listed twice keeps its first attempt; bank IDs compare as integers.
    key = ["Timestamp", "From Bank", "Account", "To Bank", "Account.1", "Amount Paid"]
    p = p[[*key, "attempt_id", "altman_pattern"]].drop_duplicates(key)
    lau = df[df["Is Laundering"] == 1].astype({"Timestamp": str}).reset_index()
    hits = lau.merge(p, on=key).set_index("index")
    df["attempt_id"], df["altman_pattern"] = hits.attempt_id, hits.altman_pattern
    # The dataset's own rate to USD per currency, from payments received in USD.
    usd = (df["Amount Received"] / df["Amount Paid"])[
        df["Receiving Currency"] == "US Dollar"
    ]
    rate = usd.groupby(df["Payment Currency"]).median()
    # parse each distinct minute once (to_datetime on a category returns a category)
    ts = df["Timestamp"].cat
    df["time"] = pd.to_datetime(ts.categories, format="%Y/%m/%d %H:%M")[ts.codes]
    fb, tb = df["From Bank"], df["To Bank"]
    keep = ~((fb == tb) & (df["Account"] == df["Account.1"]))  # self-transfers
    if start:
        start = pd.Timestamp(start)
        keep &= (df.time >= start) & (df.time < start + pd.Timedelta(hours=hours))
    df, fb, tb = df[keep], fb[keep], tb[keep]
    return pd.DataFrame(
        {
            # file and source row: the same transfer keeps its ID in every slice
            "payment_id": [pid(f"{name}:{i}") for i in df.index],
            "time": df.time,
            "from_bank": pd.Categorical.from_codes(fb % 3, list("ABC")),
            # account IDs are only unique per source bank
            "from_account": fb.astype(str) + "-" + df["Account"],
            "to_bank": pd.Categorical.from_codes(tb % 3, list("ABC")),
            "to_account": tb.astype(str) + "-" + df["Account.1"],
            # in cents, at least SGD 0.01 (a 0.01 Yen cheque would round to 0)
            "amount": (
                df["Amount Paid"]
                * df["Payment Currency"].map(rate).astype(float)
                * SGD_PER_USD
            )
            .round(2)
            .clip(lower=0.01),
            "payment_format": df["Payment Format"].cat.rename_categories(
                lambda f: FORMATS.get(f, f)
            ),
            "altman_laundering": df["Is Laundering"],
            "attempt_id": df.attempt_id.astype("Int64"),
            "altman_pattern": df.altman_pattern,
        }
    )


def split(t):
    # Transfers -> ({bank: events}, labels), with event_time already a TIME string.
    labels = t.reindex(columns=LABEL_COLS)  # Altman workloads have no planted chains
    labels = labels[(labels.altman_laundering == 1) | labels.chain_id.notna()]
    sides = {"out": ("from", "to"), "in": ("to", "from")}
    # same-minute transfers keep the source file's order
    t = t.sort_values("time", kind="stable", ignore_index=True)
    ts = t.time.astype("category").cat  # format each distinct time once
    when = ts.rename_categories(ts.categories.strftime(TIME))
    ev = pd.concat(
        t[["payment_id", "amount", "payment_format"]].assign(
            event_time=when,
            direction=d,
            bank=t[f"{me}_bank"],
            account=t[f"{me}_account"],
            counterparty_account=t[f"{other}_account"],
            counterparty_bank=t[f"{other}_bank"],
        )
        for d, (me, other) in sides.items()
    )
    # in transfer order; an intra-bank transfer's `out` stays ahead of its `in`
    ev = ev.sort_index(kind="stable")
    return {b: ev.loc[ev.bank == b, EVENT_COLS] for b in "ABC"}, labels


def read_transfers(path):
    return pd.read_csv(
        path,
        parse_dates=["time"],
        dtype={"payment_id": str, "attempt_id": "Int64", "hop": "Int64"},
    )


if __name__ == "__main__":
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("out", type=Path, help="output directory")
    p.add_argument(
        "--transfers", help="transfers CSV (e.g. the fixture) instead of Altman"
    )
    p.add_argument("--altman", default="HI-Small", choices=["HI-Small", "HI-Medium"])
    p.add_argument("--start", help="slice start (Altman time); default: whole file")
    p.add_argument("--hours", type=float, default=6, help="slice length")
    a = p.parse_args()

    if a.transfers:
        t = read_transfers(a.transfers)
    else:
        t = altman(a.altman, a.start, a.hours)
    banks, labels = split(t)
    a.out.mkdir(parents=True, exist_ok=True)
    for b, ev in banks.items():
        ev.to_csv(a.out / f"bank_{b}.csv", index=False)
    labels.to_csv(a.out / "labels.csv", index=False)
    meta = {
        "t0": t.time.min().strftime(TIME),
        "source": a.transfers or f"{DATASET}/{a.altman}",
    }
    if not a.transfers and a.start:
        meta |= {"start": a.start, "hours": a.hours}
    meta |= {"transfers": len(t), "events": {b: len(ev) for b, ev in banks.items()}}
    (a.out / "workload.json").write_text(json.dumps(meta, indent=2) + "\n")
    print(json.dumps(meta))
