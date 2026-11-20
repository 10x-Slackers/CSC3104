"""Latency and gaps of a cluster run, from the services' JSON log lines.

    for bank in a b c; do for app in scorer case privacy-node; do
      kubectl logs -n bank-$bank -l app=$app --tail=-1 --max-log-requests=20
    done; done > run.log
    kubectl logs -n replay job/replay >> run.log
    uv run python deploy/measure.py run.log

Collect within five minutes of the replay, before KEDA removes the extra scorers and
their logs. Every measure compares times within one bank (docs/evaluation.md).
"""

import json
import sys
from collections import defaultdict
from itertools import chain, pairwise


def lines(paths: list[str]):
    for path in paths:
        with open(path, errors="replace") as source:
            for line in source:
                try:  # a node's log files prefix each line with a timestamp
                    yield json.loads(line[line.index("{") :])
                except ValueError:
                    pass


def spread(values: list[float]) -> str:
    values = sorted(values)
    if not values:
        return "none"
    p50, p95 = values[len(values) // 2], values[int(len(values) * 0.95)]
    return f"p50 {p50:.2f}  p95 {p95:.2f}  max {values[-1]:.2f}  (n = {len(values)})"


def main() -> None:
    ingested = {}  # (bank, payment) -> when Kafka took the event that made a link or hit
    stored = defaultdict(list)  # bank -> the case service's stored lines
    accepted = defaultdict(list)  # bank -> when the ingestor accepted each replay batch
    searches = defaultdict(list)  # final state -> search milliseconds
    for line in lines(sys.argv[1:]):
        bank, service = line.get("bank"), line.get("service")
        if service == "scorer" and "kafka_ts" in line:
            key = (bank, line["payment_id"])
            ingested[key] = max(line["kafka_ts"], ingested.get(key, 0))
        elif service == "case" and line.get("event") == "stored":
            stored[bank].append(line)
        elif service == "node" and line.get("event") == "search":
            searches[line["state"]].append(line["ms"])
        elif service == "replay" and line.get("status") == 200:
            accepted[bank].append(line["ts"])

    # Detection latency: from the last of a case's payments entering Kafka to the case's
    # first stored message, at the same bank.
    latency, seen = defaultdict(list), set()
    for bank, messages in stored.items():
        for message in sorted(messages, key=lambda m: m["ts"]):
            times = [ingested[bank, p] for p in message["payments"] if (bank, p) in ingested]
            if (bank, message["case_id"]) not in seen and times:
                latency[bank].append(message["ts"] - max(times))
            seen.add((bank, message["case_id"]))
    for bank in sorted(latency):
        print(f"detection latency, bank {bank}, seconds: {spread(latency[bank])}")
    print(f"detection latency, all banks, seconds: {spread(list(chain(*latency.values())))}")
    for state in sorted(searches):
        print(f"search, {state}, milliseconds: {spread(searches[state])}")

    # The longest pause in each bank's stream of accepted batches and of stored cases.
    for name, times in (
        ("accepted batches", accepted),
        ("stored cases", {b: [m["ts"] for m in ms] for b, ms in stored.items()}),
    ):
        for bank in sorted(times):
            ts = sorted(times[bank])
            gap, at = max(((b - a, a) for a, b in pairwise(ts)), default=(0, 0))
            print(f"longest gap in {name}, bank {bank}: {gap:.1f} s (from ts {at:.0f})")


if __name__ == "__main__":
    main()
