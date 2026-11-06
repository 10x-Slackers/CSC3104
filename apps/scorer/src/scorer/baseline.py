"""Run locally: uv run python -m scorer.baseline <workload> B > detections.jsonl"""

import argparse
import csv
import json
from pathlib import Path

from scorer.score import Scorer


def thresholds(parser):
    parser.add_argument("bank", choices="ABC")
    parser.add_argument("--min-ratio", default="0.90")
    parser.add_argument("--max-ratio", default="0.98")
    parser.add_argument(
        "--min-delay", type=float, default=60, help="seconds, inclusive"
    )
    parser.add_argument("--window", type=float, default=1800, help="seconds, inclusive")


def make_scorer(args):
    return Scorer(
        args.bank,
        args.min_ratio,
        args.max_ratio,
        args.min_delay,
        args.window,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("workload", type=Path)
    thresholds(parser)
    args = parser.parse_args()
    scorer = make_scorer(args)
    with (args.workload / f"bank_{args.bank}.csv").open() as source:
        for event in csv.DictReader(source):
            for result in scorer.process(event):
                print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
