"""Run locally: uv run python -m scorer.baseline <workload> B > detections.jsonl"""

import argparse
import json
from pathlib import Path

from scorer.score import Scorer
from scorer.state import read_events


def thresholds(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("bank", choices="ABC")
    parser.add_argument("--min-ratio", default="0.90")
    parser.add_argument("--max-ratio", default="0.98")
    parser.add_argument(
        "--min-delay", type=float, default=60, help="seconds, inclusive"
    )
    parser.add_argument("--window", type=float, default=1800, help="seconds, inclusive")


def make_scorer(args: argparse.Namespace) -> Scorer:
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
    for event in read_events(args.workload / f"bank_{args.bank}.csv"):
        for result in scorer.process(event):
            print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
