"""Offline detection evaluation (docs/evaluation.md#offline-detection-evaluation).

uv run python -m evaluation.offline apps/dataset/out/altman-small --split dev
"""

import argparse
import json
import sys
from contextlib import redirect_stdout

from evaluation.run import run
from evaluation.score import SPLITS, load, score


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("workload", help="a prepare.py workload with labels.csv")
    parser.add_argument("--split", choices=SPLITS, default="dev")
    args = parser.parse_args()
    with redirect_stdout(sys.stderr):  # the nodes' JSON log lines, kept apart
        cases, searches = run(args.workload)
    report = score(cases, *load(args.workload), args.split)
    print(json.dumps({"split": args.split, "searches": searches} | report, indent=2))


if __name__ == "__main__":
    main()
