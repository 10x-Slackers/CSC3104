from pathlib import Path

from evaluation.run import Case, run
from evaluation.score import load, score

TINY = Path(__file__).parents[2] / "dataset/fixtures/tiny"


def test_fixture_report():
    cases, _ = run(TINY)
    report = score(cases, *load(TINY), "dev")
    assert report["patterns"] == {
        "FAN-OUT": {"attempts": 1, "caught_any_tier": 1, "caught_tier2": 0}
    }
    assert report["laundering_by_scope"] == {"in_scope": 1}
    assert report["payment_recall"] == 1
    assert report["false_positive_rate"] == 16 / 20
    assert report["cases_per_bank_per_day"] == {
        "A tier 1": 3,
        "A tier 2": 2,
        "B tier 1": 3,
        "B tier 2": 2,
        "C tier 1": 2,
        "C tier 2": 2,
    }


def test_split_by_attempt_start():
    def out(day, fmt="GIRO", to="B"):
        event = {"event_time": f"2022-09-{day:02}T09:00:00Z", "bank": "A"}
        return event | {"counterparty_bank": to, "payment_format": fmt}

    transfers = {"a1": out(5), "a2": out(6), "n1": out(6, "Cash"), "o1": out(12)}
    row = {"altman_laundering": "1", "attempt_id": "7", "altman_pattern": "CYCLE"}
    labels = {"a1": row, "a2": row, "n1": row | {"attempt_id": ""}}
    cases = [Case("B", 2, {"a2"})]
    dev, eval_ = (score(cases, transfers, labels, s) for s in ("dev", "eval"))
    # The attempt starts on 5 Sep, so both its transfers count in dev only.
    assert dev["patterns"] == {
        "CYCLE": {"attempts": 1, "caught_any_tier": 1, "caught_tier2": 1}
    }
    assert dev["payment_recall"] == 1 / 2
    assert eval_["patterns"] == {}
    assert eval_["laundering_by_scope"] == {"Cash": 1}
    # The case's last payment is on 6 Sep, so the load counts in eval.
    assert eval_["cases_per_bank_per_day"] == {"B tier 2": 1}
