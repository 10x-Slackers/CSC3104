"""Scoring cases against the labels."""

from conftest import TINY

from mulechain.evaluate import Case, Transfer, load, run, score


def test_the_tiny_workloads_report():
    cases, searches = run(TINY)
    assert searches == {"confirmed": 3, "partial": 5}
    report = score(cases, *load(TINY), "dev")
    caught = {"attempts": 1, "caught_any_tier": 1, "caught_tier2": 0}
    assert report["patterns"] == {
        "FAN-OUT": caught | {"attempts_in_scope": 1, "caught_in_scope": 1}
    }
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


def test_laundering_counts_in_the_split_its_attempt_started_in():
    def out(day, fmt="GIRO", to="B"):
        return Transfer(f"2022-09-{day:02}", "A", to, fmt)

    transfers = {"a1": out(5), "a2": out(6), "a3": out(11), "s1": out(2, to="A")}
    transfers |= {"n1": out(6, "Cash"), "o1": out(12)}
    row = {"altman_laundering": "1", "attempt_id": "7", "altman_pattern": "CYCLE"}
    labels = {"a1": row, "a2": row, "a3": row, "n1": row | {"attempt_id": ""}}
    labels["s1"] = row | {"attempt_id": "8", "altman_pattern": "STACK"}
    dev, later = (score([Case("B", 2, {"a2"})], transfers, labels, s) for s in ("dev", "eval"))
    # The attempt starts on 5 Sep, so all of it counts in dev, wherever its transfers fall.
    assert dev["patterns"]["CYCLE"] == {
        "attempts": 1,
        "caught_any_tier": 1,
        "caught_tier2": 1,
        "attempts_in_scope": 1,
        "caught_in_scope": 1,
    }
    assert dev["patterns"]["STACK"]["attempts_in_scope"] == 0  # same bank: out of reach
    assert dev["laundering_by_scope"] == {
        "after_ordinary_activity": 1,
        "in_scope": 2,
        "same_bank": 1,
    }
    assert dev["payment_recall"] == 1 / 2
    assert later["patterns"] == {} and later["laundering_by_scope"] == {"Cash": 1}
    assert later["cases_per_bank_per_day"] == {"B tier 2": 1}  # by the case's last payment
