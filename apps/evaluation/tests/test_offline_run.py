from pathlib import Path

from evaluation.run import run
from ingestor import read_events

TINY = Path(__file__).parents[2] / "dataset/fixtures/tiny"


def test_fixture_cases():
    cases, searches = run(TINY)
    got = {(c.bank, c.tier, " ".join(sorted(c.ids))) for c in cases}
    o1, v1 = "p10 p11 p12", "p15 p16 p17 p18 p19 p20"
    # m1 only B's rule links: Tier 1 at B. o1 and v1 (partial) alert everywhere,
    # promoting the new payee hits on their payments; the rest are new payee hits.
    expected = {(b, 2, ids) for b in "ABC" for ids in (o1, v1)} | {
        ("A", 1, "p06"),
        ("A", 1, "p08"),
        ("A", 1, "p13"),
        ("B", 1, "p05"),
        ("B", 1, "p06 p07"),
        ("B", 1, "p09"),
        ("C", 1, "p03"),
        ("C", 1, "p14"),
    }
    # each bank's case store holds its own payments only, as on the cluster
    own = {
        b: {e["payment_id"] for e in read_events(TINY / f"bank_{b}.csv")} for b in "ABC"
    }
    assert got == {
        (b, tier, " ".join(sorted(set(ids.split()) & own[b])))
        for b, tier, ids in expected
    }
    assert searches == {"CONFIRMED": 3, "PARTIAL": 5}
