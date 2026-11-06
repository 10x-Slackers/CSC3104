from pathlib import Path

from evaluation.run import merge, run

TINY = Path(__file__).parents[2] / "dataset/fixtures/tiny"


def test_fixture_cases():
    cases, searches = run(TINY)
    got = {(c.bank, c.tier, " ".join(sorted(c.ids))) for c in cases}
    o1, v1 = "p10 p11 p12", "p15 p16 p17 p18 p19 p20"
    # m1 only B's rule links: Tier 1 at B. o1 and v1 (partial) alert everywhere,
    # promoting the new payee hits on their payments; the rest are new payee hits.
    assert got == {(b, 2, ids) for b in "ABC" for ids in (o1, v1)} | {
        ("A", 1, "p06"),
        ("A", 1, "p08"),
        ("A", 1, "p13"),
        ("B", 1, "p05"),
        ("B", 1, "p06 p07"),
        ("B", 1, "p09"),
        ("C", 1, "p03"),
        ("C", 1, "p14"),
    }
    assert searches == {"CONFIRMED": 3, "PARTIAL": 5}


def test_merge_joins_cases_sharing_a_payment():
    (case,) = merge("A", [(1, ["a"]), (1, ["c"]), (2, ["a", "b"]), (1, ["b", "c"])])
    assert (case.bank, case.tier, case.ids) == ("A", 2, {"a", "b", "c"})
