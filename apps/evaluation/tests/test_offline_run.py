from pathlib import Path

from evaluation.run import run

TINY = Path(__file__).parents[2] / "dataset/fixtures/tiny"


def test_fixture_cases():
    cases, searches = run(TINY)
    got = {(c.bank, c.tier, " ".join(sorted(c.ids))) for c in cases}
    # Each bank's cases by its own payments. o1 (p10-p12) and v1 (p15-p20, partial)
    # alert everywhere, promoting the new payee hits on their payments; m1 only B's
    # rule links: Tier 1 at B (p06 p07). The rest are new payee hits.
    assert got == {
        ("A", 2, "p10 p12"),
        ("A", 2, "p15 p17 p18 p20"),
        ("A", 1, "p06"),
        ("A", 1, "p08"),
        ("A", 1, "p13"),
        ("B", 2, "p10 p11"),
        ("B", 2, "p15 p16 p18 p19"),
        ("B", 1, "p05"),
        ("B", 1, "p06 p07"),
        ("B", 1, "p09"),
        ("C", 2, "p11 p12"),
        ("C", 2, "p16 p17 p19 p20"),
        ("C", 1, "p03"),
        ("C", 1, "p14"),
    }
    assert searches == {"CONFIRMED": 3, "PARTIAL": 5}
