from pathlib import Path

import pandas as pd
from prepare import FORWARD, HOP_LIMIT, plant, read_transfers, split

TINY = Path(__file__).parent / "fixtures/tiny"


def test_fixture_split():
    t = read_transfers(TINY / "transfers.csv")
    banks, labels = split(t)
    ev = pd.concat(banks.values())
    # every transfer: one `out` at the sender's bank, one `in` at the receiver's
    assert len(ev) == 2 * len(t)
    assert (ev.groupby(["payment_id", "direction"]).size() == 1).all()
    m = ev.merge(t, on="payment_id")
    assert (m.bank == m.from_bank)[m.direction == "out"].all()
    assert (m.bank == m.to_bank)[m.direction == "in"].all()
    # labels stay out of bank files; only labelled transfers are listed
    assert not {"chain_id", "scenario", "altman_laundering"} & set(ev.columns)
    assert set(labels.payment_id) == set(t.payment_id) - {"p01", "p02", "p03", "p04"}
    # intra-bank transfer: `out` before `in`
    assert list(banks["B"][banks["B"].payment_id == "p02"].direction) == ["out", "in"]
    # committed fixture outputs are up to date (regenerate with prepare.py, see docstring)
    for b, e in banks.items():
        committed = pd.read_csv(TINY / f"bank_{b}.csv", dtype=str)
        assert committed.equals(e.astype(str).reset_index(drop=True))


def test_plant():
    accounts = [(b, f"{b}-{i}") for b in "ABC" for i in range(50)]
    bg = pd.DataFrame(
        [
            [f"bg{i}", pd.Timestamp("2022-09-05") + pd.Timedelta(minutes=i), *accounts[i % 150], *accounts[(i * 7 + 1) % 150], 100.0, "ACH", 0]
            for i in range(360)
        ],
        columns=["payment_id", "time", "from_bank", "from_account", "to_bank", "to_account", "amount", "payment_format", "altman_laundering"],
    )  # fmt: skip
    t = plant(bg, 5, seed=7)
    assert t.equals(plant(bg, 5, seed=7))  # reproducible
    chains = t[t.chain_id.notna()].sort_values(["chain_id", "hop"])
    assert (chains.from_bank != chains.to_bank).all()  # every hop crosses banks
    assert chains.time.max() <= bg.time.max()  # chains end inside the slice
    for (scenario, _), c in chains.groupby(["scenario", "chain_id"]):
        hops = list(c.hop)
        ratio = (c.amount.shift(-1) / c.amount).dropna()
        if scenario == "absent_link":
            assert hops == [0, 2]
            continue
        assert hops == list(range(len(hops)))
        assert (c.to_account.iloc[:-1].values == c.from_account.iloc[1:].values).all()
        if scenario == "lookalike":
            assert (ratio < FORWARD[0]).all()
        else:
            assert ratio.between(*FORWARD).all()
        if scenario == "loop":
            assert c.to_account.iloc[-1] == c.from_account.iloc[0]
        if scenario == "over_limit":
            assert len(hops) > HOP_LIMIT
        if scenario == "mule_chain":
            assert 2 <= len(hops) <= HOP_LIMIT
