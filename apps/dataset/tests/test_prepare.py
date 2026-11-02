from pathlib import Path

import pandas as pd

from dataset import prepare
from dataset.prepare import pid, read_transfers, split

TINY = Path(__file__).parents[1] / "fixtures/tiny"


def test_fixture_split():
    t = read_transfers(TINY / "transfers.csv")
    banks, labels = split(t)
    ev = pd.concat(banks.values())
    # every transfer: one `out` at the sender's bank, one `in` at the receiver's
    assert len(ev) == 2 * len(t)
    assert (ev.groupby(["payment_id", "direction"]).size() == 1).all()
    m = ev.merge(t, on="payment_id")
    out, inc = m.direction == "out", m.direction == "in"
    assert (m.bank == m.from_bank)[out].all() and (m.bank == m.to_bank)[inc].all()
    assert (m.counterparty_account == m.to_account)[out].all()
    assert (m.counterparty_account == m.from_account)[inc].all()
    # labels stay out of bank files; only labelled transfers are listed
    assert not set(prepare.LABEL_COLS[1:]) & set(ev.columns)
    assert set(labels.payment_id) == set(t.payment_id) - {"p01", "p02", "p03", "p04"}
    # intra-bank transfer: `out` before `in`
    assert list(banks["B"][banks["B"].payment_id == "p02"].direction) == ["out", "in"]
    # committed fixture outputs are up to date (regenerate with prepare.py, see docstring)
    for b, e in banks.items():
        csv = e.to_csv(index=False, date_format=prepare.TIME)
        assert csv == (TINY / f"bank_{b}.csv").read_text()
    assert labels.to_csv(index=False) == (TINY / "labels.csv").read_text()


def test_altman(tmp_path, monkeypatch):
    (tmp_path / "HI-Small_Trans.csv").write_text(
        "Timestamp,From Bank,Account,To Bank,Account,Amount Received,Receiving Currency,Amount Paid,Payment Currency,Payment Format,Is Laundering\n"
        "2022/09/05 09:00,010,8000A,0011,8000B,100,US Dollar,100,US Dollar,ACH,0\n"
        "2022/09/05 09:01,010,8000A,0010,8000A,100,US Dollar,100,US Dollar,ACH,0\n"  # self-transfer
        "2022/09/05 09:02,010,8000A,0011,8000B,0.001,Euro,0.001,Euro,Cheque,0\n"
        "2022/09/05 10:00,010,8000A,0011,8000B,100,US Dollar,100,US Dollar,ACH,0\n"  # slice end
        "2022/09/05 09:03,011,8000A,010,8000A,50,US Dollar,50,US Dollar,Wire,1\n"
        "2022/09/05 09:04,011,8000C,010,8000D,110,US Dollar,100,Euro,Wire,1\n"  # 1 EUR = 1.1 USD
    )  # fmt: skip
    (tmp_path / "HI-Small_Patterns.txt").write_text(
        "BEGIN LAUNDERING ATTEMPT - CYCLE:  Max 2 hops\n"
        "2022/09/01 00:00,001,800X,002,800Y,5,US Dollar,5,US Dollar,ACH,1\n"  # not in the slice
        "END LAUNDERING ATTEMPT - CYCLE\n\n"
        "BEGIN LAUNDERING ATTEMPT - FAN-OUT:  Max 2-degree Fan-Out\n"
        "2022/09/05 09:03,0011,8000A,0010,8000A,50.00,US Dollar,50.00,US Dollar,Wire,1\n"
        "END LAUNDERING ATTEMPT - FAN-OUT\n"
    )  # fmt: skip
    monkeypatch.setattr(
        prepare.kagglehub, "dataset_download", lambda *a, path: tmp_path / path
    )
    t = prepare.altman("HI-Small", "2022-09-05 09:00", 1)
    assert list(t.payment_id) == [pid(f"altman:{i}") for i in (0, 2, 4, 5)]
    assert list(t.from_bank + t.from_account) == ["B10-8000A", "B10-8000A", "C11-8000A", "C11-8000C"]  # fmt: skip
    assert list(t.to_bank + t.to_account) == ["C11-8000B", "C11-8000B", "B10-8000A", "B10-8000D"]  # fmt: skip
    # SGD via USD: 100 USD -> 141.36; 100 EUR -> 110 USD -> 155.50; at least 0.01
    assert list(t.amount) == [141.36, 0.01, 70.68, 155.5]
    assert list(t.payment_format) == ["GIRO", "Cheque", "MEPS+", "MEPS+"]
    assert list(t.altman_laundering) == [0, 0, 1, 1]
    # the second attempt in the file; the last laundering transfer is in no attempt
    assert t.attempt_id.tolist() == [pd.NA, pd.NA, 1, pd.NA]
    assert t.altman_pattern.fillna("").tolist() == ["", "", "FAN-OUT", ""]
    assert t.time.dtype.kind == "M"  # datetimes, not categories
    assert len(prepare.altman("HI-Small")) == 5  # whole file, self-transfer dropped
