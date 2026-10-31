import copy
import json
from pathlib import Path

import pytest

from ingestor import records

# Shared interface samples: the scorer's tests load kafka_events.json too.
FIXTURES = Path(__file__).parents[1] / "fixtures"
BATCH = json.loads((FIXTURES / "pacs008_batch.json").read_text())


def test_sample_batch_gives_the_sample_events():
    # p02 is intra-bank at B (out, then in); p06 arrives from A; p07 leaves for C
    events = json.loads((FIXTURES / "kafka_events.json").read_text())
    assert records(BATCH, "B") == events


def changed(edit):
    batch = copy.deepcopy(BATCH)
    edit(
        batch["FIToFICstmrCdtTrf"]["GrpHdr"],
        batch["FIToFICstmrCdtTrf"]["CdtTrfTxInf"][1],
    )
    return batch


@pytest.mark.parametrize(
    "edit",
    [
        lambda hdr, tx: hdr.update(NbOfTxs="2"),
        lambda hdr, tx: tx["IntrBkSttlmAmt"].update(Ccy="USD"),
        lambda hdr, tx: tx["IntrBkSttlmAmt"].update(Amt="0"),
        lambda hdr, tx: tx["IntrBkSttlmAmt"].update(Amt="NaN"),
        lambda hdr, tx: tx.update(AccptncDtTm="2022-09-05T17:10:00+08:00"),
        lambda hdr, tx: tx["PmtId"].update(EndToEndId=""),
        lambda hdr, tx: tx["CdtrAgt"]["FinInstnId"].update(BICFI="TSTDSGSG"),
        lambda hdr, tx: tx.pop("PmtTpInf"),
    ],
    ids=[
        "count",
        "currency",
        "zero",
        "nan",
        "not-utc",
        "empty-id",
        "unknown-bic",
        "missing",
    ],
)
def test_bad_batch_is_rejected_whole(edit):
    with pytest.raises((KeyError, TypeError, ValueError)):
        records(changed(edit), "B")


def test_batch_for_another_bank_is_rejected_whole():
    # p06 involves A, but p02 and p07 name neither agent as A
    with pytest.raises(ValueError, match="neither agent"):
        records(BATCH, "A")
