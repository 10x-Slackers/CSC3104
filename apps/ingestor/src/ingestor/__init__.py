"""Each bank's ingestor: pacs.008 batches in, Kafka transaction events out.

`records()` is the whole mapping and every check; `ingestor.app` produces the
records and serves HTTP and Lambda. Contract: docs/contracts/data.md (Ingestor)
and docs/contracts/streaming.md (Kafka events).
"""

import csv
import math
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import Literal, TypedDict, cast

# Fictional BICs. Replay imports this table from here, not the ingestor from dataset,
# so the ingestor's Lambda image doesn't pull in pandas and kagglehub.
BICS = {"A": "TSTASGSG", "B": "TSTBSGSG", "C": "TSTCSGSG"}
BANKS = {bic: bank for bank, bic in BICS.items()}

Direction = Literal["in", "out"]


class Event(TypedDict):
    """A Kafka record value (docs/contracts/streaming.md)."""

    payment_id: str
    event_time: str
    bank: str
    direction: Direction
    account: str
    counterparty_account: str
    counterparty_bank: str
    amount: float
    payment_format: str


def read_events(path: str | Path) -> Iterator[Event]:
    """A bank file's rows as Kafka events: the amount becomes a number."""
    with Path(path).open() as source:
        for row in csv.DictReader(source):
            yield cast(Event, row | {"amount": float(row["amount"])})


class Record(TypedDict):
    topic: str
    key: str
    headers: dict[str, str]
    value: Event


def records(batch: dict, bank: str) -> list[Record]:
    """This bank's Kafka records for one batch, as {topic, key, headers, value}.

    Any KeyError, TypeError or ValueError rejects the whole batch (HTTP 400). These
    are the scorer's validate() checks, moved here: every field present and a
    nonempty string, a known bank on both sides, a positive SGD amount, UTC time,
    and a GIRO or MEPS+ bank transfer (nothing else is replayed or ingested).
    An intra-bank transfer gives two events, `out` then `in`.
    """
    msg = batch["FIToFICstmrCdtTrf"]
    txs = msg["CdtTrfTxInf"]
    if int(msg["GrpHdr"]["NbOfTxs"]) != len(txs):
        raise ValueError("NbOfTxs doesn't match the transaction count")
    out: list[Record] = []
    for tx in txs:
        amount = float(tx["IntrBkSttlmAmt"]["Amt"])
        if tx["IntrBkSttlmAmt"]["Ccy"] != "SGD" or not 0 < amount < math.inf:
            raise ValueError("amount must be a positive SGD amount")
        if tx["PmtTpInf"]["LclInstrm"]["Prtry"] not in ("GIRO", "MEPS+"):
            raise ValueError("only GIRO and MEPS+ bank transfers are ingested")
        debtor = BANKS[tx["DbtrAgt"]["FinInstnId"]["BICFI"]]
        creditor = BANKS[tx["CdtrAgt"]["FinInstnId"]["BICFI"]]
        if bank not in (debtor, creditor):
            raise ValueError("neither agent is this bank")
        dbtr, cdtr = (
            tx["DbtrAcct"]["Id"]["Othr"]["Id"],
            tx["CdtrAcct"]["Id"]["Othr"]["Id"],
        )
        sides: dict[Direction, tuple[str, str, str, str]] = {
            "out": (debtor, dbtr, creditor, cdtr),
            "in": (creditor, cdtr, debtor, dbtr),
        }
        for direction, (me, account, other, other_account) in sides.items():
            if me != bank:
                continue
            event = Event(
                payment_id=tx["PmtId"]["EndToEndId"],
                event_time=tx["AccptncDtTm"],
                bank=bank,
                direction=direction,
                account=account,
                counterparty_account=other_account,
                counterparty_bank=other,
                amount=amount,
                payment_format=tx["PmtTpInf"]["LclInstrm"]["Prtry"],
            )
            if not all(
                isinstance(v, str) and v for k, v in event.items() if k != "amount"
            ):
                raise ValueError("fields must be nonempty strings")
            if not event["event_time"].endswith("Z"):
                raise ValueError("AccptncDtTm must be UTC (Z)")
            datetime.fromisoformat(event["event_time"])  # ValueError if malformed
            out.append(
                {
                    "topic": f"bank-{bank}-transactions",
                    "key": account,
                    # CloudEvents, Kafka binary mode; the direction keeps intra-bank IDs unique
                    "headers": {
                        "ce_specversion": "1.0",
                        "ce_id": f"{event['payment_id']}-{direction}",
                        "ce_source": f"/bank/{bank}/ingestor",
                        "ce_type": f"payment.{direction}",
                        "ce_time": event["event_time"],
                        "content-type": "application/json",
                    },
                    "value": event,
                }
            )
    return out
