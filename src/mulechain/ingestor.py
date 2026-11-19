"""Ingestor: a bank's front door. pacs.008 batches in over HTTP, Kafka events out.

    BANK=B python -m mulechain.ingestor        # a container on k3d
    mulechain.ingestor.lambda_handler          # a Lambda function URL on AWS

The first link of the delivery chain (docs/patterns.md):

- Validate at the boundary: a bad batch is rejected whole (400), so every event in
  Kafka is well formed and nothing downstream checks again.
- Acknowledge only after a durable write: 200 is sent once Kafka has every event.
  On a Kafka error the reply is 503 and replay sends the same batch again.
- Idempotent producer: Kafka drops the client library's own retries' duplicates, and
  waits for all in-sync replicas (`acks=all`).
- Key = account: one account's events land in one partition, in order.
"""

import json
import math
import os
import time
from datetime import datetime
from functools import cache

from confluent_kafka import Producer

from mulechain import BANK_TRANSFERS, log
from mulechain.serverless import as_lambda, serve

BANK = os.environ.get("BANK", "A")
BICS = {"A": "TSTASGSG", "B": "TSTBSGSG", "C": "TSTCSGSG"}  # fictional
BANK_OF = {bic: bank for bank, bic in BICS.items()}


def topic(bank: str) -> str:
    return f"bank-{bank}-transactions"


def events(batch: dict, bank: str) -> list[dict]:
    """One pacs.008 batch as this bank's events: `out` if it sends, `in` if it receives.

    A transfer inside the bank gives both. Raises KeyError, TypeError or ValueError if
    anything is missing or wrong.
    """
    message = batch["FIToFICstmrCdtTrf"]
    transfers = message["CdtTrfTxInf"]
    if int(message["GrpHdr"]["NbOfTxs"]) != len(transfers):
        raise ValueError("NbOfTxs doesn't match the number of transfers")
    out = []
    for t in transfers:
        amount = float(t["IntrBkSttlmAmt"]["Amt"])
        if t["IntrBkSttlmAmt"]["Ccy"] != "SGD" or not 0 < amount < math.inf:
            raise ValueError("the amount must be positive and in SGD")
        if t["PmtTpInf"]["LclInstrm"]["Prtry"] not in BANK_TRANSFERS:
            raise ValueError("only GIRO and MEPS+ transfers are ingested")
        if not t["AccptncDtTm"].endswith("Z"):
            raise ValueError("AccptncDtTm must be UTC")
        datetime.fromisoformat(t["AccptncDtTm"])
        sender = (BANK_OF[t["DbtrAgt"]["FinInstnId"]["BICFI"]], t["DbtrAcct"]["Id"]["Othr"]["Id"])
        receiver = (BANK_OF[t["CdtrAgt"]["FinInstnId"]["BICFI"]], t["CdtrAcct"]["Id"]["Othr"]["Id"])
        if bank not in (sender[0], receiver[0]):
            raise ValueError("neither side of the transfer is this bank")
        for direction, mine, theirs in (("out", sender, receiver), ("in", receiver, sender)):
            if mine[0] != bank:
                continue
            event = {
                "payment_id": t["PmtId"]["EndToEndId"],
                "event_time": t["AccptncDtTm"],
                "bank": bank,
                "direction": direction,
                "account": mine[1],
                "counterparty_account": theirs[1],
                "counterparty_bank": theirs[0],
                "amount": amount,
                "payment_format": t["PmtTpInf"]["LclInstrm"]["Prtry"],
            }
            if not all(isinstance(v, str) and v for k, v in event.items() if k != "amount"):
                raise ValueError("every field must be a nonempty string")
            out.append(event)
    return out


def headers(event: dict) -> dict:
    """CloudEvents headers (Kafka binary mode); the direction keeps intra-bank IDs unique."""
    return {
        "ce_specversion": "1.0",
        "ce_id": f"{event['payment_id']}-{event['direction']}",
        "ce_source": f"/bank/{event['bank']}/ingestor",
        "ce_type": f"payment.{event['direction']}",
        "ce_time": event["event_time"],
        "content-type": "application/json",
    }


@cache  # one producer per process, so warm Lambda invocations reuse its connections
def producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            "enable.idempotence": True,  # also sets acks=all
            "message.timeout.ms": 25000,  # give up inside the Lambda's 30 s
        }
    )


def handle(body: bytes | str) -> tuple[int, dict]:
    """One batch -> (HTTP status, reply)."""
    ts = time.time()  # every record's Kafka timestamp: latency is measured from here
    out, errors = [], []
    try:
        out = events(json.loads(body), BANK)
    except (KeyError, TypeError, ValueError) as error:
        status, reply = 400, {"error": f"{type(error).__name__}: {error}"}
    else:
        for event in out:
            producer().produce(
                topic(BANK),
                key=event["account"],
                value=json.dumps(event),
                headers=headers(event),
                timestamp=int(ts * 1000),
                on_delivery=lambda error, _: error and errors.append(str(error)),
            )
        producer().flush()  # returns once every event is delivered or has failed
        status, reply = (503, {"error": errors[0]}) if errors else (200, {"events": len(out)})
    ids = [event["payment_id"] for event in out]
    ms = round((time.time() - ts) * 1000, 1)
    log("ingestor", bank=BANK, status=status, ms=ms, kafka_ts=ts, payment_ids=ids, **reply)
    return status, reply


lambda_handler = as_lambda(handle)

if __name__ == "__main__":
    serve(handle)
