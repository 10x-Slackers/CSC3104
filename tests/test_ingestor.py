"""The boundary: what replay sends is what the ingestor accepts, and nothing else is."""

import copy
import json

import pytest
from conftest import TINY

from mulechain import BANK_TRANSFERS, BANKS, ingestor, read_events
from mulechain.replay import message, pacs008, transfers
from mulechain.serverless import as_lambda


def batch(*events):
    return message("m1", [pacs008(e) for e in events])


def order(event):
    return event["event_time"], event["payment_id"], event["direction"]


def test_replay_and_the_ingestor_rebuild_each_banks_events():
    for bank in BANKS:
        sent = [t for t in transfers(TINY) if bank in (t["bank"], t["counterparty_bank"])]
        made = ingestor.events(batch(*sent), bank)
        mine = list(read_events(TINY / f"bank_{bank}.csv"))
        assert sorted(made, key=order) == sorted(mine, key=order)
        assert mine and all(e["payment_format"] in BANK_TRANSFERS for e in mine)  # no cheques


def edit(path, value):
    def apply(message):
        target = message["FIToFICstmrCdtTrf"]["CdtTrfTxInf"][0]
        *parents, last = path
        for step in parents:
            target = target[step]
        target[last] = value

    return apply


@pytest.mark.parametrize(
    "change",
    [
        edit(["IntrBkSttlmAmt", "Amt"], "-5.00"),
        edit(["IntrBkSttlmAmt", "Amt"], "nan"),
        edit(["IntrBkSttlmAmt", "Ccy"], "USD"),
        edit(["PmtTpInf", "LclInstrm", "Prtry"], "Cheque"),
        edit(["AccptncDtTm"], "2022-09-05T09:00:00"),
        edit(["DbtrAgt", "FinInstnId", "BICFI"], "UNKNOWN"),
        edit(["PmtId", "EndToEndId"], ""),
        lambda message: message["FIToFICstmrCdtTrf"]["GrpHdr"].update(NbOfTxs="3"),
        lambda message: message["FIToFICstmrCdtTrf"]["CdtTrfTxInf"][0].pop("CdtrAcct"),
    ],
)
def test_one_bad_transfer_rejects_the_whole_batch(change):
    first, second, *_ = (t for t in transfers(TINY) if t["bank"] == "B")
    bad = batch(first, second)
    assert len(ingestor.events(copy.deepcopy(bad), "B")) >= 2
    change(bad)
    with pytest.raises((KeyError, TypeError, ValueError)):
        ingestor.events(bad, "B")


def test_a_transfer_between_two_other_banks_is_rejected():
    foreign = next(t for t in transfers(TINY) if "B" not in (t["bank"], t["counterparty_bank"]))
    with pytest.raises(ValueError, match="neither side"):
        ingestor.events(batch(foreign), "B")


class Kafka:
    """A producer that delivers, or fails, every event when flushed."""

    def __init__(self, error=None) -> None:
        self.error, self.sent, self.waiting = error, [], []

    def produce(self, topic, key, value, headers, timestamp, on_delivery) -> None:
        self.sent.append((topic, key, json.loads(value)))
        self.waiting.append(on_delivery)

    def flush(self) -> None:
        for on_delivery in self.waiting:
            on_delivery(self.error, None)


def test_a_batch_is_acknowledged_only_once_kafka_has_it(monkeypatch):
    monkeypatch.setattr(ingestor, "BANK", "B")
    mine = [t for t in transfers(TINY) if "B" in (t["bank"], t["counterparty_bank"])]
    body = json.dumps(batch(*mine))

    kafka = Kafka()
    monkeypatch.setattr(ingestor, "producer", lambda: kafka)
    status, reply = ingestor.handle(body)
    assert (status, reply) == (200, {"events": len(kafka.sent)})
    assert all(topic == "bank-B-transactions" for topic, _, _ in kafka.sent)
    assert all(key == event["account"] for _, key, event in kafka.sent)  # partition by account

    broken = Kafka(error="broker down")
    monkeypatch.setattr(ingestor, "producer", lambda: broken)
    assert ingestor.handle(body)[0] == 503  # replay sends the batch again
    assert ingestor.handle("{}")[0] == 400

    as_function = as_lambda(ingestor.handle)({"body": "{}"}, None)  # the same handler on AWS
    assert as_function["statusCode"] == 400
