import base64
import copy
import json
import threading
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError

import pytest

from ingestor import app, records

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
        lambda hdr, tx: tx["PmtTpInf"]["LclInstrm"].update(Prtry="Cheque"),
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
        "not-a-bank-transfer",
    ],
)
def test_bad_batch_is_rejected_whole(edit):
    with pytest.raises((KeyError, TypeError, ValueError)):
        records(changed(edit), "B")


def test_batch_for_another_bank_is_rejected_whole():
    # p06 involves A, but p02 and p07 name neither agent as A
    with pytest.raises(ValueError, match="neither agent"):
        records(BATCH, "A")


# The service (ingestor.app), with a fake Kafka producer.
BODY = (FIXTURES / "pacs008_batch.json").read_bytes()


class FakeProducer:
    """Keeps produce() calls; flush() delivers them, failing with `error` if set."""

    def __init__(self):
        self.sent, self.callbacks, self.error = [], [], None

    def produce(self, topic, key, value, headers, timestamp, on_delivery):
        record = {"topic": topic, "key": key, "headers": headers}
        self.sent.append(record | {"value": json.loads(value), "ts": timestamp})
        self.callbacks.append(on_delivery)

    def flush(self):
        for delivered in self.callbacks:
            delivered(self.error, None)
        return 0


@pytest.fixture
def kafka(monkeypatch):
    monkeypatch.setattr(app, "BANK", "B")
    monkeypatch.setattr(app, "producer", FakeProducer())
    return app.producer


def test_batch_acknowledged_once_kafka_has_every_event(kafka, capsys):
    assert app.handle(BODY) == (200, {"events": 4})
    # the shared sample: the scorer's tests load the same records
    events = json.loads((FIXTURES / "kafka_events.json").read_text())
    assert [{k: r[k] for k in events[0]} for r in kafka.sent] == events
    log = json.loads(capsys.readouterr().out)
    assert log["status"] == 200 and log["payment_ids"] == ["p02", "p02", "p06", "p07"]
    # one Kafka timestamp per batch, the logged ts, for the latency join
    assert {r["ts"] for r in kafka.sent} == {int(log["ts"] * 1000)}


def test_bad_batch_rejected_whole(kafka):
    batch = json.loads(BODY)
    batch["FIToFICstmrCdtTrf"]["GrpHdr"]["NbOfTxs"] = "2"
    assert app.handle(json.dumps(batch))[0] == 400
    assert app.handle(b"not json")[0] == 400
    assert kafka.sent == []


def test_undelivered_event_gives_503_so_replay_resends(kafka):
    kafka.error = "KafkaError{code=_MSG_TIMED_OUT}"
    assert app.handle(BODY) == (503, {"error": "KafkaError{code=_MSG_TIMED_OUT}"})


@pytest.mark.parametrize(
    "encoded_flag",
    [True, False, None],
    ids=["base64-body", "plain-body", "missing-flag"],
)
def test_lambda_function_url(kafka, encoded_flag):
    body = BODY.decode("utf-8")
    if encoded_flag:
        body = base64.b64encode(BODY).decode("ascii")

    event = {"body": body}
    if encoded_flag is not None:
        event["isBase64Encoded"] = encoded_flag

    reply = app.lambda_handler(event, None)
    assert reply["statusCode"] == 200 and json.loads(reply["body"]) == {"events": 4}
    events = json.loads((FIXTURES / "kafka_events.json").read_text())
    assert [{k: r[k] for k in events[0]} for r in kafka.sent] == events


def test_http_server(kafka):
    server = ThreadingHTTPServer(("127.0.0.1", 0), app.Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{server.server_port}"
    try:
        assert json.load(urllib.request.urlopen(f"{url}/healthz")) == {"ok": True}
        assert json.load(urllib.request.urlopen(url, BODY)) == {"events": 4}
        with pytest.raises(HTTPError) as rejected:
            urllib.request.urlopen(url, b"{}")
        assert rejected.value.code == 400
    finally:
        server.shutdown()
