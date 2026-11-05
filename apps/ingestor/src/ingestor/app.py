"""The ingestor service: one pacs.008 batch per HTTP POST in, Kafka events out.

A plain HTTP server locally and on k3d; `lambda_handler` behind a Lambda function
URL on EKS. Settings (environment): INGESTOR_BANK, KAFKA_BOOTSTRAP_SERVERS, PORT.

    INGESTOR_BANK=B uv run python -m ingestor.app
    curl -d @apps/ingestor/fixtures/pacs008_batch.json localhost:8080
"""

import base64
import json
import os
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from confluent_kafka import Producer

from ingestor import records

BANK = os.environ.get("INGESTOR_BANK", "A")
# Module level, so warm Lambda invocations reuse it. Idempotence stops librdkafka's
# retries writing duplicates and sets acks=all; every message is delivered or failed
# within message.timeout.ms (25 s), inside the Lambda's 30 s timeout.
producer = Producer(
    {
        "bootstrap.servers": os.environ.get(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        ),
        "enable.idempotence": True,
        "message.timeout.ms": 25000,
    }
)


def handle(body):
    """One batch -> (HTTP status, reply). 200 only once Kafka has every event."""
    ts = time.time()  # every record's Kafka timestamp, which latency is measured from
    try:
        out = records(json.loads(body), BANK)
    except (KeyError, TypeError, ValueError) as e:  # the whole batch, nothing produced
        out, status, reply = [], 400, {"error": f"{type(e).__name__}: {e}"}
    else:

        def delivered(err, _):
            if err:
                errors.append(str(err))

        errors = []
        for r in out:
            producer.produce(
                r["topic"],
                key=r["key"],
                value=json.dumps(r["value"]),
                headers=r["headers"],
                timestamp=int(ts * 1000),
                on_delivery=delivered,
            )
        producer.flush()  # returns within message.timeout.ms
        if errors:
            status, reply = 503, {"error": errors[0]}
        else:
            status, reply = 200, {"events": len(out)}
    # One JSON line per batch; the evaluation joins payment_id to ts.
    ids = [r["value"]["payment_id"] for r in out]
    ms = round((time.time() - ts) * 1000, 1)
    log = {"ts": ts, "service": "ingestor", "bank": BANK, "status": status, "ms": ms}
    print(json.dumps(log | reply | {"payment_ids": ids}), flush=True)
    return status, reply


def lambda_handler(event, context):
    body = event.get("body") or ""
    if event.get("isBase64Encoded", False):
        body = base64.b64decode(body).decode("utf-8")
    # Function URL, payload format 2.0: the batch is the body.
    status, reply = handle(body)
    return {
        "statusCode": status,
        "headers": {"content-type": "application/json"},
        "body": json.dumps(reply),
    }


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"  # keep-alive: replay reuses one connection

    def do_GET(self):
        # Readiness only: a Kafka outage shows as 503 on POST, not here.
        if self.path == "/healthz":
            self.reply(200, {"ok": True})
        else:
            self.reply(404, {"error": "not found"})

    def do_POST(self):
        self.reply(*handle(self.rfile.read(int(self.headers["Content-Length"]))))

    def reply(self, status, reply):
        body = json.dumps(reply).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):  # handle() logs JSON instead
        pass


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    ThreadingHTTPServer(("", port), Handler).serve_forever()
