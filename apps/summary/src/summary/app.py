"""The summary service: a plain HTTP server locally and on k3d; `lambda_handler` behind
a Lambda function URL (AuthType AWS_IAM) on EKS. The same JSON in and out either way.

    uv run python -m summary.app
    curl localhost:8080/healthz
"""

import base64
import json
import os
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from summary import handle

cold = True  # the first request in this process; Lambda cold and warm starts differ


def respond(body: str | bytes) -> tuple[int, dict]:
    """One case -> (HTTP status, reply), with one JSON log line."""
    global cold
    ts = time.time()
    case = {}
    try:
        case = json.loads(body)
        status, reply = 200, handle(case)
    except (KeyError, TypeError, ValueError, AttributeError) as e:
        status, reply = 400, {"error": f"{type(e).__name__}: {e}"}
    ms = round((time.time() - ts) * 1000, 1)
    case_id = case.get("case_id") if isinstance(case, dict) else None
    log = {"ts": ts, "service": "summary", "status": status, "ms": ms, "cold": cold}
    print(json.dumps(log | {"case_id": case_id}), flush=True)
    cold = False
    return status, reply


def lambda_handler(event: dict, context: object) -> dict:
    body = event.get("body") or ""
    if event.get("isBase64Encoded", False):
        body = base64.b64decode(body).decode("utf-8")
    status, reply = respond(body)  # function URL, payload format 2.0
    return {
        "statusCode": status,
        "headers": {"content-type": "application/json"},
        "body": json.dumps(reply),
    }


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self) -> None:
        if self.path == "/healthz":
            self.reply(200, {"ok": True})
        else:
            self.reply(404, {"error": "not found"})

    def do_POST(self) -> None:
        self.reply(*respond(self.rfile.read(int(self.headers["Content-Length"]))))

    def reply(self, status: int, reply: dict) -> None:
        body = json.dumps(reply).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):  # respond() logs JSON instead
        pass


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    ThreadingHTTPServer(("", port), Handler).serve_forever()
