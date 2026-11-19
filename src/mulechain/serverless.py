"""One handler, two adapters.

The ingestor and the summary service are each a single function,
`handle(body) -> (status, reply)`. On k3d it runs behind `serve()`, a plain HTTP
server in a container. On AWS the same function runs as a Lambda behind a function
URL, through `as_lambda()`. Same JSON in and out either way.
"""

import base64
import json
import os
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

Handle = Callable[[bytes | str], tuple[int, dict]]


def as_lambda(handle: Handle) -> Callable[[dict, object], dict]:
    def lambda_handler(event: dict, context: object) -> dict:
        body = event.get("body") or ""
        if event.get("isBase64Encoded"):
            body = base64.b64decode(body)
        status, reply = handle(body)
        return {
            "statusCode": status,
            "headers": {"content-type": "application/json"},
            "body": json.dumps(reply),
        }

    return lambda_handler


def serve(handle: Handle) -> None:
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"  # keep-alive: replay reuses one connection

        def do_GET(self) -> None:  # readiness probe
            self.reply(200, {"ok": True})

        def do_POST(self) -> None:
            self.reply(*handle(self.rfile.read(int(self.headers["Content-Length"]))))

        def reply(self, status: int, reply: dict) -> None:
            body = json.dumps(reply).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args) -> None:  # the handler logs JSON lines itself
            pass

    ThreadingHTTPServer(("", int(os.environ.get("PORT", "8080"))), Handler).serve_forever()
