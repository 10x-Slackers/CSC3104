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
from functools import cache
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

Handle = Callable[[bytes | str], tuple[int, dict]]


@cache
def credentials():
    import botocore.session  # only needed on AWS

    found = botocore.session.get_session().get_credentials()
    if found is None:  # an error isn't cached, so the next call looks again
        raise RuntimeError("no AWS credentials")
    return found


def request_headers(url: str, body: bytes) -> dict:
    """Headers for a JSON POST, signed (SigV4) if `url` is a Lambda function URL.

    The signature is made with whatever AWS role the process runs as, and is valid for
    five minutes, so callers ask again for every request. The credentials are looked up
    once; botocore renews them itself.
    """
    headers = {"Content-Type": "application/json"}
    host = urlsplit(url).hostname or ""
    if host.endswith(".on.aws"):  # <id>.lambda-url.<region>.on.aws
        from botocore.auth import SigV4Auth  # botocore is only needed on AWS
        from botocore.awsrequest import AWSRequest

        signed = AWSRequest("POST", url, data=body, headers=headers)
        SigV4Auth(credentials(), "lambda", host.split(".")[2]).add_auth(signed)
        headers = dict(signed.headers)
    return headers


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
        disable_nagle_algorithm = True  # or each reply waits ~40 ms for the client's ACK

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
