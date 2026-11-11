"""The case service's side of the summary service (apps/summary): its input and the call.

`report_input` copies only the listed case fields: this bank's own legs, and other banks'
legs as tokens. A failure or timeout means "no draft", so a summary outage never blocks
detection (a bulkhead). Signatures never reach a summary.
"""

import json
import urllib.request
from collections.abc import Callable

from case import log
from case.case_store import Case

CASE_FIELDS = ("case_id", "tier", "state", "raised_at", "merged_into")
LINK_FIELDS = ("signer", "in_pair", "in_token", "out_pair", "out_token", "key_version", "signing_key_version", "truncated")  # fmt: skip
LEG_FIELDS = ("payment_id", "direction", "account", "amount", "pair")
MESSAGE_FIELDS = ("key", "kind", "state", "detail")


def report_input(case: Case, bank: str) -> dict:
    """The summary service's JSON input: listed fields only (Caleb reviews them)."""
    return (
        {"bank": bank}
        | {f: case[f] for f in CASE_FIELDS}
        | {
            "links": [{f: getattr(c, f) for f in LINK_FIELDS} for c in case["claims"]],
            "legs": [{f: leg[f] for f in LEG_FIELDS} for leg in case["legs"]],
            "messages": [{f: m[f] for f in MESSAGE_FIELDS} for m in case["messages"]],
        }
    )


def signed(url: str, body: bytes, headers: dict, region: str) -> dict:
    """SigV4 headers for a Lambda function URL with AuthType AWS_IAM (EKS Pod Identity)."""
    from botocore.auth import SigV4Auth
    from botocore.awsrequest import AWSRequest
    from botocore.session import Session

    request = AWSRequest("POST", url, data=body, headers=headers)
    SigV4Auth(Session().get_credentials(), "lambda", region).add_auth(request)
    return dict(request.headers)


def summarizer(
    url: str, bank: str, timeout: float = 2.0, region: str = ""
) -> Callable[[Case], str | None]:
    """Call the summary service; None is "no draft". region set means sign for AWS."""

    def summarize(case: Case) -> str | None:
        if not url:
            return None
        body = json.dumps(report_input(case, bank)).encode()
        headers = {"Content-Type": "application/json"}
        try:
            if region:
                headers = signed(url, body, headers, region)
            request = urllib.request.Request(url, body, headers)
            with urllib.request.urlopen(request, timeout=timeout) as reply:
                return json.load(reply)["draft"]
        except Exception as exc:  # noqa: BLE001 - any failure means no draft
            log(event="no_draft", case_id=case["case_id"], error=type(exc).__name__)
            return None

    return summarize
