import json

from summary import handle
from summary.app import lambda_handler, respond

# The case service's report_input for o1 at bank B (apps/case/src/case/report.py).
CASE = {
    "case_id": "c0ffee",
    "tier": 2,
    "state": "confirmed",
    "raised_at": 1662370000.0,
    "merged_into": None,
    "links": [
        {"signer": "A", "in_pair": "", "in_token": "", "out_pair": "A-B", "out_token": "aa"},
        {"signer": "B", "in_pair": "A-B", "in_token": "aa", "out_pair": "B-C", "out_token": "bb"},
        {"signer": "C", "in_pair": "B-C", "in_token": "bb", "out_pair": "", "out_token": ""},
    ],
    "legs": [
        {"payment_id": "p10", "direction": "in", "account": "B-m1", "amount": 9000.0, "pair": "A-B"},
    ],
    "messages": [{"key": "c0ffee", "kind": "chain", "state": "confirmed", "detail": "B"}],
}  # fmt: skip


def test_template_restates_the_case():
    draft = handle(CASE)["draft"]
    assert draft.startswith("DRAFT: template summary, not AI-generated.")
    assert "Tier: alert" in draft and "2022-09-05" in draft
    assert "p10 in: account B-m1, SGD 9,000.00, A-B" in draft
    assert "1. start -> A -> B, confirmed by A: - -> aa" in draft
    assert "2. A -> B -> C, confirmed by B: aa -> bb" in draft
    assert "chain confirmed (B)" in draft
    assert "links may be missing" not in draft


def test_partial_low_priority_case_states_uncertainty():
    draft = handle(CASE | {"tier": 1, "state": "pending", "raised_at": None})["draft"]
    assert "Tier: low priority" in draft and "Alert raised: not raised" in draft
    assert "This case is pending: links may be missing or unconfirmed." in draft


def test_http_and_lambda_give_the_same_json(capsys):
    body = json.dumps(CASE)
    status, reply = respond(body)
    event = {"body": body, "isBase64Encoded": False}
    assert (status, reply) == (200, handle(CASE))
    assert lambda_handler(event, None)["body"] == json.dumps(reply)
    assert respond("{}")[0] == 400
    first, second, bad = (json.loads(x) for x in capsys.readouterr().out.splitlines())
    assert (first["cold"], second["cold"]) == (True, False) or not first["cold"]
    assert bad["status"] == 400 and "ts" in bad
