import json

from summary import handle
from summary.app import lambda_handler, respond

# The case service's report_input for a loop A -> B -> C -> A at bank B
# (apps/case/src/case/report.py).
CASE = {
    "bank": "B",
    "case_id": "c0ffee0123456789abcdef",
    "tier": 2,
    "state": "confirmed",
    "raised_at": 1662370000.0,
    "merged_into": None,
    "links": [
        {"signer": "A", "in_pair": "", "in_token": "", "out_pair": "A-B", "out_token": "aa"},
        {"signer": "B", "in_pair": "A-B", "in_token": "aa", "out_pair": "B-C", "out_token": "bb"},
        {"signer": "C", "in_pair": "B-C", "in_token": "bb", "out_pair": "A-C", "out_token": "cc"},
        {"signer": "A", "in_pair": "A-C", "in_token": "cc", "out_pair": "", "out_token": ""},
    ],
    "legs": [
        {"payment_id": "p10", "direction": "in", "account": "B-o1", "amount": 7000.0, "pair": "A-B"},
        {"payment_id": "p11", "direction": "out", "account": "B-o1", "amount": 6650.0, "pair": "B-C"},
    ],
    "messages": [{"kind": "chain", "detail": "B"}],
}  # fmt: skip


def draft(**changes):
    return handle(CASE | changes)["draft"]


EXPECTED = """Case c0ffee0123456789 at bank B

Alert: the rules at banks B and C each linked these payments, forming one chain of 3 transfers across banks A, B and C.
Every bank on the chain confirmed its link.
Alert raised 05 Sep 2022, 09:26 UTC.

Bank B's payments:
- p10: SGD 7,000.00 received into B-o1 from bank A.
- p11: SGD 6,650.00 sent from B-o1 to bank C.

How the money moved:
1. Bank A sent the first transfer, to bank B.
2. Bank B received a transfer from bank A, and its rule linked it to one sent on to bank C.
3. Bank C received a transfer from bank B, and its rule linked it to one sent on to bank A.
4. Bank A received the last transfer, from bank C.

Keep in mind: a rule flags suspicion, not proven laundering, and a bank's signature shows it made a claim, not that its records are right."""


def test_alert_reads_as_sentences():
    assert draft() == EXPECTED


def test_low_priority_cases_say_why():
    one_rule = draft(
        tier=1, raised_at=None, links=CASE["links"][:2] + CASE["links"][3:]
    )
    assert "Low priority: only bank B's rule links these payments along" in one_rule
    pending = [{"kind": "pending", "detail": "C"}]
    waiting = draft(tier=1, state="pending", raised_at=None, links=[], messages=pending)
    assert "the search is still waiting for bank C to reply." in waiting
    assert "No other bank has confirmed anything yet." in waiting
    hit = [{"kind": "hit", "detail": "new_payee"}]
    assert "bank B's new payee rule flagged" in draft(
        tier=1, state="hit", raised_at=None, links=[], messages=hit
    )
    local = [{"kind": "local", "detail": ""}]
    intra = [CASE["legs"][0] | {"pair": "", "direction": "out", "account": "B-bob"}]
    text = draft(
        tier=1, state="local", raised_at=None, links=[], messages=local, legs=intra
    )
    assert "between its own accounts, and no other bank is involved." in text
    assert "sent from B-bob to another account at this bank." in text


def test_tier_1_names_this_banks_hit_alongside_another_banks_chain():
    # At A: a new_payee hit on the transfer to B, then m1 (only B's rule links it).
    links = [
        {"signer": "A", "in_pair": "", "in_token": "", "out_pair": "A-B", "out_token": "aa"},
        {"signer": "B", "in_pair": "A-B", "in_token": "aa", "out_pair": "B-C", "out_token": "bb"},
    ]  # fmt: skip
    hit = [{"kind": "hit", "detail": "new_payee"}]
    text = draft(bank="A", tier=1, raised_at=None, links=links, messages=hit, legs=[])
    assert (
        "Low priority: bank A's new payee rule flagged these payments; only bank B's"
        " rule links these payments along the chain" in text
    )


def test_partial_chain_says_it_may_continue():
    assert "it may continue beyond what's shown" in draft(state="partial")


def test_http_and_lambda_give_the_same_json(capsys):
    body = json.dumps(CASE)
    status, reply = respond(body)
    event = {"body": body, "isBase64Encoded": False}
    assert (status, reply) == (200, handle(CASE))
    assert lambda_handler(event, None)["body"] == json.dumps(reply)
    assert respond("{}")[0] == 400
    _, second, bad = (json.loads(x) for x in capsys.readouterr().out.splitlines())
    assert second["cold"] is False and bad["status"] == 400 and "ts" in bad
