"""Summary service: drafts a plain-English summary of one case from a fixed template.

    python -m mulechain.summary

It is its own service so that it can fail without touching detection: the case
service gives it two seconds and shows "no draft" otherwise (a bulkhead). It has no
state, no keys and no network access, which also makes it a natural Lambda: on AWS the
same `handle` runs behind a function URL (mulechain/serverless.py).

The input is one bank's own case, sent only by that bank's case service.
"""

import json
import time
from datetime import UTC, datetime

from mulechain import log
from mulechain.serverless import as_lambda, serve

STATES = {
    "confirmed": "Every bank on the chain confirmed its link.",
    "partial": "The chain is partial: a bank stopped at the hop limit or at a branch, so the"
    " money may move on beyond what is shown.",
    "pending": "No other bank has confirmed anything yet.",
}
CAVEAT = (
    "Keep in mind: a rule flags suspicion, not proven laundering, and a bank's signature"
    " shows that it made a claim, not that its records are right."
)
cold = True  # the first request in this process; Lambda cold starts are measured apart


def listed(items) -> str:
    """Join names: A; A and B; A, B and C."""
    *first, last = sorted(set(items))
    return f"{', '.join(first)} and {last}" if first else last


def step(link: dict) -> str:
    """One link in money-flow order, as a sentence about the bank that signed it."""
    signer, start, end = link["signer"], link["in_from"], link["out_to"]
    if not start:
        return f"Bank {signer} sent the first transfer, to bank {end}."
    if not end:
        return f"Bank {signer} received the last transfer, from bank {start}."
    return (
        f"Bank {signer} received a transfer from bank {start}, and its rule linked it to one"
        f" sent on to bank {end}."
    )


def payment(leg: dict) -> str:
    amount, peer = f"SGD {leg['amount']:,.2f}", leg["peer"]
    if leg["direction"] == "in":
        source = f"from bank {peer}" if peer else "from another account at this bank"
        return f"{leg['payment_id']}: {amount} received into {leg['account']} {source}."
    target = f"to bank {peer}" if peer else "to another account at this bank"
    return f"{leg['payment_id']}: {amount} sent from {leg['account']} {target}."


def headline(case: dict) -> str:
    """What the case is and why it was raised, in one sentence."""
    bank, links, messages = case["bank"], case["links"], case["messages"]
    backed = [link["signer"] for link in links if link["in_token"] and link["out_token"]]
    if case["tier"] == 2:
        banks = [link["signer"] for link in links]
        transfers = {link[side] for link in links for side in ("in_token", "out_token")} - {""}
        return (
            f"Alert: the rules at banks {listed(backed)} each linked these payments, forming"
            f" one chain of {len(transfers)} transfers across banks {listed(banks)}."
        )
    reasons = []  # every reason a Tier 1 case is listed
    hits = [m["rule"].replace("_", " ") for m in messages if m["kind"] == "hit"]
    waiting = [m["waiting"] for m in messages if m["kind"] == "pending"]
    waiting = waiting if case["state"] == "pending" else []  # a later chain answered it
    if hits:
        reasons.append(f"bank {bank}'s {listed(hits)} rule flagged these payments")
    if waiting:
        reasons.append(f"its search is still waiting for bank {listed(waiting)} to answer")
    if links:
        reasons.append(
            f"only bank {listed(backed or [bank])}'s rule links these payments along the"
            " chain: the other banks hold the transfers, but their rules didn't fire"
        )
    if any(m["kind"] == "local" for m in messages):
        reasons.append("one search ended inside this bank, where the money started or stayed")
    return f"Low priority: {'; '.join(reasons)}."


def draft(case: dict) -> str:
    lines = [f"Case {case['id'][:16]} at bank {case['bank']}", "", headline(case)]
    if case["state"] in STATES:
        lines.append(STATES[case["state"]])
    if case["alerted_at"]:
        at = datetime.fromtimestamp(case["alerted_at"], UTC)
        lines.append(f"Alert raised {at:%d %b %Y, %H:%M UTC}.")
    if case["legs"]:
        lines += ["", f"Bank {case['bank']}'s payments:"]
        lines += [f"- {payment(leg)}" for leg in case["legs"]]
    if case["links"]:
        lines += ["", "How the money moved:"]
        lines += [f"{i}. {step(link)}" for i, link in enumerate(case["links"], 1)]
    return "\n".join([*lines, "", CAVEAT])


def handle(body: bytes | str) -> tuple[int, dict]:
    """One case as JSON -> (HTTP status, {"draft": text}), with one log line."""
    global cold
    start, case_id = time.monotonic(), None
    try:
        case = json.loads(body)
        case_id = case["id"]
        status, reply = 200, {"draft": draft(case)}
    except (KeyError, TypeError, ValueError) as error:
        status, reply = 400, {"error": repr(error)}
    ms = round((time.monotonic() - start) * 1000, 1)
    log("summary", status=status, ms=ms, cold=cold, case_id=case_id)
    cold = False
    return status, reply


lambda_handler = as_lambda(handle)

if __name__ == "__main__":
    serve(handle)
