"""Draft case summaries from a fixed template: no model, keys or network.

`handle` is the whole service, JSON in and JSON out; `summary.app` serves it over HTTP
and as a Lambda. The input is bank-local case evidence, including this bank's own
legs, so an LLM here would mean revisiting decision 0001. Contract: docs/contracts/case.md.
"""

from datetime import UTC, datetime


def when(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, UTC).strftime("%d %b %Y, %H:%M UTC")


def listed(items) -> str:
    """Join names: A; A and B; A, B and C."""
    items = sorted(set(items))
    return " and ".join(filter(None, [", ".join(items[:-1]), items[-1]]))


def other(pair: str, bank: str) -> str:
    return next((b for b in pair.split("-") if b != bank), "")


def step(link: dict) -> str:
    """One link in money-flow order, as a sentence about the bank that confirmed it."""
    signer = link["signer"]
    start, end = other(link["in_pair"], signer), other(link["out_pair"], signer)
    if not start:
        return f"Bank {signer} sent the first transfer, to bank {end}."
    if not end:
        return f"Bank {signer} received the last transfer, from bank {start}."
    return (
        f"Bank {signer} received a transfer from bank {start}, and its rule linked it"
        f" to one sent on to bank {end}."
    )


def payment(leg: dict, bank: str) -> str:
    peer = other(leg["pair"], bank)
    amount = f"SGD {leg['amount']:,.2f}"
    if leg["direction"] == "in":
        source = f"from bank {peer}" if peer else "from another account at this bank"
        return f"{leg['payment_id']}: {amount} received into {leg['account']} {source}."
    target = f"to bank {peer}" if peer else "to another account at this bank"
    return f"{leg['payment_id']}: {amount} sent from {leg['account']} {target}."


def headline(case: dict) -> str:
    """What the case is and why it was raised, in one sentence."""
    bank, links = case["bank"], case["links"]
    backed = [
        link["signer"] for link in links if link["in_token"] and link["out_token"]
    ]
    pending = [m["detail"] for m in case["messages"] if m["kind"] == "pending"]
    hits = [
        m["detail"].replace("_", " ") for m in case["messages"] if m["kind"] == "hit"
    ]
    if case["tier"] == 2:
        banks = [b for link in links for p in ("in_pair", "out_pair") for b in link[p].split("-") if b]  # fmt: skip
        transfers = {(link[p], link[t]) for link in links for p, t in (("in_pair", "in_token"), ("out_pair", "out_token")) if link[t]}  # fmt: skip
        return (
            f"Alert: the rules at banks {listed(backed)} each linked these payments,"
            f" forming one chain of {len(transfers)} transfers across banks {listed(banks)}."
        )
    if links:
        return (
            f"Low priority: only bank {listed(backed or [bank])}'s rule links these"
            " payments. The other banks on the chain hold the transfers, but their rules"
            " didn't fire."
        )
    if pending:
        return (
            f"Low priority: bank {bank}'s rule flagged these payments, and the search is"
            f" still waiting for bank {listed(pending)} to reply."
        )
    if hits:
        return (
            f"Low priority: bank {bank}'s {listed(hits)} rule flagged these payments."
        )
    return (
        f"Low priority: bank {bank}'s rule linked these payments between its own"
        " accounts; no other bank is involved."
    )


STATES = {
    "confirmed": "Every bank on the chain confirmed its link.",
    "partial": (
        "The chain is partial: the search stopped at its hop limit or a bank didn't"
        " confirm, so it may continue beyond what's shown."
    ),
    "pending": "No other bank has confirmed anything yet.",
}


def handle(case: dict) -> dict:
    """One case's evidence -> {"source": "template", "draft": text}."""
    bank = case["bank"]
    lines = [f"Case {case['case_id'][:16]} at bank {bank}", "", headline(case)]
    if case["state"] in STATES:
        lines.append(STATES[case["state"]])
    if case["raised_at"]:
        lines.append(f"Alert raised {when(case['raised_at'])}.")
    if case["merged_into"]:
        lines.append(f"This case was merged into case {case['merged_into'][:16]}.")
    if case["legs"]:
        lines += ["", f"Bank {bank}'s payments:"]
        lines += [f"- {payment(leg, bank)}" for leg in case["legs"]]
    if case["links"]:
        lines += ["", "How the money moved:"]
        lines += [f"{i}. {step(link)}" for i, link in enumerate(case["links"], 1)]
    lines += [
        "",
        (
            "Keep in mind: a rule flags suspicion, not proven laundering, and a"
            " bank's signature shows it made a claim, not that its records are right."
        ),
    ]
    return {"source": "template", "draft": "\n".join(lines)}
