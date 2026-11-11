"""Draft case summaries from a fixed template: no model, keys or network.

`handle` is the whole service, JSON in and JSON out; `summary.app` serves it over HTTP
and as a Lambda. The input is bank-local case evidence, including this bank's own
legs, so an LLM here would mean revisiting decision 0001. Contract: docs/contracts/case.md.
"""

from datetime import UTC, datetime


def when(timestamp: float | None, none: str = "-") -> str:
    if timestamp is None:
        return none
    return datetime.fromtimestamp(timestamp, UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


def ends(link: dict) -> tuple[str, str]:
    """Banks at the far end of the incoming and outgoing transfers; start/end if none."""

    def other(pair, none):
        return (
            next((b for b in pair.split("-") if b != link["signer"]), none)
            if pair
            else none
        )

    return other(link["in_pair"], "start"), other(link["out_pair"], "end")


def handle(case: dict) -> dict:
    """One case's evidence -> {"source": "template", "draft": text}."""
    links, legs = case["links"], case["legs"]
    tier = {2: "alert (two banks' rules link it)", 1: "low priority (local suspicion)"}
    lines = [
        "DRAFT: template summary, not AI-generated. For analyst review.",
        "",
        f"Case: {case['case_id']}",
        f"Tier: {tier.get(case['tier'], 'none')}",
        f"State: {case['state']}",
        f"Alert raised: {when(case['raised_at'], 'not raised')}",
    ]
    if case["merged_into"]:
        lines.append(f"Merged into case: {case['merged_into']}")
    lines += ["", f"This bank's own legs ({len(legs)}):"]
    lines += [
        f"  {leg['payment_id']} {leg['direction']}: account {leg['account']},"
        f" SGD {leg['amount']:,.2f}, {leg['pair'] or 'intra-bank'}"
        for leg in legs
    ]
    if links:
        signers = sorted({link["signer"] for link in links})
        lines += [
            "",
            f"{len(links)} signed link claim(s), from {', '.join(signers)}.",
            "Links in money-flow order (from bank -> confirming bank -> to bank):",
        ]
        for i, link in enumerate(links, 1):
            start, end = ends(link)
            lines.append(
                f"  {i}. {start} -> {link['signer']} -> {end}, confirmed by"
                f" {link['signer']}: {link['in_token'] or '-'} -> {link['out_token'] or '-'}"
            )
    lines += ["", "Received from the privacy node:"]
    lines += [
        f"  {m['kind']}{' ' + m['state'] if m['state'] != m['kind'] else ''}"
        f"{' (' + m['detail'] + ')' if m['detail'] else ''}"
        for m in case["messages"]
    ]
    lines += [
        "",
        (
            "Limits: a rule flags suspicion, not proven laundering. A signature"
            " proves which bank made a claim, not that its records are true."
        ),
    ]
    if case["state"] != "confirmed":
        lines.append(
            f"This case is {case['state']}: links may be missing or unconfirmed."
        )
    return {"source": "template", "draft": "\n".join(lines)}
