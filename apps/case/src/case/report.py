"""Draft case reports for analyst review. Template only: there is no AI report.

The allow-list filter runs first, so a client only ever sees permitted case fields.
Every report is stored as a draft labelled with its source ("template").
"""

from datetime import UTC, datetime

from case.case_store import neighbours
from security import check_peer_payload

LINK_FIELDS = (
    "signer",
    "in_pair",
    "in_token",
    "out_pair",
    "out_token",
    "key_version",
    "signing_key_version",
    "truncated",
)
# Peer/minimal evidence and internal own-bank details have distinct boundaries.
LOCAL_FIELDS = ("bank", "direction", "payment_id", "account", "amount")
ALLOWED = {
    "case_id", "state", "raised_at", "merged_into", "links", "results", "decisions",
    "chain_digest", "sender", "action", "decided_at", *LINK_FIELDS,
}  # fmt: skip


def report_input(case, bank=None):
    """Human report JSON: hex tokens plus explicitly selected own-bank details."""
    data = {k: case[k] for k in ("case_id", "state", "raised_at", "merged_into")}
    data["links"] = [{f: getattr(c, f) for f in LINK_FIELDS} for c in case["claims"]]
    for link in data["links"]:
        for field in ("in_token", "out_token"):
            link[field] = link[field].hex()
    data["results"] = [
        {"chain_digest": d, "sender": s, "state": st} for d, s, st in case["results"]
    ]
    data["decisions"] = [{"action": a, "decided_at": t} for a, t in case["decisions"]]
    check_peer_payload(data, ALLOWED)
    if bank is not None:
        if any(leg["bank"] != bank for leg in case.get("own_legs", ())):
            raise ValueError("A report may contain only its own bank's raw legs")
        data["own_legs"] = [
            {f: leg[f] for f in LOCAL_FIELDS} for leg in case.get("own_legs", ())
        ]
    return data


def when(timestamp, none="-"):
    if timestamp is None:
        return none
    return datetime.fromtimestamp(timestamp, UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


class TemplateClient:
    """Fills a fixed template from the permitted fields; no model, keys or network."""

    source = "template"

    def draft(self, data):
        links = data["links"]
        pairs = [link[p] for link in links for p in ("in_pair", "out_pair")]
        banks = sorted({b for pair in pairs if pair for b in pair.split("-")})
        signers = sorted({link["signer"] for link in links})
        lines = [
            "DRAFT: template report, not AI-generated. Pending analyst review.",
            "",
            f"Case: {data['case_id']}",
            f"State: {data['state']}",
            f"Alert raised: {when(data['raised_at'], 'not raised')}",
        ]
        if data["merged_into"]:
            lines.append(f"Merged into case: {data['merged_into']}")
        lines += [
            "",
            (
                f"Summary: {len(links)} signed link claim(s) across banks"
                f" {', '.join(banks)}, from {', '.join(signers)}."
            ),
            "",
            "Links in money-flow order (from bank -> confirming bank -> to bank):",
        ]
        for i, link in enumerate(links, 1):
            start, end = neighbours(link["signer"], link["in_pair"], link["out_pair"])
            lines.append(
                f"  {i}. {start} -> {link['signer']} -> {end}, confirmed by"
                f" {link['signer']}: {link['in_token'] or '-'} -> {link['out_token'] or '-'}"
            )
        if data.get("own_legs"):
            lines += ["", "Own-bank details (internal):"]
            lines += [
                f"  {leg['direction']}: payment {leg['payment_id']}, account {leg['account']}, amount {leg['amount']}"
                for leg in data["own_legs"]
            ]
        lines += ["", "Chain results received:"]
        lines += [
            f"  {r['state']} from {r['sender']} (digest {r['chain_digest'][:16]})"
            for r in data["results"]
        ]
        lines += ["", "Analyst decisions so far:"]
        lines += [
            f"  {when(d['decided_at'])}: {d['action']}" for d in data["decisions"]
        ]
        if not data["decisions"]:
            lines.append("  none")
        lines += [
            "",
            (
                "Limits: a rule flags suspicion, not proven laundering. A signature proves"
                " which bank made a claim, not that its records are true."
            ),
        ]
        if data["state"] != "confirmed":
            lines.append(
                f"This case is {data['state']}: links may be missing or unconfirmed."
            )
        return "\n".join(lines)


def draft_report(store, case, client, bank=None):
    """Filter, draft and store as a draft; return the new report ID.

    A client has a `source` label (stored with the report) and `draft(data) -> str`.
    """
    data = report_input(case, bank)
    return store.save_report(case["case_id"], client.source, client.draft(data))
