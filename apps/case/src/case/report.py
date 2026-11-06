"""Draft case reports for analyst review. Template only: there is no AI report.

The allow-list filter runs first, so a client only ever sees permitted case fields.
Every report is stored as a draft labelled with its source ("template").
"""

from datetime import UTC, datetime
from typing import TypedDict

from case.case_store import Case, CaseState, SqliteCaseStore, neighbours
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
# Everything a report client may see. Signatures, accounts, amounts and payment IDs
# are not listed, so they can never reach a report.
ALLOWED = {
    "case_id", "state", "raised_at", "merged_into", "links", "results", "decisions",
    "chain_digest", "sender", "action", "decided_at", *LINK_FIELDS,
}  # fmt: skip


class ReportInput(TypedDict):
    case_id: str
    state: CaseState
    raised_at: float | None
    merged_into: str | None
    links: list[dict]
    results: list[dict]
    decisions: list[dict]


def report_input(case: Case) -> ReportInput:
    """Select only approved evidence fields, with tokens as readable hex."""
    data = ReportInput(
        case_id=case["case_id"],
        state=case["state"],
        raised_at=case["raised_at"],
        merged_into=case["merged_into"],
        links=[{f: getattr(c, f) for f in LINK_FIELDS} for c in case["claims"]],
        results=[
            {"chain_digest": d, "sender": s, "state": st}
            for d, s, st in case["results"]
        ],
        decisions=[{"action": a, "decided_at": t} for a, t in case["decisions"]],
    )
    check_peer_payload(data, ALLOWED)
    return data


def when(timestamp: float | None, none: str = "-") -> str:
    if timestamp is None:
        return none
    return datetime.fromtimestamp(timestamp, UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


class TemplateClient:
    """Fills a fixed template from the permitted fields; no model, keys or network."""

    source = "template"

    def draft(self, data: ReportInput) -> str:
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


def draft_report(
    store: SqliteCaseStore, case: Case, client: TemplateClient
) -> int | None:
    """Filter, draft and store as a draft; return the new report ID.

    A client has a `source` label (stored with the report) and `draft(data) -> str`.
    """
    data = report_input(case)
    return store.save_report(case["case_id"], client.source, client.draft(data))
