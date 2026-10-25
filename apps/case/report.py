"""Draft case reports for analyst review. Template only: there is no AI report.

The allow-list filter runs first, so a client only ever sees permitted case fields.
Every report is stored as a draft labelled with its source ("template").
Decision: docs/decisions/0001-template-only-report.md.
"""

from datetime import UTC, datetime
from typing import Protocol

from case_store import chain

from apps.security.security import check_peer_payload

LINK_FIELDS = ("signer", "in_bank", "in_token", "out_bank", "out_token", "key_version")
# Everything a report client may see. Signatures, accounts, amounts and payment IDs
# are not listed, so they can never reach a report.
# ASSUMPTION: report input fields until Caleb reviews them, replace when agreed.
ALLOWED = {
    "case_id", "state", "raised_at", "merged_into", "links", "results", "decisions",
    "query_id", "status", "action", "decided_at", *LINK_FIELDS,
}  # fmt: skip


def report_input(case):
    """Copy only allow-listed fields from a case, then re-check the result."""
    data = {k: case[k] for k in ("case_id", "state", "raised_at", "merged_into")}
    data["links"] = [
        {f: getattr(c, f) for f in LINK_FIELDS} for c in chain(case["claims"])
    ]
    data["results"] = [{"query_id": q, "status": s} for q, s in case["results"]]
    data["decisions"] = [{"action": a, "decided_at": t} for a, t in case["decisions"]]
    check_peer_payload(data, ALLOWED)
    return data


class ReportClient(Protocol):
    source: str  # stored with every report, e.g. "template"

    def draft(self, data: dict) -> str: ...


def when(timestamp):
    if timestamp is None:
        return "not raised"
    return datetime.fromtimestamp(timestamp, UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


class TemplateClient:
    """Fills a fixed template from the permitted fields; no model, keys or network."""

    source = "template"

    def draft(self, data):
        links = data["links"]
        banks = sorted(
            {b for link in links for b in (link["in_bank"], link["out_bank"])}
        )
        signers = sorted({link["signer"] for link in links})
        lines = [
            "DRAFT: template report, not AI-generated. Pending analyst review.",
            "",
            f"Case: {data['case_id']}",
            f"State: {data['state']}",
            f"Alert raised: {when(data['raised_at'])}",
        ]
        if data["merged_into"]:
            lines.append(f"Merged into case: {data['merged_into']}")
        lines += [
            "",
            (
                f"Summary: {len(links)} linked transfer pair(s) across banks {', '.join(banks)},"
                f" confirmed by signed claims from {', '.join(signers)}."
            ),
            "",
            "Links (incoming bank -> confirming bank -> outgoing bank):",
        ]
        lines += [
            f"  {i}. {link['in_bank']} -> {link['signer']} -> {link['out_bank']},"
            f" confirmed by {link['signer']} (key {link['key_version']}):"
            f" {link['in_token']} -> {link['out_token']}"
            for i, link in enumerate(links, 1)
        ]
        lines += ["", "Search results:"]
        lines += [f"  {r['query_id']}: {r['status']}" for r in data["results"]]
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


def draft_report(store, case_id, client):
    """Filter, draft and store as a draft; return the new report ID."""
    data = report_input(store.get(case_id))
    return store.save_report(case_id, client.source, client.draft(data))
