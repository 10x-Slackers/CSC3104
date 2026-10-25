# 0001: Template-only case reports, no AI report

Owner: Jia Rui · Date: 2026-10-25 · Status: **agreed** (Jia Rui, 2026-10-25)

## Context

[case.md](../contracts/case.md) planned an AI-assisted draft report, with the model and hosting left TODO. An AI report would need an API key or a hosted model, would send case evidence to an outside service, and would need Caleb's review of that destination.

## Decision

- No AI report. Reports come from a fixed template (`apps/case/report.py`, `TemplateClient`) behind a `ReportClient` interface.
- An allow-list filter (`report_input`) copies only permitted case fields and re-checks them with `check_peer_payload` before any client sees them. Signatures, accounts, amounts and payment IDs are not on the list.
- Every report is stored with its source (`template`) as a **draft**, which an analyst approves or rejects in the UI.
- No API keys, no network calls, no model dependency. No AI client exists, not even as a stub.

## Consequences

- The demo and evaluation can't claim AI reporting. [case.md](../contracts/case.md) already says a template fallback doesn't count as AI reporting.
- The evaluation question "Is the analyst output grounded?" applies to template drafts only. They are grounded by construction because they only restate stored fields.
- Adding an AI client later means implementing `ReportClient`, agreeing the destination and inputs with Caleb, and replacing this decision.
