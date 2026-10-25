# Case handling contract

Owner: Jia Rui · Partners: Zane (result delivery), Cindy, Isaac (alert counts), Caleb (report inputs) · Code: `apps/case/` · Status: **draft**

Scope: the bank-local case schema, canonical chain identity, durable alert uniqueness, the analyst UI, mock intake and the AI-assisted report.

## Cases and alerts

| Item | Decision |
| --- | --- |
| Case schema | **draft**. `case_store.py`: SQLite (`CaseStore` interface, Postgres later) with tables `cases`, `transfers`, `claims`, `alerts`. Only tokens, bank IDs and claim metadata; no accounts, amounts or payment IDs |
| Claim fields | **draft**, until [protocol.md](protocol.md) defines them: `signer`, `in_bank`, `in_token`, `out_bank`, `out_token`, `query_id`, `key_version`, `signature` |
| When a chain is complete enough to alert | **draft**. At least 2 confirmed **transfers** across at least 2 banks (`CASE_MIN_TRANSFERS`, `CASE_MIN_BANKS`). One valid claim covers two transfers, so `m1` alerts |
| Canonical chain identity across overlapping searches | **draft**. A transfer is keyed `<pair>:<token>`, e.g. `A-B:ppmc:v1:k1:…`, and belongs to one case. A result sharing any transfer joins that case; a result spanning several cases merges them into the oldest. A new case's ID is SHA-256 of its sorted transfer keys. IDs are bank-local: banks that saw different first results get different IDs |
| Unique alert key (durable, bank-local) | **draft**. `alerts.case_id` primary key, inserted with `ON CONFLICT DO NOTHING` in the same `BEGIN IMMEDIATE` transaction as the claims; a merge keeps one alert |
| Result states | **draft**, until [protocol.md](protocol.md) defines them. Ranked `invalid-claim` < `no-match` < `timed-out` < `partial` < `confirmed`. A case takes its strongest result, so a late weaker result never downgrades it. Each `(query_id, status)` is recorded once; a result without claims creates no case |
| Claim verification | **draft**. `evidence.py` verifies every claim with Role 4's `verify` before storing. It uses public keys from config keyed by `(bank, key_version)`, and the claim must carry the result's `query_id` with a signer distinct from both neighbours. A failed claim is dropped and the result is downgraded to `invalid-claim`, which never alerts. Signed bytes: compact sorted JSON `["csc3104.link-claim","v1",{fields except signature}]` |
| Partial → confirmed updates | **draft**. Only a `confirmed` case can alert. A partial case (e.g. a bank offline) is stored and shown, and alerts once a later result for any of its transfers is confirmed |
| Durable pending notifications and acks | **draft**. Transactional outbox: an `alert:<case_id>` event is written in the same transaction as the alert, and a `merged:<case_id>` event when an alerted case is merged away. `outbox.py` delivers at least once: it leases events (so concurrent dispatchers don't double-send), acks on success, retries with doubling backoff (1 s up to 60 s) for at most `CASE_NOTIFY_MAX_ATTEMPTS` (10) attempts, and stores only the error class. Receivers deduplicate on `event_id`, so retries show one alert. Messages carry `event_id`, `kind`, `case_id`, `state`, `merged_into` |

## Analyst UI

- Framework: **draft**, FastAPI with server-rendered HTML (`app.py`). Run with `CASE_BANK=A CASE_DB=cases-a.db uv run apps/case/app.py` (port `PORT`, default 8000).
- Pages: a case list (state, alert time, latest decision), the notification inbox and undelivered events. Each case page lists its links in chain order, showing the bank that confirmed each one with its signing key, both tokens (shortened), the search results and the decision history. JSON: `/api/cases`, `/api/cases/{id}`.
- Analyst decisions (all simulated): continue monitoring, suspend outgoing transactions, freeze accounts, report chain evidence to the local authorities (the mock intake). They are stored append-only, with no free text, so no account number can be typed in. Freeze and suspend touch nothing; mapping tokens back to accounts would need the token index.
- The outbox dispatcher runs in a background thread of the UI. `alert`/`merged` events go to the UI's own inbox, which deduplicates on `event_id`; `POST /notifications` accepts the same messages from elsewhere. A `report` event goes to `CASE_INTAKE_URL`, after `check_peer_payload` against the report allowlist.
- Report payload (**draft**, Caleb to review): `event_id`, `kind`, `bank`, `case_id`, `state`, `merged_into`, `raised_at`, and `links` with `signer`, `in_bank`, `in_token`, `out_bank`, `out_token`.
- The mock intake (`intake.py`, port 8001) records timestamps and is outside the detection path. It keeps the first arrival per `(bank, event_id)`. `received_at − raised_at` is the intake response time, which includes the analyst's decision time.

## AI-assisted report

**Decision: no AI report**; reports are template-only ([decisions/0001](../decisions/0001-template-only-report.md)).

- `report.py`: `report_input` copies only allow-listed case fields (**draft**, Caleb reviews them) and re-checks them with `check_peer_payload` before a `ReportClient` sees them. `TemplateClient` restates the links, confirming banks, search results, decisions and limits, and flags non-confirmed cases as possibly incomplete.
- Reports are stored with their source (`template`) as drafts. An analyst generates them from the case page and approves or rejects each one once.
- Model and hosting: none. No API keys, network calls or model dependency.
- Reports are generated on demand, outside the detection path, so a report failure can't block detection. A template report **does not count** as AI reporting.

## Open

- TODO
