# Case handling contract

Owner: Jia Rui · Partners: Zane (result delivery), Cindy, Isaac (alert counts), Caleb (report inputs) · Code: `apps/case/` · Status: **draft**

Scope: the bank-local case schema, canonical chain identity, durable alert uniqueness, the analyst UI, mock intake and the AI-assisted report.

## Cases and alerts

| Item | Decision |
| --- | --- |
| Case schema | **draft**. `case_store.py`: SQLite (Postgres later) with tables `cases`, `transfers`, `claims`, `results`, `alerts`, `outbox`, `decisions`, `notifications`, `reports`. Only tokens, bank IDs and claim metadata; no accounts, amounts or payment IDs |
| Input and claim fields | **agreed with Role 3's code**. Input is `ChainResult` from [`privacy_node.proto`](../../apps/privacy_node/privacy_node.proto); each `LinkClaim` is stored as `signer`, `recipient`, `query_id`, `key_version`, `in_pair`, `in_token`, `out_pair`, `out_token`, `signature` (hex). An empty ref marks the chain's start or end |
| When a chain is complete enough to alert | **draft**. A **confirmed** case with at least 2 transfers across at least 2 banks (`CASE_MIN_TRANSFERS`, `CASE_MIN_BANKS`). Partial chains are stored and shown but never alert. The minimum chain length depends on Role 2 (see Dependencies) |
| Canonical chain identity across overlapping searches | **draft**. A new case's ID is Role 3's `chain_digest`. A transfer (`<pair>\|<token>`, as `claims.key`) belongs to one case; a result sharing any transfer joins that case, and one spanning several cases merges them into the oldest. This also joins overlapping partial chains with different digests (a chain longer than 6 transfers, cut at different windows); over-limit `v1` gives five results with one digest, one case. See [decisions/case-identity.md](../decisions/case-identity.md) |
| Unique alert key (durable, bank-local) | **draft**. `alerts.case_id` primary key, inserted with `ON CONFLICT DO NOTHING` in the same `BEGIN IMMEDIATE` transaction as the claims; a merge keeps one alert |
| Result states | **agreed with Role 3's code**. Only `CONFIRMED` and `PARTIAL` results are delivered and accepted ([protocol.md](protocol.md#result-states)); anything else is rejected. A case is confirmed once any of its results is; nothing downgrades it. Each `(chain_digest, sender, state)` is recorded once |
| Claim verification | **agreed with Role 3's code**. `evidence.verify_result` mirrors the privacy node's `Deliver` check: this bank signed part of the chain, `claims.chain_digest` matches, and `claims.valid_chain` passes. Public keys come from `<CASE_PUBLIC_KEYS>/<bank>.pem` (the `bank-public-keys` ConfigMap layout). A failed result is rejected with 422 and never stored |
| Partial → confirmed updates | **draft**. A partial case (cut by the hop limit or branching cap) is stored and shown; it alerts once a confirmed result for any of its transfers arrives |
| Durable pending notifications and acks | **draft**. Alerts and merges stay in the bank: an `alert:<case_id>` inbox entry is written in the same transaction as the alert, and a `merged:<case_id>` entry when an alerted case is merged away, so each shows once with no delivery step. Reports to the intake use a transactional outbox: a `report:<case_id>:<decision>` event is written with each "report" decision, so a later report decision sends again. `outbox.py` delivers at least once: it acks on success, retries any failure with doubling backoff (1 s up to 60 s) for at most `CASE_NOTIFY_MAX_ATTEMPTS` (10) attempts, and stores only the error class. The intake deduplicates on `(bank, event_id)`. A merge moves the old case's decisions, reports and pending report events to the surviving case |

## Analyst UI

- Framework: **draft**, FastAPI with server-rendered HTML (`app.py`). Run with `CASE_BANK=A CASE_DB=cases-a.db CASE_PUBLIC_KEYS=.secrets/public uv run python -m apps.case.app` (port `PORT`, default 8000).
- Results come in on `POST /results`: the body is one serialized `ChainResult`; the reply `{"chain_digest": …}` is sent only after it is stored, like Role 3's `Ack`.
- Pages: a case list (state, alert time, latest decision), the notification inbox and undelivered events. Each case page lists its links in chain order, showing the bank that confirmed each one with its signing key, both tokens (shortened), the search results and the decision history. JSON: `/api/cases`, `/api/cases/{id}`.
- Analyst decisions (all simulated): continue monitoring, suspend outgoing transactions, freeze accounts, report chain evidence to the local authorities (the mock intake). They are stored append-only, with no free text, so no account number can be typed in. Freeze and suspend touch nothing; mapping tokens back to accounts would need the token index.
- The report dispatcher runs in a background thread of the UI and sends `report` events to `CASE_INTAKE_URL`. The payload is built from `report_input`, so it passes the same `check_peer_payload` allowlist as draft reports.
- Report payload (**draft**, Role 4 to review): `event_id`, `bank`, `case_id`, `state`, `merged_into`, `raised_at`, and `links` with `signer`, `in_pair`, `in_token`, `out_pair`, `out_token`, `key_version`.
- The mock intake (`intake.py`, port 8001) records timestamps and is outside the detection path. It keeps the first arrival per `(bank, event_id)`. `received_at − raised_at` is the intake response time, which includes the analyst's decision time.

## AI-assisted report

**Decision: no AI report**; reports are template-only ([decisions/0001](../decisions/0001-template-only-report.md)).

- `report.py`: `report_input` copies only allow-listed case fields (**draft**, Caleb reviews them) and re-checks them with `check_peer_payload` before a report client sees them. `TemplateClient` restates the links, confirming banks, search results, decisions and limits, and flags non-confirmed cases as possibly incomplete.
- Reports are stored with their source (`template`) as drafts. An analyst generates them from the case page and approves or rejects each one once.
- Model and hosting: none. No API keys, network calls or model dependency.
- Reports are generated on demand, outside the detection path, so a report failure can't block detection. A template report **does not count** as AI reporting.

## Dependencies on other roles

Tracked in [decisions/case-identity.md](../decisions/case-identity.md#dependencies-on-other-roles), next to the `DEPENDS ON` comments in `apps/case/`.

## Open

- Retention lifetimes for cases, results and the outbox (Role 3 has the same TODO for its tables).
- Helm template for the case service and mock intake, with Isaac.
