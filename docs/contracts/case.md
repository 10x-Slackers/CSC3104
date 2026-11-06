# Case handling contract

Owner: Jia Rui · Partners: Zane (result delivery), Cindy, Isaac (alert counts), Caleb (report inputs) · Code: `apps/case/` · Status: **draft**, needs review by Zane

> [!NOTE]
> **Pending work (decided 30 Oct 2026).** The sections below describe what exists today; these items change it:
> - **Read-only analyst UI:** view, bookmark and a "reviewed" flag, with no free text. Remove the decision buttons and `POST /cases/{id}/decision`, the `decisions` and `outbox` tables, `outbox.py`, the report dispatcher, the mock intake (`intake.py`), the draft approve/reject step and the intake response time. What a bank does about a case is its own process (segregation of duties, MAS TRM §9.1.1); `Deliver` with acks still covers server callbacks.
> - **Two alert tiers** replace `CASE_MIN_TRANSFERS`/`CASE_MIN_BANKS` ([Cases and alerts](#cases-and-alerts)).
> - **Fed by the node:** the privacy node posts chains, Tier 1 hits, pending searches and fully internal chains through its outbox ([protocol.md](protocol.md#result-delivery-to-the-case-service)), each keyed for idempotency, so most of the case store's own deduplication goes. The transfer-join merge stays.
> - **Own-bank details:** snapshot this bank's legs (payment ID, account, amount) from the node's payload into a protected table at case creation; other banks' legs stay tokens. The case service never reads Valkey.
> - **PostgreSQL** (a separate database in the bank's CloudNativePG cluster), and cases are never auto-deleted (MAS Notice 626 ¶12.3 covers "results of any analysis").
> - **Summary service:** the template report moves into a separate function ([Summary service](#ai-assisted-report)).

Scope: the bank-local case schema, canonical chain identity, durable alert uniqueness, the analyst UI, mock intake and the AI-assisted report.

## Cases and alerts

| Item | Decision |
| --- | --- |
| Case schema | **draft**. `case_store.py`: SQLite (Postgres later) with tables `cases`, `transfers`, `claims`, `results`, `alerts`, `outbox`, `decisions`, `notifications`, `reports`. Only tokens, bank IDs and claim metadata; no accounts, amounts or payment IDs |
| Input and claim fields | **agreed with Role 3's code**. Input is `ChainResult` from [`privacy_node.proto`](../../apps/privacy_node/src/privacy_node/privacy_node.proto); each `LinkClaim` is stored as `signer`, `recipient`, `query_id`, `key_version`, `signing_key_version`, `truncated`, `in_pair`, `in_token`, `out_pair`, `out_token`, `signature`. Tokens and signatures are normalized to stable hex once in `Claim.from_pb`, and stored as text; peer protobuf tokens remain raw bytes. An empty ref marks the chain's start or end |
| When a chain is complete enough to alert | **draft**. A **confirmed** case with at least 2 transfers across at least 2 banks (`CASE_MIN_TRANSFERS`, `CASE_MIN_BANKS`). Partial chains are stored and shown but never alert. **pending work:** two tiers. A *rule-backed* claim has both an incoming and an outgoing ref (that bank's own rule linked them). **Tier 2, alert (pushed, at every bank on the chain):** a verified chain, confirmed **or** partial, with at least 2 rule-backed banks; partial counts because chains longer than the hop limit can never confirm. **Tier 1, local suspicion (shown, not pushed):** a hit from this bank's chain, new payee or quick repeat rule, a chain with one rule-backed bank, a pending search, or a fully internal chain; ranked by the number of rules fired, then amount, rather than requiring two (Guidelines to MAS Notice 626 B-1-1, "a combination of such situations may be indicative"). A fast pass-through link starts a search but makes no case on its own. A Tier 1 case is promoted when a chain through it reaches Tier 2. This follows MAS COSMIC's thresholds rising from Request to Provide to Alert, and fixes `m1` alerting at three banks when only B's rule linked its transfers. Expected Tier 1 load: about 4,000 hits per bank per day on HI-Small, 1–10 Sep (exploratory sweep), so the list pages |
| Canonical chain identity across overlapping searches | **draft**. A new case's ID is Role 3's `chain_digest`. A transfer `(key_version, pair, token_hex)` belongs to one case; a result sharing any transfer joins that case, and one spanning several cases merges them into the oldest. This also joins overlapping partial chains with different digests (a chain longer than 6 transfers, cut at different windows); over-limit `v1` gives five results with one digest, one case. |
| Unique alert key (durable, bank-local) | **draft**. `alerts.case_id` primary key, inserted with `ON CONFLICT DO NOTHING` in the same `BEGIN IMMEDIATE` transaction as the claims; a merge keeps one alert |
| Result states | **agreed with Role 3's code**. Only `CONFIRMED` and `PARTIAL` results are delivered and accepted ([protocol.md](protocol.md#result-states)); anything else is rejected. A case is confirmed once any of its results is; nothing downgrades it. Each `(chain_digest, sender, state)` is recorded once |
| Claim verification | Both `/results` and peer `Deliver` use `privacy_node.claims.check_result`. Receivers store its verified, normalized copy: signatures, token ownership, independent bank witnesses, digest, sender anchor, query/recipient binding and delivered state must verify; cuts stay partial. Invalid results return 422 and are never stored. `CASE_KEY_DIRECTORY` names the bank-key root; the loader uses `CASE_BANK` to combine `<root>/<bank>/public/signing/<version>.pem` with `<root>/<bank>/trusted/signing/<peer>/<version>.pem`. Keys are reread for each result. |
| Partial → confirmed updates | **draft**. A partial case (cut by the hop limit or branching cap) is stored and shown; it alerts once a confirmed result for any of its transfers arrives. **pending work:** it alerts as soon as it has 2 rule-backed banks |
| Durable pending notifications and acks | **draft**. Alerts and merges stay in the bank: an `alert:<case_id>` inbox entry is written in the same transaction as the alert, and a `merged:<case_id>` entry when an alerted case is merged away, so each shows once with no delivery step. Reports to the intake use a transactional outbox: a `report:<case_id>:<decision>` event is written with each "report" decision, so a later report decision sends again. `outbox.py` delivers at least once: it acks on success, retries any failure with doubling backoff (1 s up to 60 s) for at most `CASE_NOTIFY_MAX_ATTEMPTS` (10) attempts, and stores only the error class. The intake deduplicates on `(bank, event_id)`. A merge moves the old case's decisions, reports and pending report events to the surviving case. **pending work:** the decisions, report events, outbox and intake are removed |

## Analyst UI

- Framework: **draft**, FastAPI with server-rendered HTML (`app.py`). Run with `CASE_BANK=A CASE_DB=cases-a.db CASE_KEY_DIRECTORY=.secrets uv run python -m case.app` (port `PORT`, default 8000).
- Results come in on `POST /results`: the body is one serialized `ChainResult`; the reply `{"chain_digest": …}` is sent only after it is stored, like Role 3's `Ack`.
- Pages: a case list (state, alert time, latest decision), the notification inbox and undelivered events. Each case page lists its links in chain order, showing the bank that confirmed each one, both tokens (shortened), the search results and the decision history. JSON: `/api/cases`, `/api/cases/{id}`.
- Analyst decisions (all simulated): continue monitoring, suspend outgoing transactions, freeze accounts, report chain evidence to the local authorities (the mock intake). They are stored append-only, with no free text, so no account number can be typed in. Freeze and suspend touch nothing; mapping tokens back to accounts would need the token index.
- The report dispatcher runs in a background thread of the UI and sends `report` events to `CASE_INTAKE_URL`. The payload is built from `report_input`, so it passes the same `check_peer_payload` allowlist as draft reports.
- Report payload (**draft**, Role 4 to review): `event_id`, `bank`, `case_id`, `state`, `merged_into`, `raised_at`, and `links` with `signer`, `in_pair`, `in_token`, `out_pair`, `out_token`, `key_version`, `signing_key_version`, `truncated`.
- The mock intake (`intake.py`, port 8001) records timestamps and is outside the detection path. It keeps the first arrival per `(bank, event_id)`. `received_at − raised_at` is the intake response time, which includes the analyst's decision time.

**pending work** (UI): read-only, with a bookmark and a "reviewed" flag per case and no free text, so there's still no way to type an account number in. Analysts reach it through `kubectl port-forward` only; it has no login, so access control is cluster RBAC. Add `/healthz` for the readiness probe.

## AI-assisted report

**Decision: no AI report**; reports are template-only.

- `report.py`: `report_input` copies only allow-listed case fields (**draft**, Caleb reviews them) and re-checks them with `check_peer_payload` before a report client sees them. `TemplateClient` restates the links, confirming banks, search results, decisions and limits, and flags non-confirmed cases as possibly incomplete.
- Reports are stored with their source (`template`) as drafts. An analyst generates them from the case page and approves or rejects each one once.
- Model and hosting: none. No API keys, network calls or model dependency.
- Reports are generated on demand, outside the detection path, so a report failure can't block detection. A template report **does not count** as AI reporting.

**pending work (Jia Rui):** a summary service. The template moves into a separate function, a plain container locally and AWS Lambda on AWS: one `handle(json) -> json` with an HTTP adapter (with `/healthz`) and a `lambda_handler`, the same JSON in and out. The case service calls it through a function URL with `AWS_IAM` (signed with EKS Pod Identity), with a short timeout, and treats a failure as "no draft". As a separate function it's a bulkhead: a summary failure can't block detection. Cold and warm latency are measured separately. Its input is bank-local case evidence, including the bank's own legs, so an LLM would mean revisiting decision 0001 and Caleb's review.

## Dependencies on other roles

Tracked here and by the `DEPENDS ON` comments in `apps/case/`.

| Item | Role | Status | Role 5 file and line | Their file and line |
| --- | --- | --- | --- | --- |
| Minimum chain length for an alert (today ≥ 2 transfers across ≥ 2 banks) | Role 2 (streaming and scoring) | decided, **pending work**: replaced by the two tiers | `apps/case/src/case/case_store.py:53` | `docs/contracts/streaming.md:62` "Minimum chain length \| TODO" |
| The privacy node posts every `ChainResult` it saves to the case service's `POST /results` | Role 3 (privacy node) | decided, **pending work**: through its outbox, with own legs, hits and pending searches | `apps/case/src/case/app.py:196` | `apps/privacy_node/src/privacy_node/node.py:205` and `:264` (`store.save_chain`) |
| Public keys keyed by bank only (Role 3's code) vs by (bank, signing key version) (Role 4's contract) | Role 4 (security and privacy) | decided, **pending work**: (bank, key version) | `apps/case/src/case/evidence.py:17` | `apps/privacy_node/src/privacy_node/claims.py:39,42`; `docs/contracts/security.md:76` |
| Review of the report fields (draft reports, and the intake payload, which is built from the same `report_input`) | Role 4 (security and privacy) | decided, **pending work**: the intake goes; Caleb reviews the summary input and the own-leg data | `apps/case/src/case/report.py:15` | `docs/contracts/security.md:32`, `:166` |

## Interfaces

Agreed contract first ([conventions](../README.md#conventions)): the producer adds one sample message here, and both sides' tests load it.

| Interface | From → to | Pins down | Status | Agree by |
| --- | --- | --- | --- | --- |
| Alert tiers ([case.md](case.md#cases-and-alerts)) | Cindy, Zane → Jia Rui | Rule-backed claim, Tier 1 vs Tier 2, promotion; thresholds come from the offline sweep | **draft**, sample TODO | Wed 4 Nov |
| Case → summary service ([summary](case.md#ai-assisted-report)) | Jia Rui (both sides); the function URL: Isaac | JSON in and out, timeout, failure = no draft | **draft**, sample TODO | Fri 6 Nov |

Also part of: [Node → case service](protocol.md#interfaces), [`check_result`](security.md#interfaces).

## Open

- [x] Retention: cases and results are never auto-deleted (MAS Notice 626 ¶12.3); the outbox goes.
- [x] Helm templates: Isaac writes them from the runtime contract ([deployment.md](deployment.md#interfaces)).
