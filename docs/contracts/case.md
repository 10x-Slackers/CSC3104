# Case handling contract

Owner: Jia Rui · Partners: Zane (node → case messages), Cindy, Isaac (alert counts, deployment), Caleb (own-leg data, summary input) · Code: `apps/case/`, `apps/summary/` · Status: **draft**, needs review by Zane and Caleb

> [!NOTE]
> **Pending work (decided 30 Oct 2026).** The case service and summary service below are built; these items, owned by others, are still needed to run them end to end:
> - **Node delivery (Zane):** the privacy node posts its messages to `POST /results` through its outbox ([protocol.md](protocol.md#result-delivery-to-the-case-service)). Nothing calls it yet; the message format and shared sample are proposed below and need Zane's approval.
> - **Deployment (Isaac):** a `cases` database in each bank's CloudNativePG cluster, and Deployments for the case and summary services from the runtime contract in [deployment.md](deployment.md#per-service-settings); the summary service as a Lambda function URL on EKS.

Scope: the bank-local case schema, alert tiers, canonical chain identity, durable alert uniqueness, the read-only analyst UI and the summary service.

## Cases and alerts

| Item | Decision |
| --- | --- |
| Case schema | **draft**. `case_store.py`: PostgreSQL in the cluster (a separate `cases` database in the bank's CloudNativePG cluster, `CASE_DATABASE_URL=postgresql://…`), or a SQLite file for tests and local runs; the same SQL runs on both. Tables `cases` (tier, state, bookmark and reviewed flags), `transfers`, `messages`, `claims`, `own_legs`, `alerts`, `notifications`. Other banks' legs are tokens only; this bank's own legs are in `own_legs` |
| Input | **draft, needs Zane**. One JSON message per `POST /results` from this bank's privacy node, in four kinds: `chain`, `pending`, `local`, `hit` ([format and sample](#node-messages)). A chain carries the `ChainResult` from [`privacy_node.proto`](../../apps/privacy_node/src/privacy_node/privacy_node.proto) as protobuf JSON; each `LinkClaim` is stored as `signer`, `recipient`, `query_id`, `key_version`, `signing_key_version`, `truncated`, `in_pair`, `in_token`, `out_pair`, `out_token`, `signature`. Tokens and signatures are normalized to hex once in `Claim.from_pb`. An empty ref marks the chain's start or end |
| Own-bank details | **draft, needs Caleb**. Every message carries this bank's own legs (`payment_id`, `direction`, `account`, `amount`, `pair`, `token`), looked up by the node in its token index. They are snapshotted into `own_legs` when the message arrives, never on a page view, because the token index expires in days while cases are kept. The disclosure boundary is between banks, not inside one: B's analyst sees B's own customers' accounts and amounts, and other banks' legs stay tokens. The case service never reads Valkey |
| Alert tiers | **draft**. A *rule-backed* claim has both an incoming and an outgoing ref: that bank's own rule linked them. **Tier 2, alert** (pushed to the alert inbox, at every bank on the chain): a verified chain, confirmed **or** partial, with at least 2 rule-backed banks; partial counts because chains longer than the hop limit can never confirm. **Tier 1, low priority** (shown, not pushed): a chain where only this bank's rule fired, a pending search, a fully internal chain, or a rule hit. A chain where neither condition holds (e.g. `m1` at A or C, where only B's rule fired) is stored as evidence but shows no case. A case's tier is its highest message's, so a Tier 1 case is promoted when a chain through it reaches Tier 2. This follows MAS COSMIC's thresholds rising from Request to Provide to Alert, matches the offline evaluator's `chain_tier`, and stops `m1` alerting at three banks when only B's rule linked its transfers. The case list shows alerts first, then low-priority cases, oldest first |
| Canonical chain identity across overlapping messages | **draft**. A new case's ID is the message key (Role 3's `chain_digest` for a chain). Each transfer belongs to one case: `tok:<pair>:<token>` for a cross-bank transfer and `pay:<payment_id>` for an own leg. A message sharing any transfer joins that case, and one spanning several cases merges them into the oldest, with their evidence, alert and flags. This joins a pending search with its later chain, a rule hit with the chain through it, and overlapping partial chains with different digests (a chain longer than the hop limit arrives as several partial results); over-limit `v1` gives one case |
| Idempotency | **draft**. The node sends each message once per insert or upgrade, so the store keeps one `messages` row per key (the idempotent consumer), upgraded in place (partial → confirmed). Redeliveries change nothing. Claims keep their natural key, so overlapping results store each claim once |
| Unique alert key (durable, bank-local) | **draft**. `alerts.case_id` primary key, inserted with `ON CONFLICT DO NOTHING` in the same transaction as the message, which takes a table lock (PostgreSQL) or `BEGIN IMMEDIATE` (SQLite) so concurrent merges serialize; a merge keeps one alert |
| Case states | **agreed with Role 3's code**. A case shows its strongest message's state: `confirmed`, `partial`, `pending` (with the bank it waits for), `local`, then `hit`. Only `CONFIRMED` and `PARTIAL` chains are accepted ([protocol.md](protocol.md#result-states)); nothing downgrades a case |
| Claim verification | Both `/results` and peer `Deliver` use the shared `privacy_node.claims.check_result`: signatures, token ownership, independent bank witnesses, digest, sender anchor, query/recipient binding and delivered state must verify, and cuts are forced to partial. The message key must equal the digest. Invalid messages return 422 and are never stored. `pending`, `local` and `hit` messages and the own legs come only from this bank's node and are trusted. `CASE_KEY_DIRECTORY` names the bank-key root; the loader combines `<root>/<bank>/public/signing/<version>.pem` with `<root>/<bank>/trusted/signing/<peer>/<version>.pem`, reread for each message |
| Alert inbox | **draft**. An `alert:<case_id>` entry is written in the same transaction as the alert, and a `merged:<case_id>` entry when an alerted case is merged away, so each shows once with no delivery step |
| Retention | Cases, messages and own legs are never deleted (MAS Notice 626 ¶12.3 covers "results of any analysis") |

### Node messages

**draft, proposed by Jia Rui for Zane to approve.** Shared sample: [`apps/case/fixtures/node_messages.json`](../../apps/case/fixtures/node_messages.json), one message of each kind at bank B; the case tests load it, and the node's tests should too. `POST /results` replies `{"key": …}` with 200 once stored, or 422 for a message that isn't evidence (dead-letter it; retry anything else).

| Field | Kinds | Meaning |
| --- | --- | --- |
| `key` | all | Idempotency key: `chain_digest`, the search ID, or the scorer's hit ID |
| `kind` | all | `chain`, `pending`, `local` (a fully internal chain) or `hit` |
| `legs` | all | This bank's own legs: `payment_id`, `direction` (`in`/`out`), `account`, `amount`, `pair` and `token` (hex; both empty for an intra-bank leg) |
| `result` | `chain` | The `ChainResult` as protobuf JSON (`MessageToDict(result, preserving_proto_field_name=True)`) |
| `waiting_for` | `pending` | Banks the search is waiting for, e.g. `["C"]` |
| `rule` | `hit` | `chain`, `new_payee` or `quick_repeat` |

## Analyst UI

- Framework: **draft**, FastAPI with server-rendered HTML (`app.py`). Run with `CASE_BANK=A CASE_DATABASE_URL=cases-a.db CASE_KEY_DIRECTORY=.secrets uv run python -m case.app` (port `PORT`, default 8000). Settings are in [deployment.md](deployment.md#per-service-settings).
- **Read-only.** Analysts view cases, bookmark them and mark them reviewed; there is no free text, so no account can be typed in, and no decision. What the bank does about a case is its own process (segregation of duties, MAS TRM §9.1.1); `Deliver` with acks covers server callbacks. It's one model with a read-only view.
- Pages: the case list (tier, state, alert time, bookmark, reviewed) and the alert inbox. Each case page shows this bank's own legs, the links in chain order with the bank that confirmed each and other banks' tokens, the node messages received, and a draft summary. JSON: `/api/cases`, `/api/cases/{id}`. Readiness: `GET /healthz`.
- Access: `kubectl port-forward` only; there is no login, so access control is cluster RBAC.
- Logs: one JSON line to stdout per message, timestamped: `ts`, `service: case`, `bank`, `event` (`stored` or `rejected`), and for stored messages `key` (the `chain_digest` for a chain), `kind`, `case_id`, `tier` and `raised_at`, the time the case's alert was stored. The evaluation joins `chain_digest` to `raised_at` on one clock. A failed summary call logs `event: no_draft` with the error class.

## Summary service

**Decision: no AI report**; summaries are template-only. An LLM would see customer details, so it would mean revisiting decision 0001 and Caleb's review.

- `apps/summary/`: `summary.handle(case) -> {"source": "template", "draft": text}` is the whole service, JSON in and out. `summary.app` serves it as an HTTP server (`POST /`, readiness `GET /healthz`, port `PORT`, default 8080) and as `lambda_handler` behind a Lambda function URL with `AuthType: AWS_IAM`; both take the same JSON. Each request logs one JSON line: `ts`, `service: summary`, `status`, `ms`, `cold` (the first request in the process) and `case_id`, so cold and warm latency are measured separately.
- Input (**draft**, Caleb reviews): `report.report_input` copies only `bank`, `case_id`, `tier`, `state`, `raised_at`, `merged_into`; `links` with `signer`, `in_pair`, `in_token`, `out_pair`, `out_token`, `key_version`, `signing_key_version`, `truncated`; this bank's `legs` with `payment_id`, `direction`, `account`, `amount`, `pair`; and `messages` with `key`, `kind`, `state`, `detail`. No signatures.
- The case service calls it on each case page view with a short timeout (`CASE_SUMMARY_TIMEOUT`, 2 s) and treats any failure as "no draft". On EKS it signs the request with SigV4 (`CASE_SUMMARY_REGION`, credentials from EKS Pod Identity). As a separate function it's a bulkhead: a summary failure can't block detection (experiment 9).
- Drafts aren't stored: each is regenerated from the stored case, which is kept.

## Dependencies on other roles

Tracked here and by the `DEPENDS ON` comments in `apps/case/`.

| Item | Role | Status | Role 5 file | Their file |
| --- | --- | --- | --- | --- |
| The privacy node posts its messages to `POST /results` | Role 3 (privacy node) | **pending work** (Zane): format proposed in [Node messages](#node-messages) | `apps/case/src/case/app.py` (`results`), `evidence.py` (`parse`) | `apps/privacy_node/src/privacy_node/store.py` (`save_chain`, outbox) |
| Rule-backed claims and tiers | Roles 2 and 3 | **draft**: same definition as the offline evaluator | `apps/case/src/case/evidence.py` (`tier`) | `apps/evaluation/src/evaluation/run.py` (`chain_tier`) |
| Own-leg data and the summary input fields | Role 4 (security and privacy) | **draft**, Caleb to review | `apps/case/src/case/report.py` (`report_input`) | `docs/contracts/security.md` |
| A `cases` database, Deployments and the summary Lambda | Isaac (deployment) | **pending work**: runtime contract sent in [deployment.md](deployment.md#per-service-settings) | `apps/case/src/case/app.py`, `apps/summary/src/summary/app.py` | `deploy/charts/bank/` |

## Interfaces

Agreed contract first ([conventions](../README.md#conventions)): the producer adds one sample message here, and both sides' tests load it.

| Interface | From → to | Pins down | Status | Agree by |
| --- | --- | --- | --- | --- |
| Alert tiers ([case.md](case.md#cases-and-alerts)) | Cindy, Zane → Jia Rui | Rule-backed claim, Tier 1 vs Tier 2, promotion; thresholds come from the offline sweep | **draft**; the case tests check `m1` Tier 1 at B only, `o1` and `v1` Tier 2 at every bank | Wed 4 Nov |
| Case → summary service ([summary](case.md#summary-service)) | Jia Rui (both sides); the function URL: Isaac | JSON in and out, 2 s timeout, failure = no draft | **draft**; sample in `apps/summary/tests/test_summary.py` | Fri 6 Nov |

Also part of: [Node → case service](protocol.md#interfaces), [`check_result`](security.md#interfaces).

## Open

- [ ] Zane approves the node message format and sample, and the node posts through its outbox (Zane).
- [ ] Caleb reviews the own-leg snapshot and the summary input fields (Caleb).
- [ ] Rank Tier 1 cases by the number of rules fired, then amount (Guidelines to MAS Notice 626 B-1-1), and page the list: about 2,400 flagged payments per bank per day on HI-Small's eval days (Jia Rui).
- [x] Retention: cases are never auto-deleted (MAS Notice 626 ¶12.3).
- [x] Helm templates: Isaac writes them from the runtime contract ([deployment.md](deployment.md#interfaces)).
- [x] Analyst decisions, the report outbox and the mock intake are removed: the UI is read-only.
