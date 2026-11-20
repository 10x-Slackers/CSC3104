# Cases and the analyst page

Each bank has its own case service. It takes the messages its privacy node sends, joins them into cases, decides which are alerts, and shows them to the bank's analyst. A separate summary service drafts a plain-English summary of a case.

Code: [case/store.py](../src/mulechain/case/store.py) (cases), [case/app.py](../src/mulechain/case/app.py) (page and API), [summary.py](../src/mulechain/summary.py) (drafts).

## Messages from the node

The privacy node posts batches of messages to `POST /messages` through its outbox ([protocol.md](protocol.md#failure-handling)). The reply is `{"stored": n}`; without a reply the node sends the batch again.

| Kind | Sent when | `key` | Extra fields |
| --- | --- | --- | --- |
| `hit` | A rule fired at this bank | The scorer's hit ID | `rule`: `chain`, `new_payee` or `quick_repeat` ([detection.md](detection.md)) |
| `pending` | A search got no answer from a bank | The search ID | `waiting_for`: the bank, e.g. `"C"` |
| `local` | The money starts or stays inside this bank | The search ID | |
| `chain` | A verified chain was stored | The chain's digest | `state` (`confirmed` or `partial`), `links` (the claims, without signatures) |

Every message carries `legs`: this bank's own payments behind it. Other banks' transfers appear only as tokens in `links`. A `chain` message at bank B for money that went A → B → C:

```json
{"key": "f1207f3e…", "kind": "chain", "state": "confirmed",
 "legs": [
   {"payment_id": "p06", "direction": "in", "account": "B-m1", "amount": 10000.0, "peer": "A", "token": "f4c6b7f0…"},
   {"payment_id": "p07", "direction": "out", "account": "B-m1", "amount": 9500.0, "peer": "C", "token": "94f76064…"}],
 "links": [
   {"signer": "A", "in_from": "", "in_token": "", "out_to": "B", "out_token": "f4c6b7f0…"},
   {"signer": "B", "in_from": "A", "in_token": "f4c6b7f0…", "out_to": "C", "out_token": "94f76064…"},
   {"signer": "C", "in_from": "B", "in_token": "94f76064…", "out_to": "", "out_token": ""}]}
```

The legs are copied into the case store when the message arrives, because cases are kept long after Valkey has expired the payments. The case service never reads Valkey.

## From messages to cases

**One case per bank per chain.** Each transfer belongs to one case. `CaseStore.record` looks up the message's transfers (`tok:<token>` and, for own payments, `pay:<payment id>`) in the `refs` table:

- None is known: a new case, whose ID is the message's key.
- One case has them: the message joins it.
- Several cases have them: they merge into the oldest (`_join`). The merged case keeps the highest tier, the strongest state and the earliest alert. It is bookmarked if either was, and reviewed only if both were.

So a rule hit, the pending search it started and the chain that later confirms it land on one case, and overlapping partial chains from a long chain become one case too. See `test_messages_sharing_a_transfer_join_one_case_and_alert_once` and `test_a_message_bridging_two_cases_merges_them_into_the_older` in [test_case_store.py](../tests/test_case_store.py).

**Repeats change nothing.** A message is stored once per `(key, kind)`; a second copy returns the same case (an idempotent receiver). `test_the_node_posts_batches_and_a_resent_batch_is_harmless` in [test_case_app.py](../tests/test_case_app.py).

**Advisory lock.** A batch is stored in one transaction that holds the PostgreSQL advisory lock `JOIN_LOCK`, so two writers can't both start a case for the same transfer.

### Tiers and alerts

A claim with both an incoming and an outgoing token is **rule-backed**: that bank's own rule linked the two transfers. A bank that only sent or only received the money signs a one-sided claim, which confirms the transfer but backs nothing.

| Tier | When (`tier`) | Shown as |
| --- | --- | --- |
| 2 | A chain with rule-backed claims from at least two banks, confirmed or partial | **Alert**, at every bank on the chain |
| 1 | A chain where only this bank's rule fired; a rule hit; a pending search; a local case | Low priority |
| 0 | A chain where only another bank's rule fired | Stored, not listed |

A partial chain can alert because a chain longer than the hop limit can never be confirmed in one piece. A case gets **one alert** (`alerted_at`), set the first time it reaches Tier 2, however many chains join it later. If a case moves up a tier, its "reviewed" flag is cleared.

A rule flags suspicion, not proven laundering.

### Case states

A case shows the strongest state among its messages (`STATES`): `confirmed`, `partial`, `pending` (shown as "Waiting for bank X"), `local`, then `hit`. Nothing downgrades a case.

### Order of the list

`CaseStore.cases` lists open cases, most urgent first: alerts; then Tier 1 by how many of this bank's rules fired; then by amount; then oldest first. Merged-away and Tier 0 cases are left out. `test_cases_are_listed_alerts_first_then_by_rules_and_amount`.

## Analyst page and API

| Path | Shows |
| --- | --- |
| `GET /` | The 200 most urgent cases: tier, state, rules fired, amount, alert time, flags. Reloads every 5 s |
| `GET /cases/{id}` | One case: this bank's payments, the chain in money-flow order with the bank that signed each link, the messages received, and a draft summary |
| `POST /cases/{id}/bookmarked`, `POST /cases/{id}/reviewed` | Flip a flag |
| `GET /api/cases` | Every listed case as JSON, each with its own payment IDs (`payments`). `make cases` and the cluster checks compare this with the offline evaluator |
| `GET /api/cases/{id}` | One case as JSON; this is also what the summary service receives |
| `GET /healthz` | Readiness |

The page is read-only apart from the two flags. There is no free text and no decision: what a bank does about a case is its own process. There is no login either; the page is reached with `make ui BANK=b` (a `kubectl port-forward`), so access control is the cluster's. [deployment.md](deployment.md) has the commands.

Each stored message logs one JSON line (`event: stored` with `key`, `kind`, `case_id`, `tier`, `alerted_at`, `payments`), from which alerts can be timed ([evaluation.md](evaluation.md)).

## Summary service

`summary.handle` takes one case as JSON and returns `{"draft": text}`: a headline saying why the case was raised, this bank's payments, the path of the money bank by bank, and a caveat. It is a fixed template, not a language model, because the input holds customer details.

- **Bulkhead.** The case service gives it `SUMMARY_SECONDS = 2` and shows "No draft" on any failure or slow answer (`summarizer`), logging `event: no_draft`. A summary outage can't touch detection or the rest of the page: `test_a_slow_or_broken_summary_service_only_costs_the_draft`.
- **Stateless.** It has no state, no keys and no network access of its own. Drafts aren't stored; each page view makes a fresh one from the stored case.
- **Container or Lambda.** On k3d it is a small HTTP server (port 8080). On AWS the same `handle` runs as a Lambda behind a function URL, and the case service signs each request with SigV4 (`request_headers` in [serverless.py](../src/mulechain/serverless.py)).
- Each request logs `status`, `ms`, `cold` (the first request in the process) and `case_id`.

## What the case service trusts

Only its own bank's privacy node. NetworkPolicy lets nothing else reach port 8000 ([security.md](security.md)), and the node has already verified every chain it posts, both the ones it assembled and the ones delivered to it. So the case service checks no signatures. The summary service in turn accepts calls only from its bank's case service.

## Storage and limits

- Cases live in the bank's PostgreSQL database (SQLite in tests and the offline evaluator), in the tables `cases`, `refs`, `messages` and `legs`. They are never deleted: a bank must keep the results of its analysis.
- The disclosure boundary is between banks, not inside one. Bank B's analyst sees B's own customers' accounts and amounts; other banks' parts of a chain stay tokens.
- A case keeps saying "Waiting for bank X" if its search ends without a chain ([protocol.md](protocol.md#known-limits)).
- On a cluster, searches run while payments still arrive, so overlapping searches can finish in a different order from one run to the next. Which payments a case lists can depend on that order, since each bank joins what it receives ([evaluation.md](evaluation.md)).
- The node sends messages in batches of up to 500, and a batch is stored or refused as a whole. A batch the case service can't store is sent again until it can, and holds up the messages behind it.
