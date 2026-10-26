# Case identity

Owner: Jia Rui (Role 5) · Date: 2026-10-25 · Status: **draft**, needs review by Roles 2, 3 and 6 (the workload split's supporting roles for this decision)

## Context

The workload split asks Role 5 to "define when a chain is complete enough to alert and how overlapping searches identify the same chain", using "a durable bank-local uniqueness rule for that agreed case identity". Role 3 proposed its `chain_digest` as the case identity ([protocol.md](../contracts/protocol.md), Open), and asked how overlapping **partial** chains should merge: over-limit `v1` gives five partial results with two digests.

## Decision

- **Case ID = Role 3's `chain_digest`** (`apps/privacy_node/claims.py`) of the first result that creates the case. Searches from different banks over the same transfers already share it (loop `o1` from B and C).
- **Join on a shared transfer.** Each transfer, keyed `<pair>|<token>` like `claims.key`, belongs to one case. A result that shares any transfer with a case joins it; a result that spans several cases merges them into the oldest, which keeps exactly one alert. This answers Role 3's question: the five `v1` partials join one case, even though they have two digests.
- **One alert per case at each bank:** `alerts.case_id` is the primary key, written in the same transaction as the claims and its inbox entry; reports to the intake use a transactional outbox.
- **Only `CONFIRMED` and `PARTIAL` results are accepted**, matching what the privacy node delivers. A case is confirmed once any of its results is, and is never downgraded. Only a confirmed case can alert.

Code: `apps/case/case_store.py`, `apps/case/evidence.py`. Tests: `apps/case/test_case_store.py`, using real results from Role 3's nodes (`apps/case/sample_chains.py`).

## Dependencies on other roles

Role numbers follow the workload split: 2 streaming and scoring, 3 privacy node protocol, 4 security and privacy.

| Item | Role | Status | Role 5 file and line | Their file and line |
| --- | --- | --- | --- | --- |
| Minimum chain length for an alert (today ≥ 2 transfers across ≥ 2 banks) | Role 2 (streaming and scoring) | pending | `apps/case/case_store.py:53` | `docs/contracts/streaming.md:45` "Minimum chain length \| TODO" |
| The privacy node posts every `ChainResult` it saves to the case service's `POST /results` | Role 3 (privacy node) | pending | `apps/case/app.py:201` | `apps/privacy_node/node.py:183` and `:247` (`store.save_chain`) |
| Public keys keyed by bank only (Role 3's code) vs by (bank, signing key version) (Role 4's contract) | Role 4 (security and privacy) | pending | `apps/case/evidence.py:17` | `apps/privacy_node/claims.py:53,56`; `docs/contracts/security.md:70` |
| Review of the report fields (draft reports, and the intake payload, which is built from the same `report_input`) | Role 4 (security and privacy) | pending | `apps/case/report.py:16` | `docs/contracts/security.md:26`, `:160` |

Resolved by Role 3's merged code (assumptions removed): claim fields, result fields, result states, signed claim bytes and claim verification now use `privacy_node.proto` and `claims.py`. The outbox retry timings are a Role 5 choice, so no dependency is recorded for them.
