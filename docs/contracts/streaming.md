# Streaming and scoring contract

Owner: Cindy · Partners: Macarius (events), Zane (search handoff), Caleb (tokens), Isaac (scaling, baseline comparison) · Code: `apps/scorer/` · Status: **draft**, local implementation available; integration decisions below remain open

> [!WARNING]
> **Outdated (30 Oct 2026).** Under review after a full codebase walkthrough; don't build against this doc until it's updated. Expected to change:
> - **Kafka events**: ingest moves out of the scorer (replay produces to Kafka); topic partitioning
> - **Detection rule**: the rule and its parameters are under review
> - **Token index**, **Search handoff**: index and scorer state move out of memory into Valkey, shared with the privacy node
> - **Scaling**

Scope: bank-local Kafka, the ingestor, token-index population, the scorer's rule windows, KEDA scaling and the local-only baseline.

## Kafka events

| Item | Decision |
| --- | --- |
| Topic names | `bank-A-transactions`, `bank-B-transactions`, `bank-C-transactions` |
| Event identity / dedup key | `(payment_id, direction, account)` within one bank; scorer deduplication lasts only while the event is retained |
| Partitioning key and ordering guarantee | Local `account`; preserves partition arrival order for that account, not event-time order. One partition per bank preserves global replay order when the shared history cap matters |
| Replay, late and out-of-order events | Match either arrival order within each account's event-time window; ignore events older than the latest timestamp seen for that account minus `--window`. Events are retained for twice `--window` so an accepted late event still finds its partner |

The ingestor reads Role 1's JSONL replay from stdin using the event format in
[data.md](data.md). It rejects invalid or wrong-bank events; the consumer also
checks the topic and account key. Both accept `--bootstrap-servers` (default
`localhost:9092`). Producer queue waits and final flush are each bounded at 30 seconds.
The k3d Kafka has only an in-cluster listener, so run the scripts inside the
cluster with `--bootstrap-servers kafka-kafka-bootstrap:9092`; a port-forward does
not work because the broker advertises in-cluster addresses. The consumer may
start before the topic exists: non-fatal Kafka errors are logged and retried.

Scorer history is in memory: at most `--max-events` (default 10,000) globally,
with time expiry per account. Capacity eviction removes the oldest arrival; each
account's watermark is kept, so late events stay ignored after eviction. Consumer offsets
commit synchronously after optional indexing, scoring and output flush succeed;
processing and fatal Kafka errors stop the consumer without committing the failed record.

Restart/reassignment requires a full replay to rebuild state. Use a fresh
`--group` (default `scorer-<bank>`); `auto.offset.reset=earliest` does not rewind
existing committed offsets. Replay requires the original records to remain in
Kafka. Output and commits are not atomic; repeated detections are possible.
There is no durable state or multi-replica recovery; use one consumer per bank.
Integrated mode rejects resumed committed offsets, history missing offset zero,
and multi-partition topics at assignment. Use a fresh group to rebuild state;
reassignment after commits also stops rather than trusting incomplete memory.
This guard does not make replicas safe: an idle replica can still have no state
while receiving gRPC traffic. Kafka lag alone is not readiness.

SIGINT/SIGTERM finish the current local handoff/commit and close the consumer.
The search worker stops between operations, leaving pending searches/deliveries
for recovery; the server stops and the worker has a bounded shutdown wait.
In-memory candidates and token/link state are not checkpointed.

## Detection rule

The rule indicates suspicion, not proven laundering. Start with a single-currency linear-chain fixture, and label unsupported cases explicitly.

| Parameter | Value | Notes |
| --- | --- | --- |
| Incoming-to-outgoing value ratio | Outgoing / incoming: 0.90–0.98 inclusive | `--min-ratio`, `--max-ratio`; draft defaults pending team approval |
| Time window | 1–30 minutes inclusive, using transaction timestamps | `--min-delay` (60 seconds), `--window` (1800 seconds) |
| Minimum chain length | TODO | |
| Hop limit | TODO | Shared with [protocol.md](protocol.md) |
| Branching cap | TODO | |
| Fees, currencies, insufficient history | Uses Role 1's USD-only data; no separate fee model. Missing/evicted history produces no local match | TODO: agree reporting of unsupported cases and incomplete history |

The scorer reports every qualifying incoming/outgoing pair for the same local
account; it does not allocate incoming funds across payments or perform graph
search. Ground-truth labels are never used. JSONL output contains local candidate
links, not confirmed chains or analyst alerts: bank, account, incoming/outgoing
payment IDs and times, ratio, delay in seconds and `detection_id`.

`detection_id` is SHA-256 of the UTF-8 compact JSON array
`[bank, incoming_payment_id, outgoing_payment_id]`, with default JSON escaping.
Replaying the same pair produces the same ID. This output stays bank-local;
it is not an approved peer payload.

## Token index

Optional `TokenIndex` population runs on validated cross-bank events before
scoring, including unflagged and scorer-too-late events. It uses the original
`payment_id` and actual `counterparty_bank` with Role 4's `canonical_pair`,
`derive_token` and `load_pairwise_keys`; same-bank events are skipped. See
[security.md](security.md) for the token and secret-file contract.

Enable with `--key-version k1` and repeated `--pairwise-key PEER PATH` options on
`python -m scorer.stream consume`; omitting both keeps scorer-only operation. Missing/invalid
configuration fails; an event whose pair lacks a key is not committed. A new
key/version requires a new index and replay; there is no old-key fallback.

The in-memory index maps `(canonical bank pair, key version, token)` to unique
local `(payment_id, direction, account)` references. `lookup(pair, version, token)`
returns those references in deterministic order and rejects unrelated pairs.
Duplicate replay adds no duplicate references. The index and lookup results stay
bank-local; Role 3 must not send them as peer responses. There is currently no
expiry, and the default size is unlimited: TODO, agree tracing/retry lifetime
and retention with Roles 3 and 6 before discarding records, including unmatched
payments. Optional `--max-token-references` stops processing at capacity without
evicting references or committing the failed event. Duplicate references remain
acceptable at capacity. This is a fail-stop resource bound, not an expiry policy.

## Search handoff

Integrated mode runs Role 2's consumer inside the privacy-node process. Use
`uv run python -m privacy_node.node --bank B --bootstrap-servers <brokers>
--group <fresh-group> --search` with the existing pairwise, signing, public-key
and peer options from [protocol.md](protocol.md). `--workload` selects the existing
standalone CSV mode instead; the two input modes are mutually exclusive.

The consumer calls `LocalBank.process(event)`, which uses the existing `Scorer`
and `TokenIndex` under one local lock and publishes token references and local
links before returning candidates. Kafka offsets commit only after this update
and candidate output succeed. Integrated mode does not read CSV. The node's
search loop starts each traceable candidate once per process using its stable
`detection_id`; intra-bank legs have no peer token and are not searched. Existing
gRPC messages contain tokens/claims, never these raw local references.

Candidates and token/link state remain in memory, not in the node's SQLite
protocol store. A crash after offset commit but before search requires full
replay with a fresh group; there is no atomic durable handoff or exactly-once
guarantee. Role 3's existing pending/timed-out search retries and result outbox
are unchanged. TODO: agree persistence/recovery with Role 6 and a grace/retry
policy when a peer has not ingested an event yet (currently `NO_MATCH`).

Optional `--ingestion-grace-seconds N` treats a valid origin-side `NO_MATCH` as
`PENDING` until a fixed deadline from that search's first attempt. Retries use
fresh query IDs; peer replies, claim verification and message schemas are
unchanged. After the deadline, an absent record becomes final `NO_MATCH` and
missing-record retries stop. Default `0` preserves existing behavior; enabling
it and choosing a duration still require team agreement. Grace deadlines are
in memory and restart on a new process. Existing unavailable-peer retries are
separate and unchanged. This does not resolve a peer mistaking an ingested
incoming transfer for an endpoint before its outgoing leg arrives.

## Scaling

TODO: KEDA trigger (Kafka consumer-group lag), min and max replicas, and the metrics exported for [evaluation.md](../evaluation.md).

## Local-only baseline

`uv run python -m scorer.baseline apps/dataset/fixtures/tiny B` reads only that bank's
CSV and exports JSONL candidates using the same scorer, without Kafka or peers.
Repeat for A and C. Defaults can be overridden with `--min-ratio`, `--max-ratio`,
`--min-delay`, `--window` and `--max-events`.

Verified tiny-fixture local pairs (stable IDs reproduce on replay):

| Bank | Detections | Incoming → outgoing |
| --- | --- | --- |
| A | 1 | p17 → p18 |
| B | 4 | p06 → p07; p10 → p11; p15 → p16; p18 → p19 |
| C | 3 | p11 → p12; p16 → p17; p19 → p20 |

TODO: Isaac specifies the full-system baseline comparison in [evaluation.md](../evaluation.md).

## Open

- TODO: team approval of rule defaults and chain/branching rules with Role 3.
- TODO: token-index retention and recovery/deployment policy with Roles 3 and 6.
