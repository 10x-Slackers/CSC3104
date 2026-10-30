# Streaming and scoring contract

Owner: Cindy · Partners: Macarius (events), Zane (search handoff), Caleb (tokens), Isaac (scaling, baseline comparison) · Code: `apps/scorer/` · Status: **draft**, needs review by Macarius and Zane

> [!NOTE]
> **Pending work (decided 30 Oct 2026).** The sections below describe what exists today; these items change it:
> - **Consume only:** the ingest mode and `validate()` move to the [ingestor](data.md#ingestor) (Macarius, who also owns this doc's event schema).
> - **Topic:** a declared `KafkaTopic`, 6 partitions, key = account, `retention.ms -1`, replication 3 with `min.insync.replicas` 2 in the HA bank. Replicas own partitions, so they share no mutable state.
> - **Four rules** replace the single rule ([Detection rule](#detection-rule)), and the scorer runs as a library over a bank file for the offline evaluator.
> - **State in Valkey:** window state, token index, local links and Tier 1 hits move out of memory, shared with the privacy node, which stops running the scorer in-process ([Search handoff](#search-handoff)).
> - **Event time:** a per-account watermark with allowed lateness = window; trim by event time, with a TTL only for garbage collection. `--max-events` is removed.
> - **Commits:** `enable.auto.offset.store=false`, `store_offsets()` after the Valkey write, auto-commit on (still at-least-once; duplicates are harmless because `detection_id` is deterministic).
> - **Intra-bank hops** are collapsed into one local link; KEDA scaling as in [Scaling](#scaling).

Scope: bank-local Kafka, the ingestor, token-index population, the scorer's rule windows, KEDA scaling and the local-only baseline.

## Kafka events

| Item | Decision |
| --- | --- |
| Topic names | `bank-A-transactions`, `bank-B-transactions`, `bank-C-transactions` |
| Event identity / dedup key | `(payment_id, direction, account)` within one bank; scorer deduplication lasts only while the event is retained |
| Partitioning key and ordering guarantee | Local `account`; preserves partition arrival order for that account, not event-time order. One partition per bank preserves global replay order when the shared history cap matters. **pending work:** 6 partitions; the event-time window absorbs interleaving, so global order no longer matters |
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
| Minimum chain length | Replaced by the alert tiers | **pending work:** Tier 2 needs at least two banks whose own rule linked the transfers ([case.md](case.md)) |
| Hop limit | 4 (`HOP_LIMIT`, up to 5 transfers) | Shared with [protocol.md](protocol.md); checked by a 4/6/8 sweep |
| Branching cap | 1 (`BRANCH_CAP`): linear chains | Fan rules flag accounts instead |
| Fees, currencies, insufficient history | Uses Role 1's USD-only data; no separate fee model. Missing/evicted history produces no local match | **pending work:** all amounts in SGD ([data.md](data.md)); history bounded by the event-time trim, not a count |

**pending work (Cindy):** four rules per account, at every bank, with starting thresholds tuned on the dev split ([evaluation.md](../evaluation.md#rules)). No regulator publishes numeric thresholds, so each rule cites a red flag and its numbers come from tuning.

| Rule | Starting thresholds | Basis | Output |
| --- | --- | --- | --- |
| Fast pass-through | out/in 0.90–0.98 within 1–30 min (today's rule) | Guidelines to MAS Notice 626 B-2(iii), "withdrawn immediately after being deposited" | Local link; starts a search |
| Slow pass-through | ACH in and out, out/in 0.90–1.00 within 72 h | B-4(i) "high velocity of funds", B-5(iv) "within a short period"; Altman's 90th-percentile hop delay is 69.7 h | Local link; starts a search |
| Fan-in | ≥ 5 distinct ACH senders within 72 h | B-4(v), funds "from various third parties"; "multiple depositors using a single account" | Account flag (Tier 1 hit) |
| Fan-out | ≥ 5 distinct ACH receivers within 72 h | No Notice 626 red flag names it; Altman's fan-out pattern and the NRA 2024's layering across many accounts | Account flag (Tier 1 hit) |

- Slow pass-through, fan-in and fan-out each write a Tier 1 hit. The fast rule never makes a case on its own (on Altman's dev days it found 1 laundering link in 4,278), but it stays because it's the cited "immediately" flag and the planted acceptance chains depend on it.
- Tier 1 cases are ranked by how many rules fired (B-1-1, "a combination of such situations may be indicative") rather than requiring two, which cut recall from 362 to 23 accounts on the dev days.
- ACH carries 87% of Altman's laundering but 12% of transfers, a dataset artefact; the ACH filter is reported as dev-tuned.
- **Intra-bank hops:** follow a bank's internal hops locally and emit one link from the first cross-bank payment in to the last cross-bank payment out, so the node signs one claim. Fully internal chains are flagged for the node to post as Tier 1 cases.

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
expiry, and the default size is unlimited. **pending work:** in Valkey, the index TTL is the search deadline plus the retry window (pending searches expire after 24 h), which is also its PDPA retention limit. Optional `--max-token-references` stops processing at capacity without
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
are unchanged.

**pending work:** the scorer writes to Valkey and the privacy node reads from it, so they run in separate pods. Keys: a sorted set per account for window state; `tok:<pair>:<ver>:<token>` → `(payment, direction, account, amount)`; `link:in:<payment>` → outgoing payments; `hit:<id>` → (rule, account, payments), where the ID is a hash of rule, account and first payment so a replay rewrites the same key; plus the per-account watermark and dedup keys. Windows: 30 min of all events for the fast rule, 72 h of ACH events only for the others. Only the scorer and the privacy node may connect. On loss, Valkey is rebuilt by replaying from `earliest` with a fresh group.

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

**pending work** (chart: Isaac): a KEDA `ScaledObject` on consumer-group lag.

- `maxReplicaCount` 6 (the partition count), `minReplicaCount` 1 on k3d.
- `offsetResetPolicy: earliest`: with the default `latest`, a fresh group reports lag 1, so the first replay would never scale.
- `lagThreshold` = measured events/s per replica × acceptable delay.
- `pollingInterval` 30 s and `cooldownPeriod` 300 s (defaults) bound the reaction time and are reported.
- Bank B's hot account carries 13% of its events, which caps its speed-up at about 3.7× with 6 replicas; report it as a measured limit.

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

**pending work:** the offline evaluator ([evaluation.md](../evaluation.md#offline-detection-evaluation)) supersedes this as the local-only vs cross-bank comparison, running the same scorer code.

## Interfaces

Agreed contract first ([conventions](../README.md#conventions)): the producer adds one sample message here, and both sides' tests load it.

| Interface | From → to | Pins down | Status | Agree by |
| --- | --- | --- | --- | --- |
| Kafka transaction event ([streaming.md](streaming.md#kafka-events)) | Macarius → Cindy | Topic, key, JSON fields, CloudEvents headers, two events per intra-bank transfer | **draft**, sample TODO | Mon 2 Nov |
| Token and key API ([security.md](security.md)) | Caleb → Cindy, Zane | `derive_token` signature, 16 bytes out, key file layout and `key_version`, one test vector | **draft**, sample TODO | Mon 2 Nov |
| Valkey layout ([streaming.md](streaming.md#search-handoff)) | Cindy → Zane | Key names, token-index value, local links (including collapsed intra-bank links), `hit:` keys, windows, TTL and trim | **draft**, sample TODO | Wed 4 Nov |
| Alert tiers ([case.md](case.md#cases-and-alerts)) | Cindy, Zane → Jia Rui | Rule-backed claim, Tier 1 vs Tier 2, promotion; thresholds come from the offline sweep | **draft**, sample TODO | Wed 4 Nov |
| Scorer as a library | Cindy → Isaac | Entry point over one bank file, in-memory state store | **draft**, sample TODO | Wed 4 Nov |

## Open

- [ ] Rule thresholds, from the offline sweep on the dev split (Cindy, with Isaac).
