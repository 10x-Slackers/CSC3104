# Streaming and scoring contract

Owner: Cindy · Partners: Macarius (events), Zane (search handoff), Caleb (tokens), Isaac (scaling, baseline comparison) · Code: `apps/scorer/` · Status: **draft**, needs review by Macarius and Zane

> [!NOTE]
> **Pending work (decided 30 Oct 2026).** The sections below describe what exists today; these items change it:
> - **Event schema (draft, Macarius → Cindy):** each event gains `counterparty_account` and CloudEvents headers ([Kafka events](#kafka-events)).
> - **Topic:** a declared `KafkaTopic`, 6 partitions, key = account, `retention.ms -1`, replication 3 with `min.insync.replicas` 2 in the HA bank. Replicas own partitions, so they share no mutable state.
> - **Four rules** replace the single rule: fast pass-through, chain, new payee and quick repeat, on cross-bank bank transfers ([Detection rule](#detection-rule)). The scorer runs as a library over a bank file for the offline evaluator.
> - **Alert tiers** replace the minimum chain length ([case.md](case.md#cases-and-alerts)).
> - **State in Valkey:** window state, token index, local links, payee sets and hits move out of memory, shared with the privacy node, which stops running the scorer in-process ([Search handoff](#search-handoff)).
> - **Event time:** a per-account watermark with allowed lateness = window; trim by event time, with a TTL only for garbage collection. `--max-events` is removed.
> - **Commits:** `enable.auto.offset.store=false`, `store_offsets()` after the Valkey write, auto-commit on (still at-least-once; duplicates are harmless because `detection_id` is deterministic).
> - **Intra-bank hops** are collapsed into one local link; KEDA scaling as in [Scaling](#scaling).

Scope: bank-local Kafka, the ingestor, token-index population, the scorer's rule windows, KEDA scaling and the local-only baseline.

## Kafka events

| Item | Decision |
| --- | --- |
| Topic names | `bank-A-transactions`, `bank-B-transactions`, `bank-C-transactions` |
| Event identity / dedup key | `(payment_id, direction, account)` within one bank; scorer deduplication lasts only while the event is retained. **pending work:** the `ce_id` header, `<payment_id>-<direction>`, names the same event; a batch the ingestor resends can write an event twice, so consumers drop repeats by it |
| Record value (**draft**) | **pending work:** JSON with nine fields: the eight in [data.md](data.md#bank-event-files-bank_abccsv) plus `counterparty_account`, the other side's account (never sent to peers). `amount` is a number in SGD; every other field is a nonempty string. The shared sample is [`kafka_events.json`](../../apps/ingestor/fixtures/kafka_events.json), the four records the ingestor writes for the batch in [data.md](data.md#ingestor) |
| Record headers (**draft**) | **pending work:** CloudEvents in Kafka binary mode ([spec](https://github.com/cloudevents/spec/blob/main/cloudevents/bindings/kafka-protocol-binding.md)): `ce_specversion` `1.0`, `ce_id` `<payment_id>-<direction>`, `ce_source` `/bank/<X>/ingestor`, `ce_type` `payment.out` or `payment.in`, `ce_time` (the payment's `event_time`), `content-type` `application/json`. The Kafka record timestamp is when the ingestor produced it, which latency is measured from ([evaluation.md](../evaluation.md#metric-definitions)) |
| Intra-bank transfers (**draft**) | **pending work:** two records, `out` then `in`, with the same `payment_id`: `p02` (B-bob → B-carol) is key `B-bob`, `ce_id` `p02-out`, then key `B-carol`, `ce_id` `p02-in` |
| Partitioning key and ordering guarantee | Local `account`; preserves partition arrival order for that account, not event-time order. One partition per bank preserves global replay order when the shared history cap matters. **pending work:** 6 partitions; the event-time window absorbs interleaving, so global order no longer matters |
| Replay, late and out-of-order events | Match either arrival order within each account's event-time window; ignore events older than the latest timestamp seen for that account minus `--window`. Events are retained for twice `--window` so an accepted late event still finds its partner |

The [ingestor](data.md#ingestor) (`apps/ingestor/`) validates every event and
produces these records; the scorer only consumes them and trusts their fields. It
still checks the topic and account key. Run it with
`uv run python -m scorer.stream B --bootstrap-servers <brokers> --group <fresh-group>`
(default `localhost:9092`). The k3d Kafka has only an in-cluster listener, so run it
inside the cluster with `kafka-kafka-bootstrap:9092`; a port-forward does not work
because the broker advertises in-cluster addresses. The consumer may start before
the topic exists: non-fatal Kafka errors are logged and retried. A scorer test loads
the shared sample [`kafka_events.json`](../../apps/ingestor/fixtures/kafka_events.json).

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
| Minimum chain length | Replaced by the alert tiers | **pending work:** [case.md](case.md#cases-and-alerts) |
| Hop limit | 4 (`HOP_LIMIT`, up to 5 transfers) | Shared with [protocol.md](protocol.md); checked by a 4/6/8 sweep |
| Branching cap | 1 (`BRANCH_CAP`): linear chains | The new payee rule flags the payments of fans instead |
| Fees, currencies, insufficient history | Uses Role 1's data, all amounts in SGD ([data.md](data.md#preparation-rules-preparepy)); no separate fee model. Missing/evicted history produces no local match | **pending work:** history bounded by the event-time trim, not a count |

**pending work (Cindy):** four rules, at every bank, on **cross-bank bank transfers**: `GIRO` and `MEPS+` (Altman's ACH and Wire, relabelled in [data.md](data.md)). Cheque, card, cash and Bitcoin payments are never flagged. Starting thresholds come from the exploratory sweep below and are confirmed on the dev split ([evaluation.md](../evaluation.md#rules)). No regulator publishes numeric thresholds, so each rule cites a red flag and its numbers come from tuning.

| Rule | Starting thresholds | Basis | Output |
| --- | --- | --- | --- |
| Fast pass-through | out/in 0.90–0.98 within 1–30 min (today's rule) | Guidelines to MAS Notice 626 B-2(iii), "withdrawn immediately after being deposited" | Local link; starts a search |
| Chain (slow pass-through) | Bank transfer in and out of one account, out/in 0.90–1.10 within 72 h; at least one of the two payments is with another bank | B-4(i) "high velocity of funds", B-5(iv) "within a short period"; Altman's 90th-percentile hop delay is 69.7 h | Local link; starts a search |
| New payee | A bank transfer to an account at another bank that this customer has never paid | TODO (Cindy): cite the red flag. Covers fan-in, fan-out, scatter/gather and bipartite patterns, which create first-time payer–payee pairs | Hit |
| Quick repeat | Another bank transfer to the same payee at another bank within 24 h of a flagged payment to it | TODO (Cindy): cite the red flag | Hit |

- Chain, new payee and quick repeat each write a hit; how hits become cases and alerts is in [case.md](case.md#cases-and-alerts). The fast rule writes no hit (on Altman's dev days it found 1 laundering link in 4,278), but it stays because it's the cited "immediately" flag and the planted acceptance chains depend on it.
- Bank transfers carry 87% of Altman's laundering but 12% of transfers, a dataset artefact; the payment-type filter is reported as dev-tuned. Cheque, card and cash laundering, mostly from bank-70 hub accounts whose laundering payments look like their ordinary ones, is out of reach of any rule.
- **Exploratory sweep** (Macarius, 31 Oct, a scratchpad script, not the offline evaluator; amounts in SGD, three banks, every hit counted as caught):
  - **Chain band 0.90–1.10:** laundering forwards about the same amount (median 1.00). Compared with 0.50–1.50 it cut this rule's ordinary flags by about 80% (3,272 to 664 a day on HI-Small's held-out days) for 1.1 points of recall.
  - **New payee instead of fan-in and fan-out:** the fan rules (≥ 5 counterparties in 72 h) caught 8–56% of the fan, scatter/gather and bipartite attempts; a first-payment-to-a-new-payee rule caught 67–81%. An exception for payees with ≥ 30 earlier payers (businesses) was dropped: only the payee's bank sees all of a payee's payers, and counting this bank's own customers removed 16 ordinary flags a day.
  - **Quick repeat:** on HI-Small's held-out days, 71% of the repeat laundering payments to a just-flagged payee came within 24 h (61% in the same minute), against 3% of ordinary repeats, which are mostly weekly (median 166 h). It added 1.5 points of recall (21 of 1,410) for about 70 ordinary flags a day.
  - **No amount floor:** a S$1,500 floor on both payments of a link cut CYCLE and RANDOM attempts caught from 68 to 63 of 72.
  - **Together:** 95.2% of cross-bank laundering bank transfers on HI-Small's held-out days (1,343 of 1,410) with 1.53% of all payments flagged as ordinary, and 92.5% on HI-Medium (16,450 of 17,777, 2.17% flagged) with the thresholds unchanged ([evaluation.md](../evaluation.md#results)).
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

Optional `TokenIndex` population runs on cross-bank events before
scoring, including unflagged and scorer-too-late events. It uses the original
`payment_id` and actual `counterparty_bank` with Role 4's `canonical_pair`,
`derive_token` and `load_pairwise_keys`; same-bank events are skipped. See
[security.md](security.md) for the token and secret-file contract.

Enable with `--key-version k1` and repeated `--pairwise-key PEER PATH` options on
`python -m scorer.stream`; omitting both keeps scorer-only operation. Missing/invalid
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

### Valkey layout (**draft**, Cindy → Zane)

**pending work:** the scorer writes to Valkey and the privacy node reads from it, so they run in separate pods. One Valkey per bank, so keys carry no bank prefix. Only the scorer and the privacy node connect ([security.md](security.md#mtls-and-networkpolicy)). Client: [valkey-py](https://github.com/valkey-io/valkey-py).

| Key | Type | Value | Read by |
| --- | --- | --- | --- |
| `win:<account>` | sorted set | One member per GIRO/MEPS+ event on this account: compact JSON (sorted keys) of `payment_id`, `direction`, `amount`, `payment_format`, `counterparty_bank`, `counterparty_account`. Score = `event_time` in Unix seconds | Scorer |
| `pay:<payment_id>:<direction>` | hash | `account`, `amount`, `event_time`, `counterparty_bank`, and `token` (hex) for cross-bank events. Every event | Node: own legs, the token for a payment, which bank to ask next |
| `tok:<pair>:<key_version>:<token>` | hash | `payment_id`, `direction`, `account`, `amount`. Cross-bank events only; `<token>` is hex | Node: peer lookups |
| `link:in:<payment_id>` / `link:out:<payment_id>` | set | Outgoing payments a fast or chain rule linked to this incoming one, and the reverse | Node: onward and backward links |
| `hit:<hit_id>` | hash | `rule` (`chain`, `new_payee`, `quick_repeat`; the fast rule writes no hit), `account`, `payments` (JSON list), `event_time` | Node |
| `hits` | stream | One entry `hit <hit_id>` per new hit | Node, with `XREADGROUP` |
| `payees:<account>` | set | Accounts at other banks this customer has paid by bank transfer | Scorer: new payee |
| `payers:<account>` | set | Distinct payers into this account | Scorer: the ≥ 30 cap |
| `flag:<account>:<counterparty_account>` | string | Unix seconds of the last flagged payment between them | Scorer: quick repeat |

- **IDs:** `hit_id` is SHA-256 of the compact JSON `[bank, rule, *payment_ids]`, so a replay rewrites the same key.
- **Watermark and trim:** an account's watermark is the highest score in `win:<account>`, so there's no separate key. Events more than the window behind it are ignored as late. Each write runs `ZREMRANGEBYSCORE win:<account> -inf (<watermark − 2 × 72 h>`, which keeps twice the longest rule window.
- **Repeats:** a resent event (same `ce_id`) builds the same `win:` member, so the scorer skips it when `ZSCORE` finds it. No separate dedup key. Events outside `win:` (cards, cash, cheques) don't feed any rule, and their `pay:`/`tok:` writes are idempotent.
- **One transaction per event:** the scorer reads first, then writes all keys for the event in one `MULTI`/`EXEC`, then calls `store_offsets()`. A crash before `EXEC` writes nothing; after it, the redelivered event is a repeat. So `hits` gets one entry per hit, except after a full rebuild, when the node skips hit IDs it already has.
- **Concurrency:** partition ownership gives `win:`, `payees:` and `flag:` a single writer. `payers:<account>` is keyed by the payee, so several replicas add to it; `SADD` is safe, but whether the ≥ 30 cap applies can depend on processing order across partitions.
- **Intra-bank hops (draft):** links include intra-bank legs, and the node follows them when it answers, until it reaches a cross-bank payment, within `HOP_LIMIT`. The two legs of an internal hop are on different accounts and partitions, so collapsing them when writing would depend on which replica runs first.
- **TTL (garbage collection only):** 7 days, refreshed on write, for every key except `payees:` and `payers:`. It must exceed 2 × 72 h of event time, or a quiet account loses chain state in live running.
- **Loss:** Valkey is derived state. Rebuild it by replaying from `earliest` with a fresh group.

Sample: [`valkey_state.json`](../../apps/scorer/fixtures/valkey_state.json), Bank B's keys after the four records in [`kafka_events.json`](../../apps/ingestor/fixtures/kafka_events.json). `p06` → `p07` on `B-m1` (S$10,000 in from A, S$9,500 out to C 15 minutes later) fires fast and chain (one link, one `chain` hit) and new payee. The card payment `p02` gets only `pay:` keys. Tokens are placeholders until the 16-byte test vector is frozen ([security.md](security.md)).

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
| Kafka transaction event ([streaming.md](streaming.md#kafka-events)) | Macarius → Cindy | Topic, key, JSON fields, CloudEvents headers, two events per intra-bank transfer | **agreed** (Cindy, 3 Nov), sample [`kafka_events.json`](../../apps/ingestor/fixtures/kafka_events.json) | Mon 2 Nov |
| Valkey layout ([streaming.md](streaming.md#search-handoff)) | Cindy → Zane | Key names, token-index value, local links (including collapsed intra-bank links), `hit:` keys, windows, TTL and trim | **draft**, sample [`valkey_state.json`](../../apps/scorer/fixtures/valkey_state.json); Zane to approve | Wed 4 Nov |
| Scorer as a library | Cindy → Isaac | Entry point over one bank file, in-memory state store | **draft**, sample TODO | Wed 4 Nov |

Also part of: [Token and key API](security.md#interfaces), [Alert tiers](case.md#interfaces).

## Open

- [ ] Intra-bank hops: followed by the node when it reads, rather than collapsed by the scorer (Cindy, with Zane).
- [ ] TTL for `tok:` and `pay:`: a peer's query can arrive up to 72 h after the event under the chain rule, longer than the 24 h search retention in [security.md](security.md) (Cindy, with Caleb).
- [ ] Rule thresholds, from the offline sweep on the dev split, starting from the exploratory values above (Cindy, with Isaac).
- [ ] Cite red flags for the new payee and quick repeat rules (Cindy).
- [ ] TTL for the `payees:` sets, which keep the whole history (Cindy, with Caleb for PDPA retention).
