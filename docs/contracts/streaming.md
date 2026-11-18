# Streaming and scoring contract

Owner: Cindy · Partners: Macarius (events), Isaac (search handoff, privacy node), Caleb (tokens), Isaac (scaling, baseline comparison) · Code: `apps/scorer/` · Status: **draft**, needs review by Macarius and Zane

> [!NOTE]
> **Pending work (decided 30 Oct 2026).** The sections below describe what exists today; these items change it:
> - **Topic:** a declared `KafkaTopic`, 6 partitions, key = account, `retention.ms -1`, replication 3 with `min.insync.replicas` 2 in the HA bank. Replicas own partitions, so they share no mutable state.
> - **Alert tiers** replace the minimum chain length ([case.md](case.md#cases-and-alerts)).
> - KEDA scaling as in [Scaling](#scaling).

Scope: bank-local Kafka, the ingestor, token-index population, the scorer's rule windows, KEDA scaling and the local-only baseline.

## Kafka events

| Item | Decision |
| --- | --- |
| Topic names | `bank-A-transactions`, `bank-B-transactions`, `bank-C-transactions` |
| Event identity / dedup key | `(payment_id, direction, account)` within one bank; scorer deduplication lasts only while the event is retained. The `ce_id` header, `<payment_id>-<direction>`, names the same event; a batch the ingestor resends can write an event twice, and the scorer drops the repeat by this key |
| Record value | JSON with the nine fields of a [bank event](data.md#bank-event-files-bank_abccsv), including `counterparty_account`, the other side's account (never sent to peers). `amount` is a number in SGD; every other field is a nonempty string. The shared sample is [`kafka_events.json`](../../apps/ingestor/fixtures/kafka_events.json), the four records the ingestor writes for the batch in [data.md](data.md#ingestor) |
| Record headers | CloudEvents in Kafka binary mode ([spec](https://github.com/cloudevents/spec/blob/main/cloudevents/bindings/kafka-protocol-binding.md)): `ce_specversion` `1.0`, `ce_id` `<payment_id>-<direction>`, `ce_source` `/bank/<X>/ingestor`, `ce_type` `payment.out` or `payment.in`, `ce_time` (the payment's `event_time`), `content-type` `application/json`. The Kafka record timestamp is when the ingestor produced it, the same for a whole batch and logged as its `ts`; latency is measured from it ([evaluation.md](../evaluation.md#metric-definitions)) |
| Intra-bank transfers | Two records with the same `payment_id`: `p02` (B-bob → B-carol) is key `B-bob`, `ce_id` `p02-out`, and key `B-carol`, `ce_id` `p02-in`. The ingestor produces `out` first, but the keys differ, so the records usually land in different partitions and a consumer can read them in either order. Only the bank files keep `out` before `in`; consumers rely on `event_time`, which both records share |
| Partitioning key and ordering guarantee | Local `account`; preserves partition arrival order for that account, not event-time order. **pending work:** 6 partitions; the event-time window absorbs interleaving, so global order doesn't matter |
| Replay, late and out-of-order events | Match either arrival order within each account's event-time window; ignore events more than 72 h (the chain window) behind the latest event seen for that account. Events are kept for twice that, so an accepted late event still finds its partner |

The [ingestor](data.md#ingestor) (`apps/ingestor/`) receives pacs.008 batches over
HTTP, rejects invalid or wrong-bank ones whole, and produces these records; the
scorer only consumes them and trusts their fields. It still checks the topic and
account key. Run the scorer with
`SCORER_BANK=B VALKEY_URL=valkey://valkey:6379 uv run python -m scorer.stream`; the
settings are in [deployment.md](deployment.md#per-service-settings), and a flag overrides
each one (`--help`). The k3d Kafka has only an in-cluster listener, so run it
inside the cluster with `kafka-kafka-bootstrap:9092`; a port-forward does not work
because the broker advertises in-cluster addresses. The consumer may start before
the topic exists: non-fatal Kafka errors are logged and retried. A scorer test loads
the shared sample [`kafka_events.json`](../../apps/ingestor/fixtures/kafka_events.json).

The live scorer keeps its state in the bank's Valkey (`ValkeyState`, [Valkey layout](#valkey-layout-draft-cindy--zane));
tests and the offline evaluator use a `MemoryState` with the same methods
([Scorer as a library](#scorer-as-a-library-draft-cindy--isaac)). The window is trimmed by
event time per account, with no count cap; an account's watermark is its latest kept
event. In a `MemoryState` the other entries (`pay`, links, hits, `last`) last for the
life of the process, about 536 MB for Bank B over all of `altman-small`; in Valkey they
expire with the TTL.

Each event's writes go to Valkey in one `MULTI`/`EXEC`; then `store_offsets()` stores
its offset, and auto-commit commits it in the background (`enable.auto.offset.store`
false). This is at-least-once: an event redelivered after a crash is found in its
account's window and skipped, so it writes nothing twice. A restart or partition
handover continues from the committed offsets with no replay, because the state is in
Valkey; replicas share no mutable state because each account belongs to one partition.
The consumer prefetches at most 16 MiB (`queued.max.messages.kbytes`). A wrongly routed
record or a fatal Kafka error stops the scorer without storing that offset. SIGTERM
and SIGINT stop it between events; closing the consumer commits the stored offsets.

Each event that writes a link or a hit logs one JSON line to stdout, for latency:
`{"ts": 1793500001.2, "service": "scorer", "bank": "B", "payment_id": "p07", "kafka_ts": 1793500000.123, "links": [["p06", "p07"]], "hits": ["chain", "new_payee"]}`.
`kafka_ts` is the record's Kafka timestamp, set when the ingestor wrote it, and `ts` is
when the scorer's Valkey write finished. Errors are logged as `level: error` lines.

The privacy node still runs the older in-memory path in-process (`consume()` with
`require_replay`): there, a restart needs a full replay with a fresh group, offsets
commit synchronously, and assignment rejects resumed offsets or several partitions.
That path goes once the node reads Valkey.

## Detection rule

The rule indicates suspicion, not proven laundering. Start with a single-currency linear-chain fixture, and label unsupported cases explicitly.

| Parameter | Value | Notes |
| --- | --- | --- |
| Incoming-to-outgoing value ratio | Outgoing / incoming: 0.90–0.98 inclusive | `--min-ratio`, `--max-ratio`; draft defaults pending team approval |
| Time window | 1–30 minutes inclusive, using transaction timestamps | `--min-delay` (60 seconds), `--window` (1800 seconds) |
| Minimum chain length | Replaced by the alert tiers | **pending work:** [case.md](case.md#cases-and-alerts) |
| Hop limit | 4 (`HOP_LIMIT`, up to 5 transfers) | Shared with [protocol.md](protocol.md); checked by a 4/6/8 sweep |
| Branching cap | 1 (`BRANCH_CAP`): linear chains | The new payee rule flags the payments of fans instead |
| Fees, currencies, insufficient history | Uses Role 1's data, all amounts in SGD ([data.md](data.md#preparation-rules-preparepy)); no separate fee model. Missing or trimmed history produces no local match | History is bounded by the event-time trim, not a count |

Four rules, at every bank, on **cross-bank bank transfers**: `GIRO` and `MEPS+` (Altman's ACH and Wire, relabelled in [data.md](data.md)). Cheque, card, cash and Bitcoin payments are never replayed or ingested ([data.md](data.md#preparation-rules-preparepy)), so the scorer never sees them, and a customer's payment history (new payee, quick repeat) is bank transfers only. Starting thresholds come from the exploratory sweep below and are confirmed on the dev split ([evaluation.md](../evaluation.md#rules)). No regulator publishes numeric thresholds, so each rule cites a red flag and its numbers come from tuning.

| Rule | Starting thresholds | Basis | Output |
| --- | --- | --- | --- |
| Fast pass-through | out/in 0.90–0.98 within 1–30 min (today's rule) | Guidelines to MAS Notice 626 B-2(iii), "withdrawn immediately after being deposited" | Local link; starts a search |
| Chain (slow pass-through) | Bank transfer in and out of one account, out/in 0.90–1.10 within 72 h; at least one of the two payments is with another bank | B-4(i) "high velocity of funds", B-5(iv) "within a short period"; Altman's 90th-percentile hop delay is 69.7 h | Local link; starts a search |
| New payee | A bank transfer to an account at another bank that this customer has never paid | TODO (Cindy): cite the red flag. Covers fan-in, fan-out, scatter/gather and bipartite patterns, which create first-time payer–payee pairs | Hit |
| Quick repeat | Another bank transfer to the same payee at another bank within 24 h of a flagged payment to it | TODO (Cindy): cite the red flag | Hit |

- Chain, new payee and quick repeat each write a hit; how hits become cases and alerts is in [case.md](case.md#cases-and-alerts). The fast rule writes no hit (on Altman's dev days it found 1 laundering link in 4,278), but it stays because it's the cited "immediately" flag and the planted acceptance chains depend on it.
- Bank transfers carry 87% of Altman's laundering but 12% of transfers, a dataset artefact; the payment-type filter is reported as dev-tuned. Cheque, card and cash laundering, mostly from bank-70 hub accounts whose laundering payments look like their ordinary ones, is out of reach of any rule.
- **Exploratory sweep** (Macarius, 31 Oct, re-run 7 Nov on the offline evaluator's definitions; a scratchpad script, not the offline evaluator; amounts in SGD, three banks, bank-transfer history, every hit counted as caught; comparisons on HI-Small's dev split):
  - **Chain band 0.90–1.10:** laundering forwards about the same amount (median 1.00). Compared with 0.50–1.50 it cut this rule's ordinary flags by about 80% (3,877 to 796 a day) for 0.7 points of recall.
  - **New payee instead of fan-in and fan-out:** the fan rules (≥ 5 counterparties in 72 h) caught 4–78% of the fan, scatter/gather and bipartite attempts; a first-payment-to-a-new-payee rule caught 83–89%. An exception for payees with ≥ 30 earlier payers (businesses) was dropped: only the payee's bank sees all of a payee's payers, and counting this bank's own customers removed 12 ordinary flags a day.
  - **Quick repeat:** 85% of the 126 repeat laundering payments to a just-flagged payee came within 24 h (67% in the same minute). Dev's five days start with no history, so its ordinary repeats can't include weekly ones: 63% came within 24 h, falling from 55% on 2 Sep to 26% on 5 Sep. It added 1.1 points of recall (16 of 1,471) for 4,836 ordinary flags a day, 99% of them on 1–2 Sep, when every payee is new; on 3–5 Sep it added about 105 a day.
  - **Amount floor:** a S$1,500 floor on both payments of a link caught the same CYCLE and RANDOM attempts (42 of 42) and lost 2 laundering payments (1,439 of 1,471), while removing 352 of the chain rule's 796 ordinary flags a day (6.33% to 6.28% of payments flagged). The starting point stays no floor; experiment 3 tunes it ([evaluation.md](../evaluation.md#experiments)).
  - **Together:** 98.0% of dev's cross-bank laundering bank transfers (1,441 of 1,471) with 6.33% of all payments flagged as ordinary, 93.7% on eval (963 of 1,028, 1.54% flagged), and 92.6% on HI-Medium (16,461 of 17,777, 2.98% flagged) with the thresholds unchanged ([evaluation.md](../evaluation.md#results)).
- **Intra-bank hops:** the scorer writes a link for each account as usual, internal legs included, and the node follows them through the bank until it reaches a cross-bank payment, so it still signs one claim ([protocol.md](protocol.md#searches-from-valkey)). Where the money starts or stays inside the bank, the node posts the internal path as a `local` message to the hit's Tier 1 case; a chain with no cross-bank payment raises no chain hit and is left to the bank's own monitoring.

The scorer reports every qualifying incoming/outgoing pair for the same local
account; it does not allocate incoming funds across payments or perform graph
search. Ground-truth labels are never used. JSONL output contains local candidate
links, not confirmed chains or analyst alerts: bank, account, incoming/outgoing
payment IDs and times, ratio, delay in seconds, `detection_id` and `rules` (`fast`,
`chain` or both). Rule hits (chain, new payee, quick repeat) go to the state, not the output.

Quick repeat depends on arrival order. In file order it's exact. Live, an account's
events share a partition and replay runs in time order, so an in leg normally arrives
before its out leg. If it doesn't, the link and `chain` hit are still written, but that
out leg doesn't count as flagged for a later quick repeat. A late, older payment never
replaces a newer `last:` entry and is never a quick repeat.

`detection_id` is SHA-256 of the UTF-8 compact JSON array
`[bank, incoming_payment_id, outgoing_payment_id]`, with default JSON escaping.
Replaying the same pair produces the same ID. This output stays bank-local;
it is not an approved peer payload.

## Token index

The live scorer writes a token for every cross-bank bank transfer it sees, flagged or
not, and late or repeated ones too: a `tok:<pair>:<version>:<token>` entry for every
active key version, and the query version's token in the payment's `pay:` entry
([Valkey layout](#valkey-layout-draft-cindy--zane)). Same-bank events get no token.
Tokens come from Role 4's `TokenIndex.tokens()` (`canonical_pair`, `derive_token`,
pair keys from X25519 files); see [security.md](security.md) for the token and
key-file contract. A peer looks a token up with `lookup(pair, version, token)`, and
lookup never crosses versions. The entries stay in this bank's Valkey; Role 3 must not
send them as peer responses.

Keys come from `KEY_DIR` (or `--key-dir`): `<KEY_DIR>/<bank>/active.json` and the
X25519 files in the security.md layout; the bank's own public key under `trusted/` is
skipped. The older flags `--key-version`, `--x25519-private` and repeated
`--peer-public-key PEER PATH` load one version from explicit files, and override
`KEY_DIR`. Without either, no tokens are written.

With `KEY_DIR`, the scorer re-reads `active.json` every 10 s between events and follows
the rotation steps in [security.md](security.md#versions-rotation-and-reload):

- **Introduce:** a newly listed version is backfilled, so every `pay:` entry with
  another bank gets a `tok:` key under it.
- **Switch:** the query version (the `pay:` entry's `token`) changes only once every
  peer lists it, and every `pay:` entry is rewritten to it.
- **Retire:** a version no longer listed retires, and its `tok:` keys expire with the
  TTL. This step writes nothing, so it does no backfill pass. Switch and retire in
  separate steps, as security.md requires.

Every check runs before the index changes, so a missing key file or a half-written
`active.json` is logged as an error and the old keys stay in use. What counts as new is
worked out from the config last applied, so a reload retried after a backfill stopped
part-way does the whole backfill again. A pod that restarts while more than one
version is active redoes the backfill at start, in case the last one didn't finish; a
normal start with one version costs nothing.

Every replica runs each backfill pass over every `pay:` entry, and this is needed, not
just repeated work: each replica's pass covers the payments other replicas wrote under
the old config in the up to 10 s before they reloaded, which would otherwise miss the
new version's token or keep the old one. Don't trim the backfill down to one replica.

A pass pauses scoring on that replica while it runs. Reads go 1,000 `HGETALL`s per round
trip and writes 1,000 entries per `MULTI`/`EXEC`. A bank holds roughly 350,000–420,000
`pay:` entries after replaying all of `altman-small` (1,156,694 events over three banks,
[data.md](data.md#replay-replaypy)), and a pass over 100,000 entries took 2.5 s to introduce k2 and 2.4 s to switch
(Valkey 9.1.3 in Docker on a laptop, loopback; retiring took no time). That is about
10 s per pass for a whole bank, which will show as lag in experiment 12 and stays well
under librdkafka's `max.poll.interval.ms` (300 s).

The privacy node's in-process path still keeps its own in-memory `TokenIndex`
(`add()`/`lookup()` with references, `--max-token-references` as a fail-stop bound);
that goes once the node reads Valkey.

## Search handoff

Integrated mode runs Role 2's consumer inside the privacy-node process. Use
`uv run python -m privacy_node.node --bank B --bootstrap-servers <brokers>
--group <fresh-group> --search` with `--key-directory ROOT` and the TLS
and peer options from [protocol.md](protocol.md). `--workload` selects the existing
standalone CSV mode instead. `--valkey-url` is the deployed shape: the node reads the scorer's state from Valkey and never consumes Kafka ([protocol.md](protocol.md#searches-from-valkey)). The three input modes are mutually exclusive. The rest of this section is about integrated mode.

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

The scorer writes to Valkey (`ValkeyState` in `scorer/state.py`) and the privacy node reads from it (`privacy_node/valkey_bank.py`), so they run in separate pods. One Valkey per bank, so keys carry no bank prefix. Only the scorer and the privacy node connect ([security.md](security.md#mtls-and-networkpolicy)). Client: [valkey-py](https://github.com/valkey-io/valkey-py).

| Key | Type | Value | Read by |
| --- | --- | --- | --- |
| `win:<account>` | sorted set | One member per GIRO/MEPS+ event on this account: compact JSON (sorted keys) of `payment_id`, `direction`, `event_time`, `amount`, `payment_format`, `counterparty_bank`, `counterparty_account`. Score = `event_time` in Unix seconds | Scorer |
| `pay:<payment_id>:<direction>` | hash | `account`, `amount`, `event_time`, `counterparty_bank`, and `token` (hex) for cross-bank events. Every event | Node: own legs, the token for a payment, which bank to ask next |
| `tok:<pair>:<key_version>:<token>` | hash | `payment_id`, `direction`, `account`, `amount`. Cross-bank events only; `<token>` is hex | Node: peer lookups |
| `link:in:<payment_id>` / `link:out:<payment_id>` | set | Outgoing payments a fast or chain rule linked to this incoming one, and the reverse | Node: onward and backward links |
| `hit:<hit_id>` | hash | `rule` (`chain`, `new_payee`, `quick_repeat`; the fast rule writes no hit), `account`, `payments` (JSON list) | Node |
| `hits` | stream | One entry `hit <hit_id>` per new hit | Node, with `XREADGROUP` |
| `last:<account>:<counterparty_account>` | string | `<unix seconds>:<0 or 1>`: this customer's last payment to that payee, and whether it was flagged (a new payee hit or a chain's outgoing leg). None yet means a new payee | Scorer: new payee, quick repeat |

- **IDs:** `hit_id` is SHA-256 of the compact JSON `[bank, rule, *payment_ids]`, so a replay rewrites the same key.
- **Watermark and trim:** an account's watermark is the highest score in `win:<account>`, so there's no separate key. Events more than 72 h (the longest rule window, the chain rule's) behind it are ignored as late. Each write runs `ZREMRANGEBYSCORE win:<account> -inf (<watermark − 2 × 72 h>`, which keeps twice the longest rule window.
- **Repeats:** a resent event (same `ce_id`) builds the same `win:` member, so the scorer finds it in the window and skips it. No separate dedup key. `pay:`/`tok:` writes are idempotent.
- **One transaction per event:** the scorer reads first, then writes all keys for the event in one `MULTI`/`EXEC`, then calls `store_offsets()`. A crash before `EXEC` writes nothing; after it, the redelivered event is a repeat. So `hits` gets one entry per hit, except after a full rebuild, when the node skips hit IDs it already has.
- **Concurrency:** every key is per account or per payment, so partition ownership gives each one writer and replicas share no mutable state.
- **Intra-bank hops (draft):** links include intra-bank legs, and the node follows them when it answers, until it reaches a cross-bank payment, within `HOP_LIMIT`. The two legs of an internal hop are on different accounts and partitions, so collapsing them when writing would depend on which replica runs first.
- **TTL (garbage collection only):** 7 days, refreshed on write, for every key except `last:`. It must exceed 2 × 72 h of event time, or a quiet account loses chain state in live running.
- **Loss:** Valkey is derived state. Rebuild it by replaying from `earliest` with a fresh group into an empty Valkey. Over existing state a fresh group rewrites nothing, because every event is found in its window or is more than 72 h behind the watermark.

Sample: [`valkey_state.json`](../../apps/scorer/fixtures/valkey_state.json), Bank B's keys after the four records in [`kafka_events.json`](../../apps/ingestor/fixtures/kafka_events.json). `p06` → `p07` on `B-m1` (S$10,000 in from A, S$9,500 out to C 15 minutes later) fires fast and chain (one link, one `chain` hit) and new payee. The intra-bank GIRO payment `p02` (B-bob → B-carol) writes window, `pay:` and `last:` keys but no hit. Tokens are placeholders, since they depend on each bank's X25519 keys; `test_valkey.py` checks real tokens with generated keys, and the rest of the sample key by key.

Optional `--ingestion-grace-seconds N` treats a valid origin-side `NO_MATCH` as
`PENDING` until a fixed deadline from that search's first attempt. Retries use
fresh query IDs; peer replies, claim verification and message schemas are
unchanged. After the deadline, an absent record becomes final `NO_MATCH` and
missing-record retries stop. Default `0` preserves existing behavior; enabling
it and choosing a duration still require team agreement. The grace runs from
the first attempt's `started` time in the `searches` table, so it survives
restarts and is shared by replicas. Unavailable-peer retries follow
[protocol.md](protocol.md)'s retry policy. This does not resolve a peer mistaking an ingested
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
`--min-delay` and `--window` (the fast rule).

Verified tiny-fixture local pairs (stable IDs reproduce on replay):

| Bank | Detections | Incoming → outgoing |
| --- | --- | --- |
| A | 1 | p17 → p18 |
| B | 4 | p06 → p07; p10 → p11; p15 → p16; p18 → p19 |
| C | 3 | p11 → p12; p16 → p17; p19 → p20 |

**pending work:** the offline evaluator ([evaluation.md](../evaluation.md#offline-detection-evaluation)) supersedes this as the local-only vs cross-bank comparison, running the same scorer code.

## Scorer as a library (**draft**, Cindy → Isaac)

The offline evaluator runs the live rule code over bank files, with no Kafka or Valkey.

```python
from scorer import score_file

state = score_file("apps/dataset/fixtures/tiny", "B")
```

- `score_file(workload, bank)` reads `<workload>/bank_<bank>.csv` in file order, runs every rule (they skip anything but `GIRO` and `MEPS+`, as replay does) and returns a `MemoryState`. It writes no tokens; the node's CSV mode indexes its own.
- `MemoryState` (`scorer/state.py`) holds the same entries as the [Valkey layout](#valkey-layout-draft-cindy--zane), in dicts, and the live `ValkeyState` has the same methods, so the rules are one piece of code. `test_valkey.py` runs the tiny fixture through both and checks they agree. The node and the evaluator read it with:
  - `links(payment_id, direction)`: the `link:in:` or `link:out:` payment IDs, sorted
  - `payment(payment_id, direction)`: the `pay:` entry, or `None`
  - `lookup(pair, key_version, token)`: the `tok:` entry, or `None` (live only: the scorer writes tokens when it has key files)
  - `hits()`: every hit as `{rule, account, payments, hit_id}`, in the order written
- Sample: [`tiny_expected.json`](../../apps/scorer/fixtures/tiny_expected.json), every bank's links and hits on the tiny fixture. It matches issue #27's fixture table and was generated from #27's reference script (chain, new payee, quick repeat) plus the fast rule; `test_score_file_matches_library_sample` checks the scorer against it. The fixture has no quick repeat.

## Interfaces

Agreed contract first ([conventions](../README.md#conventions)): the producer adds one sample message here, and both sides' tests load it.

| Interface | From → to | Pins down | Status | Agree by |
| --- | --- | --- | --- | --- |
| Kafka transaction event ([streaming.md](streaming.md#kafka-events)) | Macarius → Cindy | Topic, key, JSON fields, CloudEvents headers, two events per intra-bank transfer | **agreed** (Cindy, 3 Nov), sample [`kafka_events.json`](../../apps/ingestor/fixtures/kafka_events.json) | Mon 2 Nov |
| Valkey layout ([streaming.md](streaming.md#valkey-layout-draft-cindy--zane)) | Cindy → Isaac (privacy node) | Key names, token-index value, local links (including intra-bank legs), `hit:` keys, windows, TTL and trim | **agreed** (Isaac, 18 Nov; the node reads it), sample [`valkey_state.json`](../../apps/scorer/fixtures/valkey_state.json) | Wed 4 Nov |
| Scorer as a library ([streaming.md](streaming.md#scorer-as-a-library-draft-cindy--isaac)) | Cindy → Isaac | Entry point over one bank file, in-memory state store | **draft**, sample [`tiny_expected.json`](../../apps/scorer/fixtures/tiny_expected.json); Isaac to approve | Wed 4 Nov |

Also part of: [Token and key API](security.md#interfaces), [Alert tiers](case.md#interfaces).

## Open

- [x] Intra-bank hops: followed by the node when it reads, rather than collapsed by the scorer.
- [ ] TTL for `tok:` and `pay:`: a peer's query can arrive up to 72 h after the event under the chain rule, longer than the 24 h pending-search expiry in [protocol.md](protocol.md) (Cindy, with Caleb for PDPA retention).
- [ ] Rule thresholds, from the offline sweep on the dev split, starting from the exploratory values above (Cindy, with Isaac).
- [ ] Cite red flags for the new payee and quick repeat rules (Cindy).
- [ ] TTL for the `last:` keys, which keep the whole history (Cindy, with Caleb for PDPA retention).
