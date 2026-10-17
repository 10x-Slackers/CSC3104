# Data contract (Role 1, draft for M1)

Owner: Role 1 · Partners: Role 2 (events, rule), Role 6 (evaluation) · Code: `apps/dataset/`

Items marked **draft** need agreement from the partner role before M1 (9 Oct).

## Source

IBM AML synthetic dataset (Altman et al., NeurIPS 2023), Kaggle `ealtman2019/ibm-transactions-for-anti-money-laundering-aml`, **version 8 pinned**, file `HI-Small_Trans.csv`.

| HI-Small fact | Value |
|---|---|
| Transfers | 5,078,345 (1–18 Sep 2022, almost all 1–10 Sep) |
| Altman laundering | 5,177 (1 in 981) |
| Timestamp resolution | minute; file is **not** time-sorted |
| Transaction ID | **none** → we generate `payment_id` |
| Bank IDs | zero-padding is inconsistent (`010` = `0010`): 30,528 spellings, 30,470 banks |
| Account IDs | unique only per source bank (8 IDs reused across banks) |
| Both sides USD | 36.6 % · cross-currency 1.4 % |
| Self-transfers | 591,212 (481,056 `Reinvestment`) |

## Preparation rules (`prepare.py`)

1. Keep transfers where **both currencies are US Dollar** and **sender ≠ receiver account**. This leaves 1,668,941 transfers (1,912 Altman laundering). Other currencies are unsupported and dropped.
2. **Bank mapping:** `int(source bank) % 3` → `0=A, 1=B, 2=C`. The event split is A 30.5 %, B 35.9 %, C 33.7 %, and 66.6 % of transfers cross banks.
3. **Account:** `<int source bank>-<account hex>`, e.g. `3208-8000F4580`.
4. **`payment_id`:** the first 16 hex characters of `sha256("altman:<source row>")`. The same transfer keeps the same ID in every slice. Planted transfers use `sha256("<chain_id>:<hop>")`, which has the same format, so planted IDs can't be told apart.
5. **Time:** Altman time is treated as UTC and written as ISO 8601 (`2022-09-05T09:20:00Z`). Planted times are also whole minutes.
6. Each transfer becomes **two events with the same `payment_id`**: `out` at the sender's bank and `in` at the receiver's bank. Each bank file is sorted by `(event_time, payment_id)`, and an intra-bank `out` comes before its `in`.

## Bank event files: `bank_{A,B,C}.csv`

| Column | Example | Notes |
|---|---|---|
| `payment_id` | `008f4681d368aece` | shared by both endpoints; the HMAC input for Role 4 |
| `event_time` | `2022-09-05T00:00:00Z` | event time, not replay time |
| `bank` | `A` | this bank |
| `direction` | `out` / `in` | |
| `account` | `22164-801F75F50` | this bank's customer |
| `counterparty_bank` | `B` | which bank to ask next; the other bank's customer account is **not** included (minimal data: peers find their own account from the shared token) |
| `amount` | `360542.13` | USD |
| `payment_format` | `Cheque` | ACH, Cheque, Credit Card, Cash, Wire, Bitcoin |

**Replay output:** one JSON object per event row, one per line (`amount` as a number), in event-time order. Role 2's ingestor publishes these to the bank-local Kafka; topic, key and partitioning are Role 2's.

## Labels: `labels.csv` (evaluation only)

These are read only by the evaluation harness. They never go into bank files, Kafka or peer messages. Any `payment_id` not listed is ordinary activity.

| Column | Meaning |
|---|---|
| `payment_id` | the transfer |
| `altman_laundering` | `1` = Altman's own label. Report these separately; they are not planted chains (**draft**, Role 6) |
| `chain_id`, `hop` | planted chain and position (0 = first transfer) |
| `scenario` | see below |

## Planted scenarios

Planted chains run between existing accounts in the slice, start in the first half of the slice, and every hop crosses banks.

| Scenario | Shape | Expected system result |
|---|---|---|
| `mule_chain` | 2–4 hops, each forwards 90–98 % within 1–30 min | confirmed chain; one alert at each involved bank |
| `lookalike` | same shape but forwards only 20–50 % | no confirmed link, no alert |
| `loop` | 3 hops returning to the first account (A→B→C→A) | search terminates (visited tokens); one alert per bank, not two at the start bank |
| `absent_link` | 3 hops with hop 1 removed | search stops at the gap; hop 2 is never linked to hop 0 |
| `over_limit` | hop limit + 2 = 6 hops | search truncated at the hop limit (partial) |

**Draft parameters** (`prepare.py` constants, to agree with Roles 2 and 3): `HOP_LIMIT = 4`, mules forward `0.90–0.98`, delay `1–30 min`, starting amount USD 5,000–50,000, format ACH or Wire.

## Workloads

All workloads are reproducible: the dataset version is pinned and the seed is fixed. Each one has a `workload.json` recording `t0`, its source, its parameters and its counts.

| Workload | Command (`uv run apps/dataset/prepare.py …`) | Size |
|---|---|---|
| `fixtures/tiny` (committed) | `apps/dataset/fixtures/tiny --transfers apps/dataset/fixtures/tiny/transfers.csv` | 20 transfers |
| `dev-small` | `apps/dataset/out/dev-small --start 2022-09-05 --hours 6 --seed 1` | 48,668 transfers, 50 chains |
| `eval-small` | `apps/dataset/out/eval-small --start 2022-09-07 --hours 6 --seed 2` | 47,988 transfers, 50 chains |
| `dev-medium` | `apps/dataset/out/dev-medium --start 2022-09-05 --hours 48 --seed 1` | about 350k transfers |
| `eval-medium` | `apps/dataset/out/eval-medium --start 2022-09-07 --hours 48 --seed 2` | about 350k transfers |

**Held out:** dev uses 5–6 Sep with seed 1; eval uses 7–8 Sep with seed 2. Tune rules on dev only.

## Fixture expected results (`apps/dataset/fixtures/tiny`)

| Chain | Transfers | Banks | Expected |
|---|---|---|---|
| — | p01–p04 | mixed, p02 intra-bank B | ordinary, no alert |
| — | p05 | B→C | `altman_laundering = 1`, not a planted chain |
| `m1` | p06 → p07 | A→B→C | confirmed; alerts at A, B, C |
| `l1` | p08 → p09 | A→B→C | 40 % forwarded → no alert |
| `o1` | p10 → p11 → p12 | A→B→C→A | confirmed, terminates; one alert each at A, B, C |
| `x1` | p13, ~~hop 1~~, p14 | A→B, C→A | no-match at B; p14 not linked |
| `v1` | p15 → … → p20 | A→B→C→A→B→C→A | partial, cut at hop 4 |

For Role 4's two-bank token test, `p01`, `p06`, `p08`, `p10` and `p13` all appear in both `bank_A.csv` and `bank_B.csv`.

## Replay (`replay.py`)

Run one replay per bank, each reading only its own file and printing its events for that bank's ingestor (Role 2). Pass the same `--start-at` to all three so the banks stay in step: an event is printed at `start_at + (event_time − t0) / speed`.

```sh
uv run apps/dataset/replay.py apps/dataset/fixtures/tiny A --speed 0   # no pacing
uv run apps/dataset/replay.py apps/dataset/out/dev-small A --speed 60 --start-at <epoch> | <Role 2 ingestor>
```

At `--speed 60`, the 6 h `small` workload takes 6 min. `--speed 0` sends as fast as possible, for load tests.

## Role 2 local scoring (draft thresholds)

`uv run apps/scorer/baseline.py apps/dataset/fixtures/tiny B` exports local
candidate links as JSONL (no labels or cross-bank confirmation). Defaults are
inclusive: outgoing/incoming ratio 0.90–0.98, delay 60–1800 seconds. Override
with `--min-ratio`, `--max-ratio`, `--min-delay`, `--window`, `--max-events`.
All qualifying pairs are reported; incoming funds are not allocated across payments.
Each result includes a stable `detection_id`: SHA-256 of the compact JSON array
`[bank, incoming_payment_id, outgoing_payment_id]` (UTF-8, default JSON escaping).
Complete replay reproduces these IDs so downstream consumers can deduplicate.
History/deduplication retain at most 10,000 events globally, within 30 minutes of
each account's greatest timestamp; late events inside that horizon match in either
arrival order, older events are ignored. Capacity eviction removes the oldest
arrival and forgets an account's timestamp when none of its events remain.

Pipe replay into `uv run apps/scorer/stream.py ingest B`; separately run
`uv run apps/scorer/stream.py consume B`. Both accept `--bootstrap-servers`.
Topics are `bank-<bank>-transactions`, keys are local accounts. Invalid/wrong-bank
events stop processing; consumer offsets commit synchronously after output flush.
Producer queue backpressure fails after 30 seconds per event; final flush is
separately bounded at 30 seconds.
State is in memory: use a fresh `--group` to replay from the beginning after
restart or reassignment (output can repeat). Output and offsets are not atomic.
`auto.offset.reset=earliest` does not rewind an existing group's committed offsets;
complete replay needs all original records still retained in the topic.
Use one consumer per bank for this local prototype; history is not transferred
between consumers. A single partition per bank also preserves global replay order
when the shared history capacity limit is reached.
Tiny fixture expectations are A: 1, B: 4, C: 3 local links.

## Pending team decisions

- Rule parameters and hop limit (Roles 2 and 3) → update the constants in `prepare.py` and regenerate.
- Event format is fixed here; Role 2 tells Role 1 if they change it.
- How replay output reaches Role 2's ingestor: a pipe into its stdin is the default (Role 2).
- How the Kubernetes replay Job gets workloads (built into the image, a volume, or generated inside the Job) and the replay image (Role 6, by 23 Oct).
