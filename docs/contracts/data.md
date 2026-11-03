# Data contract

Owner: Macarius · Partners: Cindy (events, rule), Isaac (evaluation, replay Job), Caleb (token input) · Code: `apps/dataset/`, `apps/ingestor/` · Status: **draft**, needs review by Cindy (events)

> [!NOTE]
> **Pending work (decided 30 Oct 2026).** The sections below describe what exists today; these items change it:
> - **SGD, all currencies:** `prepare.py` keeps every transfer and converts amounts to SGD: first to USD with the dataset's own rates (which keeps Altman's cross-currency hops consistent), then at MAS's September 2022 average, US$1 = S$1.4136 ([SingStat, source MAS](https://data.gov.sg/datasets/d_b2b7ffe00aaec3936ed379369fdf531b/view)). The USD-only filter keeps only 34% of CYCLE and 22% of RANDOM hops.
> - **Bank event files:** add `counterparty_account` (Singapore wires carry both accounts, MAS Notice 626 ¶11.4); `amount` in SGD.
> - **Singapore payment types:** `prepare.py` writes Altman's `ACH` as `GIRO` and `Wire` as `MEPS+`; cheque, card, cash and Bitcoin keep their names. The detection rules use only GIRO and MEPS+ ([streaming.md](streaming.md#detection-rule)).
> - **Labels and splits:** fetch Altman's patterns file and label each laundering transfer with its attempt ID and `altman_pattern` type. Detection is evaluated offline on dev/eval splits by attempt start ([evaluation.md](../evaluation.md#rules)). Planted chains go only into test fixtures, as the acceptance suite.
> - **Replay and ingestor:** one replay process sends pacs.008 messages over HTTP to each bank's [ingestor](#ingestor), which replaces the stdin pipe.
> - **Fixture expectations** follow the alert tiers in [case.md](case.md).

Scope: the synthetic dataset, the split into Banks A, B and C, planted chains, labels, workloads, the committed fixture and replay.

## Source

IBM AML synthetic dataset (Altman et al., NeurIPS 2023 Datasets and Benchmarks): Kaggle `ealtman2019/ibm-transactions-for-anti-money-laundering-aml`, **version 8**, file `HI-Small_Trans.csv`. It is about 476 MB and needs no Kaggle login. The first run downloads it to `~/.cache/kagglehub`.

| Fact | Value | Consequence |
| --- | --- | --- |
| Transfers | 5,078,345, from 1 to 18 Sep 2022 | |
| Laundering (Altman's label) | 5,177 | |
| Timestamps | Minute resolution; not sorted | We sort |
| Transaction ID | None | We generate `payment_id` |
| Bank IDs | Inconsistent zero-padding (`010` = `0010`) | Compared as integers |
| Account IDs | Unique only within a bank | Prefixed with the bank ID |
| Ordinary activity | Ends on 10 Sep; 11–18 Sep has 715 transfers (self-transfers removed), 652 of them laundering, as the generator finishes its patterns | Never used for false-positive counts ([evaluation.md](../evaluation.md#rules)) |

**pending work:** `HI-Medium_Trans.csv` and `HI-Medium_Patterns.txt` (same Kaggle version; 31,898,238 transfers, about 3.0 GB; ordinary activity ends on 16 Sep) are used once, for the out-of-sample check in [evaluation.md](../evaluation.md#experiments).

## Preparation rules (`prepare.py`)

1. **Supported cases:** keep only transfers where both sides are in US dollars and the sender and receiver accounts differ. That leaves 1,668,941 transfers, 1,912 of them Altman laundering. Other currencies are **unsupported** and dropped; there are no fees to model.
2. **Banks:** `int(bank ID) % 3` maps to `0 = A`, `1 = B`, `2 = C`. Events split A 30.5 %, B 35.9 %, C 33.7 %, and 66.6 % of transfers cross banks.
3. **Account:** `<int bank ID>-<account hex>`, e.g. `3208-8000F4580`.
4. **`payment_id`:** the first 16 hex characters of `sha256("altman:<source row>")`, so a transfer keeps the same ID in every slice. Planted transfers use `sha256("<chain_id>:<hop>")`, so they look the same as real ones.
5. **Time:** Altman time is treated as UTC and written as ISO 8601, e.g. `2022-09-05T09:20:00Z`.
6. **Events:** each transfer becomes two events sharing one `payment_id`: `out` at the sender's bank and `in` at the receiver's bank. This is the shared identifier both endpoints use for the token. Each bank file is sorted by `(event_time, payment_id)`. For an intra-bank transfer, `out` comes before `in`.

## Bank event files: `bank_{A,B,C}.csv`

Each bank only ever reads its own file.

| Column | Example | Notes |
| --- | --- | --- |
| `payment_id` | `008f4681d368aece` | Same at both endpoints: a stand-in for the ISO 20022 EndToEndId or Swift UETR, which both banks see unchanged ([ISO 20022](https://www.iso20022.org/standardsrepository/type/PaymentIdentification13), [Swift](https://www.swift.com/payments/what-unique-end-end-transaction-reference-uetr)). The input for `derive_token` ([security.md](security.md)) |
| `event_time` | `2022-09-05T00:00:00Z` | Event time, not replay time |
| `bank` | `A` | This bank |
| `direction` | `out` / `in` | |
| `account` | `22164-801F75F50` | This bank's customer only |
| `counterparty_account` | `3208-8000F4580` | **pending work:** The other side's account, for the new payee and quick repeat rules and each payee's payer count; never sent to peers |
| `counterparty_bank` | `B` | Which bank to query next |
| `amount` | `360542.13` | USD today; **pending work:** SGD |
| `payment_format` | `Wire` | ACH, Cheque, Credit Card, Cash, Wire, Bitcoin. **pending work:** `GIRO` (Altman's ACH) and `MEPS+` (Altman's Wire) |

## Labels: `labels.csv` (evaluation only)

Only the evaluation harness reads this file. Labels never go into bank files, Kafka, scoring or peer messages. Any `payment_id` not listed is ordinary activity.

| Column | Meaning |
| --- | --- |
| `payment_id` | The transfer |
| `altman_laundering` | `1` = Altman's own label. These aren't planted chains, so report them separately (**draft**, with Isaac) |
| `chain_id`, `hop` | Planted chain and position (0 = first transfer) |
| `scenario` | See below |
| `attempt_id`, `altman_pattern` | **pending work:** Altman's laundering attempt and its type (e.g. `CYCLE`, `FAN-IN`), from `HI-Small_Patterns.txt`; the offline evaluator reports per type. Laundering transfers in no listed attempt (about a third of the cross-bank bank-transfer laundering) leave both empty and are counted by day |

## Planted scenarios

Planted chains run between accounts that already exist in the slice, start in its first half, and **cross banks on every hop**. They are the deterministic acceptance suite, never detection-quality evidence, because they use the fast rule's own band. The expected results are **draft**: they depend on decisions in [protocol.md](protocol.md) and [case.md](case.md). **pending work:** with the alert tiers, a chain alerts (Tier 2) only when at least two banks' rules link its transfers, so a 2-transfer chain, which only the middle bank's rule links, is not a Tier 2 alert. Hits that link nothing, such as a new payee hit on a fixture payment (the fixture has no payment history), are Tier 1 cases at that bank.

| Scenario | Shape | Expected result |
| --- | --- | --- |
| `mule_chain` | 2–4 hops; each forwards 90–98 % within 1–30 min | Confirmed; one alert at each involved bank |
| `lookalike` | Same shape, but forwards only 20–50 % | No confirmed link; no alert |
| `loop` | 3 hops, ending back at the first account (e.g. A→B→C→A) | Search terminates on visited tokens; one alert per bank, not two at the start bank |
| `absent_link` | 3 hops with hop 1 removed | No match at the gap; hop 2 is never linked to hop 0 |
| `over_limit` | Hop limit + 2 = 6 hops | Partial, cut at the hop limit |

Constants in `prepare.py` (**draft**, to agree with Cindy and Zane):

- `HOP_LIMIT = 4`
- `FORWARD = 0.90–0.98`, `LOOKALIKE_FORWARD = 0.20–0.50`
- `DELAY_MIN = 1–30` minutes
- starting amount USD 5,000–50,000; format ACH or Wire (**pending work:** GIRO or MEPS+)

`--chains` sets the number of chains per scenario (default 10, i.e. 50 chains).

## Workloads

Run each with `uv run python -m dataset.prepare <args>`. Outputs go to the git-ignored `apps/dataset/out/`. Every workload writes a `workload.json` recording `t0`, its source, its parameters and its counts. The dataset version is pinned and seeds are fixed, so runs are reproducible.

| Workload | Args | Transfers (including planted) |
| --- | --- | --- |
| `fixtures/tiny` (committed) | `apps/dataset/fixtures/tiny --transfers apps/dataset/fixtures/tiny/transfers.csv` | 20 |
| `dev-small` | `apps/dataset/out/dev-small --start 2022-09-05 --hours 6 --seed 1` | 48,668 |
| `eval-small` | `apps/dataset/out/eval-small --start 2022-09-07 --hours 6 --seed 2` | 47,988 |
| `dev-medium` | `apps/dataset/out/dev-medium --start 2022-09-05 --hours 48 --seed 1` | ~351k |
| `eval-medium` | `apps/dataset/out/eval-medium --start 2022-09-07 --hours 48 --seed 2` | ~351k |

**Held out:** dev covers 5–6 Sep with seed 1, and eval covers 7–8 Sep with seed 2. Tune only on dev.

**pending work:** `altman-dev` and `altman-eval`, the splits by attempt start (attempts starting 1–5 Sep and 6–10 Sep, each with the following days so attempts can finish), with no planted chains, for the offline evaluator. The 6-hour and 48-hour slices stay for load tests and the demo.

Use `--hours 5` or more: chains start in the first half of the slice and the longest (`over_limit`) spans up to 150 minutes.

## Fixture: `apps/dataset/fixtures/tiny`

20 hand-written transfers covering every scenario. `test_prepare.py` checks that the committed bank files and `labels.csv` match `transfers.csv`.

`--transfers` takes a CSV with the columns `payment_id,time,from_bank,from_account,to_bank,to_account,amount,payment_format,altman_laundering,chain_id,hop,scenario` (the last three are empty for unplanted transfers), as in `fixtures/tiny/transfers.csv`.

| Chain | Transfers | Banks | Expected (**draft**) |
| --- | --- | --- | --- |
| — | p01–p04 | Mixed; p02 is intra-bank at B | Ordinary; no alert |
| — | p05 | B→C | `altman_laundering = 1`; not a planted chain |
| `m1` | p06 → p07 | A→B→C | Confirmed; alerts at A, B and C. **pending work:** not a Tier 2 alert, since only B's rule links p06 → p07; a Tier 1 case at B |
| `l1` | p08 → p09 | A→B→C | 40 % forwarded; no alert |
| `o1` | p10 → p11 → p12 | A→B→C→A | Confirmed and terminates; one alert each at A, B and C (still Tier 2: B's and C's rules link it) |
| `x1` | p13, (hop 1 missing), p14 | A→B, C→A | No match at B; p14 not linked |
| `v1` | p15 → … → p20 | A→B→C→A→B→C→A | Partial, cut at hop 4 |

Payment IDs shared by each bank pair (useful for token tests):

| Pair | Shared IDs |
| --- | --- |
| A–B | p01, p06, p08, p10, p13, p15, p18 |
| A–C | p03, p04, p12, p14, p17, p20 |
| B–C | p05, p07, p09, p11, p16, p19 |

## Replay (`replay.py`)

Run one replay per bank. Each prints its bank's events as JSON lines, in file order, for that bank's ingestor ([streaming.md](streaming.md)). In each line every field is a string except `amount`, which is a number. An event is printed at `start_at + (event_time − t0) / speed`, so all three banks must be given the same `--start-at` to stay in step.

```sh
uv run python -m dataset.replay apps/dataset/fixtures/tiny A --speed 0                        # no pacing
uv run python -m dataset.replay apps/dataset/out/dev-small A --speed 60 --start-at <epoch> | <ingestor>
```

At `--speed 60`, a 6-hour workload takes 6 minutes. `--speed 0` sends as fast as possible, for load tests.

## Ingestor

Each bank's entry point: it turns a payment message into this contract's events, validates them and produces them to Kafka. `ingestor.records()` (`apps/ingestor/`) does the mapping and every check, and its test loads both shared samples. `ingestor.app` produces the records to Kafka and serves them two ways: an HTTP server, as a plain container locally and on k3d, and `lambda_handler` behind a Lambda function URL (`AuthType: AWS_IAM`, requests signed with SigV4) on EKS. Its settings are in [deployment.md](deployment.md#per-service-settings). It replaces the scorer's stdin ingest mode, which the scorer drops in [#19](https://github.com/10x-Slackers/CSC3104/issues/19).

```sh
INGESTOR_BANK=B uv run python -m ingestor.app                     # Kafka at localhost:9092
curl -d @apps/ingestor/fixtures/pacs008_batch.json localhost:8080   # {"events": 4}
```

**Input:** a subset of ISO 20022 **pacs.008** (FIToFICustomerCreditTransfer, as used by FAST) as JSON over HTTP POST, one batch per message. JSON keys use the XML element names. The shared sample, [`pacs008_batch.json`](../../apps/ingestor/fixtures/pacs008_batch.json), is a batch to Bank B with three fixture payments: `p02` (intra-bank at B), `p06` (from A) and `p07` (to C). Its header and `p06`:

```json
{"FIToFICstmrCdtTrf": {
  "GrpHdr": {"MsgId": "replay-B-000001", "CreDtTm": "2022-09-05T09:25:00Z", "NbOfTxs": "3"},
  "CdtTrfTxInf": [{"PmtId": {"EndToEndId": "p02"}, ...}, {
    "PmtId": {"EndToEndId": "p06"},
    "PmtTpInf": {"LclInstrm": {"Prtry": "MEPS+"}},
    "IntrBkSttlmAmt": {"Ccy": "SGD", "Amt": "10000.00"},
    "AccptncDtTm": "2022-09-05T09:10:00Z",
    "DbtrAcct": {"Id": {"Othr": {"Id": "A-m0"}}}, "DbtrAgt": {"FinInstnId": {"BICFI": "TSTASGSG"}},
    "CdtrAcct": {"Id": {"Othr": {"Id": "B-m1"}}}, "CdtrAgt": {"FinInstnId": {"BICFI": "TSTBSGSG"}}
  }, {"PmtId": {"EndToEndId": "p07"}, ...}]
}}
```

Bank identifiers are fictional BICs: A `TSTASGSG`, B `TSTBSGSG`, C `TSTCSGSG`. The one table is `ingestor.BICS`, which replay imports. It lives in the ingestor rather than in `dataset` so the ingestor's Lambda image doesn't pull in pandas and kagglehub.

| Event field | From |
| --- | --- |
| `payment_id` | `PmtId.EndToEndId`, also the idempotency key |
| `event_time` | `AccptncDtTm` (UTC, `Z`) |
| `bank` | The ingestor's own bank (config) |
| `direction` | `out` if `DbtrAgt` is this bank, `in` if `CdtrAgt` is; an intra-bank transfer emits both, `out` first, though Kafka can deliver them in either order ([streaming.md](streaming.md#kafka-events)) |
| `account`, `counterparty_account` | `DbtrAcct` / `CdtrAcct`, by direction |
| `counterparty_bank` | The other agent's BIC, mapped back to `A`/`B`/`C` |
| `amount` | `IntrBkSttlmAmt.Amt` (a decimal string), written as a JSON number; `Ccy` must be `SGD` |
| `payment_format` | `PmtTpInf.LclInstrm.Prtry`: `GIRO` and `MEPS+` for bank transfers (Altman's ACH and Wire), and `Cheque`, `Cash`, `Credit Card` and `Bitcoin` as proprietary local instruments |

The events and their Kafka headers are in [streaming.md](streaming.md#kafka-events).

### Batches and responses

**agreed** (Isaac, 2 Nov 2026).

| Item | Decision |
| --- | --- |
| Endpoint | `POST /` with the batch as the JSON body; `GET /healthz` for the readiness probe |
| What a batch holds | Only payments that involve this bank. Replay sends each payment to both banks involved, or once if it's intra-bank: `p06` goes to A and to B, `p02` only to B |
| Batch size | The payments due at the same moment, at most 100. At `--speed 60` one Altman minute replays in one second; on HI-Small, 1–10 Sep, a bank receives 158–192 payments a second on average (p99 418–471, busiest minute 1,870), so usually two batches a second. 100 payments are about 40 KB, far under the function URL's [6 MB request limit](https://docs.aws.amazon.com/lambda/latest/dg/API_CreateFunctionUrlConfig.html) |
| `200` | Kafka confirmed every event: `flush()` returned and no delivery failed. Body `{"events": <n>}` |
| `400` | The whole batch is rejected and nothing is produced: `NbOfTxs` doesn't match the transaction count, a payment names neither agent as this bank, `Ccy` isn't `SGD`, the amount isn't positive, `AccptncDtTm` isn't UTC (`Z`), a field is missing or empty, or a BIC is unknown. Body `{"error": "<reason>"}`. Replay stops with an error, because resending the same batch can't succeed |
| `5xx`, `429`, timeout | Replay resends the same batch (same `MsgId` and `EndToEndId`s) with exponential backoff and full jitter, from 1 s doubling to at most 30 s, until it succeeds ([AWS](https://aws.amazon.com/builders-library/timeouts-retries-and-backoff-with-jitter/)). `503` means Kafka didn't confirm every event within `message.timeout.ms`; `429` is Lambda throttling. Replay keeps one queue per bank, so a bank that's down delays only its own batches |
| Duplicates | A resent batch can write some events twice; consumers drop repeats by `ce_id` ([streaming.md](streaming.md#kafka-events)) |

Behaviour:

- **Validation at the boundary:** `records()` makes the scorer's `validate()` checks (the `400` row); the scorer keeps its own until [#19](https://github.com/10x-Slackers/CSC3104/issues/19).
- **Acknowledge only after a durable write:** `handle()` produces every event keyed by `account` to `bank-<X>-transactions`, calls `flush()` once, then returns 200. If Kafka reports any event undelivered, it returns 503 and replay resends the batch, which is safe because `EndToEndId` is a caller-supplied idempotency key ([AWS](https://aws.amazon.com/builders-library/making-retries-safe-with-idempotent-APIs/)). With no broker reachable, the 503 comes after 25 s.
- **Producer:** `confluent_kafka` with `enable.idempotence: True` (librdkafka defaults it to false; enabling it sets `acks=all`) and `message.timeout.ms` 25 s against a 30 s Lambda timeout. It's created at module level so warm invocations reuse it.
- **Code shape:** one `handle(body) -> (status, reply)` with two thin adapters: `Handler`, for the standard library's `ThreadingHTTPServer`, with keep-alive, `POST /` and `GET /healthz`; and `lambda_handler`, for the function URL's payload format 2.0, whose `body` is the batch.
- **Logs:** one JSON line per batch to stdout, e.g. `{"ts": 1793500000.123, "service": "ingestor", "bank": "B", "status": 200, "ms": 4.2, "events": 4, "payment_ids": ["p02", "p02", "p06", "p07"]}`. `ts` is also every record's Kafka timestamp, so the evaluation joins each `payment_id` to it ([evaluation.md](../evaluation.md#metric-definitions)). A rejected batch logs its `error` and no IDs.
- **Why not gRPC:** Lambda can't be a gRPC target (ALB gRPC target groups don't allow Lambda), and the input is a payment-network message, not a schema we design. gRPC is used between long-lived peers; HTTP + JSON at the serverless edge.
- **Stated simplifications:** JSON instead of FAST's XML; a subset of pacs.008 that leaves out elements a real message requires but detection doesn't need, such as the charge bearer (`ChrgBr`) and the debtor and creditor parties (`Dbtr`, `Cdtr`); intra-bank book transfers wouldn't go through FAST; cheque, cash and card aren't credit transfers, so they're carried as a proprietary local instrument.

**Replay (**pending work**):** one process feeds all three banks from one clock, so `--start-at` coordination goes away. It retries a batch on 5xx, adds `--rate` (events/s, open-loop) for load tests, and keeps `--speed` for the demo. On k3d it runs as a Job in its own `replay` namespace; on EKS it signs requests with SigV4.

## Interfaces

Agreed contract first ([conventions](../README.md#conventions)): the producer adds one sample message here, and both sides' tests load it.

| Interface | From → to | Pins down | Status | Agree by |
| --- | --- | --- | --- | --- |
| pacs.008 batch ([Ingestor](data.md#ingestor)) | Replay → ingestor (Macarius); the Job: Isaac | Field subset, BIC table, batch size, 4xx vs 5xx and retry, SGD only | **agreed** (Isaac, 2 Nov 2026), sample [`pacs008_batch.json`](../../apps/ingestor/fixtures/pacs008_batch.json) | Mon 2 Nov |
| Labels file and splits | Macarius → Isaac | Columns, the split by attempt start, laundering in no attempt counted by day | **draft**, sample TODO | Wed 4 Nov |

Also part of: [Kafka transaction event](streaming.md#interfaces).

## Open

- [ ] How the Kubernetes replay Job gets its workload: baked into the image or mounted (Macarius, with Isaac).
- [x] Rule parameters and hop limit: `HOP_LIMIT = 4` stays; the planted-chain constants stay as acceptance-test values, and the detection rules are in [streaming.md](streaming.md#detection-rule).
- [x] How replay output reaches the ingestor: HTTP POST of pacs.008 batches ([Ingestor](#ingestor)).
- [x] How Altman-labelled transfers are scored: offline, per attempt and pattern type ([evaluation.md](../evaluation.md#offline-detection-evaluation)).
