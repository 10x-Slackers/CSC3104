# Data contract

Owner: Macarius · Partners: Cindy (events, rule), Isaac (evaluation, replay Job), Caleb (token input) · Code: `apps/dataset/`, `apps/ingestor/` · Status: **draft**, needs review by Cindy (events) and Isaac (labels)

> [!NOTE]
> **Pending work (decided 30 Oct 2026).** The sections below describe what exists today; these items change it:
> - **Fixture expectations** follow the alert tiers in [case.md](case.md).

Scope: the synthetic dataset, the split into Banks A, B and C, labels, workloads, the committed fixture with its planted chains, and replay.

## Source

IBM AML synthetic dataset (Altman et al., NeurIPS 2023 Datasets and Benchmarks): Kaggle `ealtman2019/ibm-transactions-for-anti-money-laundering-aml`, **version 8**, files `HI-Small_Trans.csv` (the transfers) and `HI-Small_Patterns.txt` (the laundering attempts). They are about 476 MB and need no Kaggle login. The first run downloads them to `~/.cache/kagglehub`.

| Fact | Value | Consequence |
| --- | --- | --- |
| Transfers | 5,078,345, from 1 to 18 Sep 2022 | |
| Laundering (Altman's label) | 5,177 | |
| Laundering attempts | 370 in the patterns file, of 8 types; 3,198 of the 5,166 laundering transfers kept are in one | The rest are counted by day ([evaluation.md](../evaluation.md#rules)) |
| Currencies | 15, including Bitcoin; 63% of transfers aren't in US dollars on both sides | Converted to SGD (rule 2) |
| Timestamps | Minute resolution; not sorted | We sort |
| Transaction ID | None | We generate `payment_id` |
| Bank IDs | Inconsistent zero-padding (`010` = `0010`) | Compared as integers |
| Account IDs | Unique only within a bank | Prefixed with the bank ID |
| Ordinary activity | Ends on 10 Sep; 11–18 Sep has 715 transfers (self-transfers removed), 652 of them laundering, as the generator finishes its patterns | Never used for false-positive counts ([evaluation.md](../evaluation.md#rules)) |

`--altman HI-Medium` reads `HI-Medium_Trans.csv` and `HI-Medium_Patterns.txt` instead (same Kaggle version; 31,898,238 transfers, about 3.0 GB; ordinary activity ends on 16 Sep). They are used for the out-of-sample check in [evaluation.md](../evaluation.md#experiments), and their bank transfers can be replayed on EKS ([Workloads](#workloads)).

## Preparation rules (`prepare.py`)

1. **Supported cases:** every currency; only self-transfers (the same account on both sides) are dropped. That leaves 4,487,133 transfers, 5,166 of them Altman laundering. There are no fees to model. Keeping only US-dollar transfers, as before, kept only 34 % of CYCLE and 22 % of RANDOM hops.
2. **Amounts in SGD:** `Amount Paid` goes to USD at the dataset's own rate, then to SGD at MAS's September 2022 average, US$1 = S$1.4136 ([SingStat, source MAS](https://data.gov.sg/datasets/d_b2b7ffe00aaec3936ed379369fdf531b/view)). A currency's rate is the median of `Amount Received / Amount Paid` over payments received in US dollars (EUR 1 = US$1.1718 on HI-Small), which keeps Altman's cross-currency hops consistent. Amounts are in cents, at least SGD 0.01, because 4,358 ordinary payments (e.g. a 0.01 Shekel cheque) would otherwise round to 0, which the ingestor rejects. For example, a US$360,542.13 cheque becomes SGD 509,662.35.
3. **Banks:** `int(bank ID) % 3` maps to `0 = A`, `1 = B`, `2 = C`. Events split A 30.0 %, B 38.1 %, C 32.0 %, and 65.3 % of transfers cross banks. Three banks merge some of Altman's banks: banks 10 and 70 both become B, so a transfer between them is intra-bank. On HI-Small, 1–10 Sep, 98.3 % of laundering transfers cross Altman's banks but 65.6 % cross A, B and C, so the rules' scope (cross-bank GIRO and MEPS+) holds 55.4 % of laundering instead of 83.0 %. Recall inside the scope barely changes (95.8 % with three banks, 95.4 % with every Altman bank as its own bank, in the exploratory sweep), so three banks shrink the scope, not the rules' accuracy.
4. **Account:** `<int bank ID>-<account hex>`, e.g. `3208-8000F4580`.
5. **`payment_id`:** the first 16 hex characters of `sha256("<file>:<source row>")`, e.g. `sha256("HI-Small:2288336")` gives `9ce850992a9f4cec`. A transfer keeps the same ID in every slice, and HI-Small and HI-Medium never share an ID. The fixture's transfers keep their hand-written IDs (`p01`–`p20`).
6. **Payment types:** Altman's `ACH` becomes `GIRO` and `Wire` becomes `MEPS+` (Singapore payment systems); cheque, card, cash and Bitcoin keep their names. Only GIRO and MEPS+ are bank transfers, which the detection rules use ([streaming.md](streaming.md#detection-rule)). Replay sends only GIRO and MEPS+ payments, so the other types never reach the ingestor, Kafka or the scorer; they stay in the bank files for the offline evaluator's out-of-scope counts ([evaluation.md](../evaluation.md#rules)).
7. **Time:** Altman time is treated as UTC and written as ISO 8601, e.g. `2022-09-05T09:20:00Z`.
8. **Events:** each transfer becomes two events sharing one `payment_id`: `out` at the sender's bank and `in` at the receiver's bank. This is the shared identifier both endpoints use for the token. Each bank file is in `event_time` order; transfers in the same minute keep their order in the source file. For an intra-bank transfer, `out` comes before `in`.
9. **Attempts:** a laundering transfer listed in the patterns file gets that attempt's number (`attempt_id`, counting the file's `BEGIN` lines from 0) and type (`altman_pattern`). It's matched on time, both bank IDs, both accounts and the amount paid; one listed twice keeps its first attempt. For example, attempt `0` is a `FAN-OUT` from account `21174-800737690`.

## Bank event files: `bank_{A,B,C}.csv`

Each bank only ever reads its own file. The examples are one row of `bank_A.csv` in `altman-small`.

| Column | Example | Notes |
| --- | --- | --- |
| `payment_id` | `9ce850992a9f4cec` | Same at both endpoints: a stand-in for the ISO 20022 EndToEndId or Swift UETR, which both banks see unchanged ([ISO 20022](https://www.iso20022.org/standardsrepository/type/PaymentIdentification13), [Swift](https://www.swift.com/payments/what-unique-end-end-transaction-reference-uetr)). The input for `derive_token` ([security.md](security.md)) |
| `event_time` | `2022-09-05T00:00:00Z` | Event time, not replay time |
| `bank` | `A` | This bank |
| `direction` | `out` | `out` at the sender's bank, `in` at the receiver's |
| `account` | `22164-801F75F50` | This bank's customer only |
| `counterparty_account` | `2947-806272DA0` | The other side's account (Singapore wires carry both, MAS Notice 626 ¶11.4), for the new payee and quick repeat rules; never sent to peers |
| `counterparty_bank` | `B` | Which bank to query next |
| `amount` | `509662.35` | SGD in cents, at least `0.01` (rule 2) |
| `payment_format` | `Cheque` | `GIRO`, `MEPS+`, `Cheque`, `Credit Card`, `Cash` or `Bitcoin` (rule 6) |

## Labels: `labels.csv` (evaluation only)

Only the evaluation harness reads this file. Labels never go into bank files, Kafka, scoring or peer messages. Any `payment_id` not listed is ordinary activity.

| Column | Meaning |
| --- | --- |
| `payment_id` | The transfer |
| `altman_laundering` | `1` = Altman's own label, the ground truth for detection quality |
| `attempt_id`, `altman_pattern` | Altman's laundering attempt (`0`–`369` on HI-Small, rule 9) and its type: `BIPARTITE`, `CYCLE`, `FAN-IN`, `FAN-OUT`, `GATHER-SCATTER`, `RANDOM`, `SCATTER-GATHER` or `STACK`. The offline evaluator reports per type. Laundering transfers in no listed attempt (840 of the 2,499 cross-bank GIRO and MEPS+ laundering transfers on HI-Small, 1–10 Sep) leave both empty and are counted by day |
| `chain_id`, `hop` | Planted chain and position (0 = first transfer); fixture only |
| `scenario` | See below; fixture only |

Examples: `866bab90f796f92f,1,0,FAN-OUT,,,` is in attempt 0; `0c0c5b0ee33a7df5,1,,,,,` is laundering in no attempt; the fixture's `p06,0,,,m1,0,mule_chain` is hop 0 of planted chain `m1`. The fixture has no real Altman attempts, so its `p05,1,0,FAN-OUT,,,` is a stand-in that fills both attempt columns in the labels sample.

**Splits** (for the offline evaluator, [evaluation.md](../evaluation.md#rules)): dev = the attempts whose first transfer is on 1–5 Sep, eval = 6–10 Sep; laundering in no attempt goes by its own day. Both read all of `altman-small`: eval reads 1–5 Sep as rule history that it doesn't count, and each reads the days after its own so attempts can finish. False positives count only on the split's own days. Rule 1 drops 11 of the patterns file's 3,209 laundering transfers as self-transfers (all GIRO, each in a different `GATHER-SCATTER` attempt; 72 on HI-Medium). That changes the first transfer of 2 attempts, but not its day, so no attempt changes split.

## Planted scenarios (fixture only)

`prepare.py` no longer plants chains; the fixture's are hand-written. They **cross banks on every hop** and are the deterministic acceptance suite, never detection-quality evidence, because they use the fast rule's own band. The expected results are **draft**: they depend on decisions in [protocol.md](protocol.md) and [case.md](case.md). **pending work:** with the alert tiers, a chain alerts (Tier 2) only when at least two banks' rules link its transfers, so a 2-transfer chain, which only the middle bank's rule links, is not a Tier 2 alert. Hits that link nothing, such as a new payee hit on a fixture payment (the fixture has no payment history), are Tier 1 cases at that bank.

| Scenario | Shape | Expected result |
| --- | --- | --- |
| `mule_chain` | Each hop forwards 90–98 % within 1–30 min | Confirmed; one alert at each involved bank |
| `lookalike` | Same shape, but forwards only 20–50 % | No confirmed link; no alert |
| `loop` | 3 hops, ending back at the first account (e.g. A→B→C→A) | Search terminates on visited tokens; one alert per bank, not two at the start bank |
| `absent_link` | 3 hops with hop 1 removed | No match at the gap; hop 2 is never linked to hop 0 |
| `over_limit` | Hop limit + 2 = 6 hops | Partial, cut at the hop limit |

## Workloads

Run each with `uv run python -m dataset.prepare <args>`. Outputs go to the git-ignored `apps/dataset/out/`. Every workload writes a `workload.json` recording `t0`, its source, its parameters and its counts. The dataset version is pinned and nothing is random, so runs are reproducible.

| Workload | Args | Transfers | Used for |
| --- | --- | --- | --- |
| `fixtures/tiny` (committed) | `apps/dataset/fixtures/tiny --transfers apps/dataset/fixtures/tiny/transfers.csv` | 20 | Tests, acceptance suite |
| `slice-6h` | `apps/dataset/out/slice-6h --start 2022-09-05 --hours 6` | 129,415 | Load tests, demo |
| `slice-48h` | `apps/dataset/out/slice-48h --start 2022-09-05 --hours 48` | 943,939 | Load tests |
| `altman-small` | `apps/dataset/out/altman-small` (all of HI-Small) | 4,487,133 | Offline evaluation, dev and eval |
| `altman-1-6sep` | `apps/dataset/out/altman-1-6sep --start 2022-09-01 --hours 144` | 2,696,535 | Offline = live (experiment 13) |
| `altman-medium` | `apps/dataset/out/altman-medium --altman HI-Medium` | 29,336,378 | Out-of-sample check; its bank transfers for replay on EKS (below) |

`altman-small` takes about 30 s and writes 825 MB; `altman-medium` takes about 4 minutes and writes 5.5 GB. Peak memory on Linux (a Python 3.12 container) is 2.9 GB for `altman-small` and about 19 GB for `altman-medium` (estimated from 6.5 times the transfers; Docker's 8 GB VM is too small to measure it). macOS reports about 4 GB and 11 GB, because it compresses memory and doesn't count the compressed part. So `altman-medium` is built on a machine with that much memory, not in k3d (an 8 GB VM) or in a pod. HI-Medium gives the same inputs as the exploratory sweep: 31,086 laundering transfers on 1–16 Sep, 17,777 of them cross-bank GIRO or MEPS+.

**Bank transfers only, for replay.** Replay reads only the GIRO and MEPS+ rows, so the bank files a replay Job gets can leave out the rest. For `altman-medium` that takes them from 5.5 GB to 817 MB (A 274 MB, B 271 MB, C 276 MB), and replay reads the same 4,509,233 payments in the same order. `workload.json` is copied unchanged because replay needs its `t0`, so its counts are still the full workload's. The offline evaluator keeps reading the full `altman-medium` (rule 6).

```sh
mkdir -p apps/dataset/out/altman-medium-bt
for b in A B C; do  # keep the header and rows whose 9th column, payment_format, is GIRO or MEPS+
  awk -F, 'NR==1 || $9=="GIRO" || $9=="MEPS+"' apps/dataset/out/altman-medium/bank_$b.csv > apps/dataset/out/altman-medium-bt/bank_$b.csv
done
cp apps/dataset/out/altman-medium/workload.json apps/dataset/out/altman-medium-bt/
```

**One workload for dev and eval.** The plan named two, `altman-dev` and `altman-eval`. Eval reads 1–5 Sep as history, and dev reads the days after 5 Sep so its attempts can finish, so both would hold every day of HI-Small. They share `altman-small`, and the evaluator applies the split ([Splits](#labels-labelscsv-evaluation-only)).

**Slices.** A slice is every transfer from `--start` for `--hours`; `slice-6h` is 5 Sep, 00:00–06:00. `slice-6h` and `slice-48h` replace `dev-small`, `eval-small`, `dev-medium` and `eval-medium`, which differed only in their start day and planted-chain seed. A slice starts with no history, so every payee looks new on its first payment and the new payee rule fires more than it does on `altman-small`. Starting empty nearly doubled the ordinary payments flagged in the exploratory sweep ([evaluation.md](../evaluation.md#rules)). Use slices for load tests and the demo, never for detection quality. The exception is `altman-1-6sep`: it starts where HI-Small starts (1 Sep 00:00), so it misses no history. Its bank files are the first rows of `altman-small`'s, up to the end of 6 Sep, and its labels are `altman-small`'s for those payments.

## Fixture: `apps/dataset/fixtures/tiny`

20 hand-written transfers covering every scenario, with amounts already in SGD. `test_prepare.py` checks that the committed bank files and `labels.csv` match `transfers.csv`.

`--transfers` takes a CSV with the columns `payment_id,time,from_bank,from_account,to_bank,to_account,amount,payment_format,altman_laundering,attempt_id,altman_pattern,chain_id,hop,scenario`, as in `fixtures/tiny/transfers.csv`. `attempt_id` and `altman_pattern` are empty outside an Altman attempt, and the last three are empty for unplanted transfers.

**pending work:** no fixture account has earlier payments, so the new payee rule fires on every cross-bank GIRO and MEPS+ payment: 17 of the 20, all but p01 (cheque), p02 (intra-bank) and p04 (card). Each hit is at the sender's bank, each a Tier 1 case unless a chain promotes it.

| Chain | Transfers | Banks | Expected (**draft**) |
| --- | --- | --- | --- |
| — | p01–p04 | Mixed; p02 is intra-bank at B | Ordinary; no alert |
| — | p05 | B→C | `altman_laundering = 1` in a stand-in attempt `0` (`FAN-OUT`), so the labels sample shows both columns; not a planted chain |
| `m1` | p06 → p07 | A→B→C | Confirmed; alerts at A, B and C. **pending work:** not a Tier 2 alert, since only B's rule links p06 → p07; a Tier 1 case at B |
| `l1` | p08 → p09 | A→B→C | 40 % forwarded; no alert |
| `o1` | p10 → p11 → p12 | A→B→C→A | Confirmed and terminates; one alert each at A, B and C (still Tier 2: B's and C's rules link it). **pending work:** the alert promotes the new payee cases on p10, p11 and p12 |
| `x1` | p13, (hop 1 missing), p14 | A→B, C→A | No match at B; p14 not linked |
| `v1` | p15 → … → p20 | A→B→C→A→B→C→A | Partial, cut at hop 4. **pending work:** a Tier 2 alert at A, B and C (partial counts; all three banks' rules link it) |

Payment IDs shared by each bank pair (useful for token tests):

| Pair | Shared IDs |
| --- | --- |
| A–B | p01, p06, p08, p10, p13, p15, p18 |
| A–C | p03, p04, p12, p14, p17, p20 |
| B–C | p05, p07, p09, p11, p16, p19 |

## Replay (`replay.py`)

One process sends all three banks their payments as pacs.008 batches over HTTP ([Batches and responses](#batches-and-responses)), so one clock drives every bank. It reads each GIRO and MEPS+ transfer's `out` event from the three bank files, in event-time order, and sends the payment to the ingestor of each bank it involves: `p06` to A and to B, `p02` once to B. Cheque, card, cash and Bitcoin payments stay in the bank files and are never sent (rule 6).

```sh
uv run python -m dataset.replay apps/dataset/fixtures/tiny --url B=http://localhost:8080 --speed 0
uv run python -m dataset.replay apps/dataset/out/slice-6h --url A=<url> --url B=<url> --url C=<url>             # demo: --speed 60
uv run python -m dataset.replay apps/dataset/out/slice-6h --url A=<url> --url B=<url> --url C=<url> --rate 2000 # load test
```

| Option | Meaning |
| --- | --- |
| `--url BANK=URL` | A bank's ingestor, once per bank, e.g. `B=http://localhost:8080`. A bank without one is skipped, so one local ingestor is enough for a test |
| `--speed` | Simulated seconds per real second, default 60. A payment is sent at `start + (event_time − t0) / speed`, so a 6-hour workload takes 6 minutes; `0` sends as fast as possible |
| `--rate` | Events per second, open-loop, ignoring event times, for load tests. Each payment makes two events, so `--rate 2000` sends 1,000 payments a second |

- **Batches:** each bank has its own queue and one sender, so a bank that's down delays only its own batches. Each bank gets its payments in event-time order, though payments in the same minute can arrive in a different order than in the bank file. A batch is the payments that became due while the bank's previous batch was being sent, at most 100. Its `MsgId` is `replay-<bank>-<n>` (e.g. `replay-B-000001`), and its `CreDtTm` is its last payment's time.
- **Open loop:** the clock never waits for a reply, so a slow ingestor shows as a growing `queued` count in the log.
- **Responses:** after a `200` the sender moves on. A `5xx`, `429` or network error resends the same batch, with full jitter from 1 s doubling to 30 s, until it succeeds. A `400` stops replay with exit status 1.
- **Logs:** one JSON line per POST, e.g. `{"ts": 1793868103.47, "service": "replay", "bank": "B", "msg_id": "replay-B-000001", "status": 200, "attempt": 0, "ms": 2.5, "payments": 1, "queued": 0}`, plus the reply when it isn't a `200`, and a last line such as `{"service": "replay", "payments": 18, "seconds": 1.7}`.
- **Measured:** all of `altman-small` at `--speed 0` (700,510 payments, 1,156,694 deliveries to banks) took about 26 s against ingestors that reply at once, using 31 MB of memory. All of `altman-medium` (4,509,233 payments) took 173 s and 29 MB: replay reads the files line by line, so its memory doesn't grow with the workload. At the default `--speed 60`, `altman-medium` takes about 11 hours, since its events run from 1 to 28 Sep.
- **pending work:** on k3d it runs as a Job in its own `replay` namespace, sending to each bank's `ingestor` Service ([#29](https://github.com/10x-Slackers/CSC3104/issues/29)); on EKS it sends HTTPS to the function URLs and signs each request with SigV4, service `lambda`. Today it speaks plain HTTP.

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
| `payment_format` | `PmtTpInf.LclInstrm.Prtry`: `GIRO` or `MEPS+`, the bank transfers (Altman's ACH and Wire); anything else is a `400` |

The events and their Kafka headers are in [streaming.md](streaming.md#kafka-events).

### Batches and responses

**agreed** (Isaac, 2 Nov 2026).

| Item | Decision |
| --- | --- |
| Endpoint | `POST /` with the batch as the JSON body; `GET /healthz` for the readiness probe |
| What a batch holds | Only GIRO and MEPS+ payments that involve this bank. Replay sends each payment to both banks involved, or once if it's intra-bank: `p06` goes to A and to B, `p02` only to B |
| Batch size | The payments due at the same moment, at most 100. At `--speed 60` one Altman minute replays in one second; on HI-Small, 1–10 Sep, a bank receives 25–28 GIRO and MEPS+ payments a second on average (p99 53–57, busiest minute 383), so usually one batch a second. 100 payments are about 40 KB, far under the function URL's [6 MB request limit](https://docs.aws.amazon.com/lambda/latest/dg/API_CreateFunctionUrlConfig.html) |
| `200` | Kafka confirmed every event: `flush()` returned and no delivery failed. Body `{"events": <n>}` |
| `400` | The whole batch is rejected and nothing is produced: `NbOfTxs` doesn't match the transaction count, a payment names neither agent as this bank, `Ccy` isn't `SGD`, the amount isn't positive, `AccptncDtTm` isn't UTC (`Z`), a field is missing or empty, a BIC is unknown, or a payment isn't `GIRO` or `MEPS+` (e.g. `Cheque`). Body `{"error": "<reason>"}`. Replay stops with an error, because resending the same batch can't succeed |
| `5xx`, `429`, timeout | Replay resends the same batch (same `MsgId` and `EndToEndId`s) with exponential backoff and full jitter, from 1 s doubling to at most 30 s, until it succeeds ([AWS](https://aws.amazon.com/builders-library/timeouts-retries-and-backoff-with-jitter/)). `503` means Kafka didn't confirm every event within `message.timeout.ms`; `429` is Lambda throttling. Replay keeps one queue per bank, so a bank that's down delays only its own batches |
| Duplicates | A resent batch can write some events twice; consumers drop repeats by `ce_id` ([streaming.md](streaming.md#kafka-events)) |

Behaviour:

- **Validation at the boundary:** `records()` makes every check the scorer's `validate()` used to (the `400` row); since #25 the scorer only consumes and trusts the fields.
- **Acknowledge only after a durable write:** `handle()` produces every event keyed by `account` to `bank-<X>-transactions`, calls `flush()` once, then returns 200. If Kafka reports any event undelivered, it returns 503 and replay resends the batch, which is safe because `EndToEndId` is a caller-supplied idempotency key ([AWS](https://aws.amazon.com/builders-library/making-retries-safe-with-idempotent-APIs/)). With no broker reachable, the 503 comes after 25 s.
- **Producer:** `confluent_kafka` with `enable.idempotence: True` (librdkafka defaults it to false; enabling it sets `acks=all`) and `message.timeout.ms` 25 s against a 30 s Lambda timeout. It's created at module level so warm invocations reuse it.
- **Code shape:** one `handle(body) -> (status, reply)` with two thin adapters: `Handler`, for the standard library's `ThreadingHTTPServer`, with keep-alive, `POST /` and `GET /healthz`; and `lambda_handler`, for the function URL's payload format 2.0, whose `body` is the batch.
- **Logs:** one JSON line per batch to stdout, e.g. `{"ts": 1793500000.123, "service": "ingestor", "bank": "B", "status": 200, "ms": 4.2, "events": 4, "payment_ids": ["p02", "p02", "p06", "p07"]}`. `ts` is also every record's Kafka timestamp, so the evaluation joins each `payment_id` to it ([evaluation.md](../evaluation.md#metric-definitions)). A rejected batch logs its `error` and no IDs.
- **Why not gRPC:** Lambda can't be a gRPC target (ALB gRPC target groups don't allow Lambda), and the input is a payment-network message, not a schema we design. gRPC is used between long-lived peers; HTTP + JSON at the serverless edge.
- **Stated simplifications:** JSON instead of FAST's XML; a subset of pacs.008 that leaves out elements a real message requires but detection doesn't need, such as the charge bearer (`ChrgBr`) and the debtor and creditor parties (`Dbtr`, `Cdtr`); intra-bank book transfers wouldn't go through FAST; cheque, cash and card aren't credit transfers, so they aren't sent at all and the only instruments are GIRO and MEPS+. The fixture's `p02` (`B-bob → B-carol`, S$45.50) is GIRO rather than a card payment, so the fixture and the shared samples keep an intra-bank transfer; that changes no rule hit.


## Interfaces

Agreed contract first ([conventions](../README.md#conventions)): the producer adds one sample message here, and both sides' tests load it.

| Interface | From → to | Pins down | Status | Agree by |
| --- | --- | --- | --- | --- |
| pacs.008 batch ([Ingestor](data.md#ingestor)) | Replay → ingestor (Macarius); the Job: Isaac | Field subset, BIC table, batch size, 4xx vs 5xx and retry, SGD only | **agreed** (Isaac, 2 Nov 2026), sample [`pacs008_batch.json`](../../apps/ingestor/fixtures/pacs008_batch.json) | Mon 2 Nov |
| Labels file and splits ([Labels](#labels-labelscsv-evaluation-only)) | Macarius → Isaac | Columns, the split by attempt start, laundering in no attempt counted by day | **draft**, sample [`labels.csv`](../../apps/dataset/fixtures/tiny/labels.csv) (the fixture's); Isaac to approve | Wed 4 Nov |

Also part of: [Kafka transaction event](streaming.md#interfaces).

## Open

- [ ] How the Kubernetes replay Job gets its workload: baked into the image or mounted (Macarius, with Isaac). Proposed on [#29](https://github.com/10x-Slackers/CSC3104/issues/29): mounted, because `altman-medium-bt` (817 MB) is too big to bake into an image.
- [x] Rule parameters and hop limit: `HOP_LIMIT = 4` stays (in the privacy node); planted chains are hand-written in the fixture, and the detection rules are in [streaming.md](streaming.md#detection-rule).
- [x] How replay output reaches the ingestor: HTTP POST of pacs.008 batches ([Ingestor](#ingestor)).
- [x] How Altman-labelled transfers are scored: offline, per attempt and pattern type ([evaluation.md](../evaluation.md#offline-detection-evaluation)).
