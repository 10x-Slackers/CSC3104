# Data

The payments the banks see: where they come from, how they become three banks' files, and how they enter a bank. The detection that follows is in [detection.md](detection.md).

```text
IBM AML dataset → prepare.py → workloads/<name>/ → replay.py → pacs.008 over HTTP → ingestor → Kafka
```

## Source

The IBM AML synthetic dataset (Altman et al., NeurIPS 2023 Datasets and Benchmarks): Kaggle `ealtman2019/ibm-transactions-for-anti-money-laundering-aml`, version 8, files `HI-Small_Trans.csv` (the transfers) and `HI-Small_Patterns.txt` (the laundering attempts). It needs no Kaggle login. The first run downloads about 476 MB to `~/.cache/kagglehub`.

| Fact | Value |
| --- | --- |
| Transfers | 5,078,345, from 1 to 18 Sep 2022, at minute resolution |
| Labelled laundering (Altman's own label) | 5,177 transfers |
| Laundering attempts | 370 in the patterns file, of 8 types (fan-in, fan-out, cycle and so on) |
| Currencies | 15, including Bitcoin |
| Ordinary activity | Ends on 10 Sep. After that the generator only finishes its laundering patterns, so 11–18 Sep is never used to count false positives |

The dataset has no transaction ID, its bank IDs are padded inconsistently, and its account IDs are unique only within a bank. Preparation fixes all three.

## Preparation

[prepare.py](../src/mulechain/prepare.py) turns the dataset into a **workload**: one event file per bank plus the labels. Nothing in it is random and the dataset version is pinned (`DATASET`), so a workload can be rebuilt exactly.

| Step | Rule |
| --- | --- |
| Kept transfers | Every currency. Only self-transfers (the same account on both sides) are dropped, leaving 4,487,133 transfers, 5,166 of them laundering |
| Amounts in SGD | The amount paid goes to US dollars at the dataset's own rate (the median of received / paid over payments received in US dollars), then to SGD at MAS's September 2022 average, US$1 = S$1.4136 (`SGD_PER_USD`, [SingStat](https://data.gov.sg/datasets/d_b2b7ffe00aaec3936ed379369fdf531b/view)). Rounded to cents, at least S$0.01 |
| Banks | `int(bank ID) % 3`: 0 is A, 1 is B, 2 is C. A gets 30.0 % of the events, B 38.1 %, C 32.0 %, and 65.3 % of transfers cross banks. Folding many banks into three makes some of Altman's cross-bank transfers intra-bank |
| Accounts | `<bank ID>-<account>`, for example `3208-8000F4580` |
| `payment_id` | The first 16 hex characters of `sha256("<file>:<source row>")` (`pid`), so a transfer keeps its ID in every workload |
| Payment types | Altman's `ACH` becomes `GIRO` and `Wire` becomes `MEPS+`, Singapore's bank transfer systems (`FORMATS`). Cheque, card, cash and Bitcoin keep their names |
| Time | Altman time is read as UTC and written as ISO 8601, for example `2022-09-05T09:20:00Z` |
| Events | Each transfer becomes two events with one `payment_id`: `out` at the sender's bank and `in` at the receiver's. Each bank file is in time order |
| Attempts | A laundering transfer listed in the patterns file gets that attempt's number and type |

Only **GIRO and MEPS+** are bank transfers (`BANK_TRANSFERS` in [`__init__.py`](../src/mulechain/__init__.py)). They are the only payments replayed, ingested and scored. The other types stay in the bank files so the evaluator can count what the rules can't reach ([evaluation.md](evaluation.md)).

## Workload files

A workload is a directory under `workloads/`. Only `workloads/tiny` is committed; the rest are built locally and ignored by git.

**`bank_A.csv`, `bank_B.csv`, `bank_C.csv`.** One row per event. Each bank only ever reads its own file.

| Column | Example | Notes |
| --- | --- | --- |
| `payment_id` | `p06` | The same at both banks, like the end-to-end ID both banks see on a real payment ([ISO 20022](https://www.iso20022.org/standardsrepository/type/PaymentIdentification13)). Both banks derive the transfer's token from it ([security.md](security.md)) |
| `event_time` | `2022-09-05T09:10:00Z` | When the payment happened, not when it is replayed |
| `bank` | `B` | This bank |
| `direction` | `in` | `out` at the sender's bank, `in` at the receiver's |
| `account` | `B-m1` | This bank's customer |
| `counterparty_account` | `A-m0` | The other side's account, which Singapore wires carry (MAS Notice 626 ¶11.4). Used by two rules; never sent to another bank |
| `counterparty_bank` | `A` | Which bank to ask about this transfer |
| `amount` | `10000.0` | SGD |
| `payment_format` | `MEPS+` | `GIRO`, `MEPS+`, `Cheque`, `Credit Card`, `Cash` or `Bitcoin` |

**`labels.csv`.** Only the evaluator reads it; a bank never sees a label. It lists laundering transfers (`altman_laundering` = 1, with `attempt_id` and `altman_pattern` when the transfer is in a listed attempt) and the fixture's planted chains (`chain_id`, `hop`, `scenario`). Any payment not listed is ordinary.

**`workload.json`.** The earliest event time `t0` (replay paces from it), the source, the slice parameters and the counts.

## Workloads

```sh
uv run python -m mulechain.prepare workloads/slice-6h --start 2022-09-05 --hours 6
```

| Workload | Arguments after the output directory | Transfers | Used for |
| --- | --- | --- | --- |
| `tiny` (committed) | `--transfers workloads/tiny/transfers.csv` | 20 | Tests and the cluster checks |
| `slice-6h` | `--start 2022-09-05 --hours 6` | 129,415 | Demo, load and failure runs |
| `slice-24h` | `--start 2022-09-05 --hours 24` | 472,251 | Load runs |
| `altman-small` | none (all of HI-Small) | 4,487,133 | Offline detection quality, both splits |
| `altman-1-6sep` | `--start 2022-09-01 --hours 144` | 2,696,535 | Comparing a cluster run with the offline evaluator |
| `altman-medium` | `--altman HI-Medium` | 29,336,378 | A check on data the thresholds were not tuned on (about 3 GB to download and far more memory to build) |

- The evaluator reads `altman-small` whole and applies the split itself: `--split dev` counts attempts that start on 1–5 Sep, `--split eval` those on 6–10 Sep (`SPLITS` in [evaluate.py](../src/mulechain/evaluate.py)).
- A slice starts with no payment history, so every payee looks new at first and the new payee rule fires more than it would mid-stream. Use slices for load, failure and demo runs, not for detection quality. `altman-1-6sep` starts where the dataset starts, so it misses no history.

## The tiny workload

Twenty hand-written transfers on 5 Sep 2022, already in SGD ([transfers.csv](../workloads/tiny/transfers.csv)). Its planted chains cross banks on every hop. They check behaviour, not detection quality, because they are written to fit the rules. p01 (a cheque) and p04 (a card payment) are never replayed, so 18 transfers reach the banks.

| Chain | Transfers | Path | Result |
| --- | --- | --- | --- |
| none | p01–p05 | mixed; p02 stays inside B | Ordinary, except p05, the one labelled laundering transfer (a FAN-OUT attempt). p03 and p05 are first payments to a payee at another bank: a Tier 1 case each |
| `m1` mule chain | p06 → p07 | A→B→C, 95 % passed on in 15 min | Confirmed, but only B's rule links it: a Tier 1 case at B, no alert |
| `l1` lookalike | p08 → p09 | A→B→C, 40 % passed on | Not linked |
| `o1` loop | p10 → p11 → p12 | A→B→C→A | One confirmed chain, a Tier 2 alert at all three banks. It ends at A because the money returns after A's own payment left, so A's rule links nothing further; the fixture does not exercise loop detection |
| `x1` absent link | p13, p14 | A→B, C→A, middle transfer missing | Not linked across the gap |
| `v1` over the hop limit | p15 → … → p20 | six transfers round A, B, C twice | Partial chains cut at the hop limit, still a Tier 2 alert at all three banks |

Every cross-bank bank transfer here is also a first payment to its payee, so each is a new payee hit and a Tier 1 case unless a chain's case takes it in. The run ends with 14 cases, 5 at A, 5 at B and 4 at C (`EXPECTED` in [test_system.py](../tests/test_system.py); tiers are in [cases.md](cases.md)).

## Replay

[replay.py](../src/mulechain/replay.py) stands in for the payment network. One process reads each bank transfer once (from its `out` event, in time order across the banks, `transfers`) and sends it to the ingestor of every bank on it: p06 goes to A and to B, p02 only to B.

```sh
make replay WORKLOAD=slice-6h ARGS="--speed 0"      # a Job in the cluster
uv run python -m mulechain.replay workloads/tiny --url B=http://localhost:8080 --speed 0
```

| Flag | Meaning |
| --- | --- |
| `--url BANK=URL` | A bank's ingestor, once per bank. A bank without one is skipped. A Lambda function URL is signed with SigV4 (`request_headers` in [serverless.py](../src/mulechain/serverless.py)) |
| `--speed N` | Simulated seconds per real second, default 60: six hours of payments take six minutes. `0` sends as fast as the ingestors take them |
| `--rate N` | A fixed number of events per second, ignoring event times, for load runs. Each transfer makes two events |

- **One queue and one sender per bank** (`send`), so a slow bank delays only its own batches and each bank gets its payments in order.
- **Batches.** A batch is whatever became due while the last one was being sent, at most 100 transfers (`BATCH`). Its `MsgId` is `replay-<bank>-<number>`.
- **Retries.** Anything but a `200` or a `400` sends the same batch again after `random(0, min(30 s, 2^attempt s))`. A `400` can never succeed, so it stops the replay.
- **Back-pressure.** With `--speed N` or `--rate` the clock never waits for a reply, and a slow ingestor shows as a growing `queued` count in the log. With `--speed 0` the reader waits while any bank has 1,000 transfers queued (`BACKLOG`).
- **Logs.** One JSON line per POST (`bank`, `message`, `status`, `attempt`, `transfers`, `ms`, `queued`) and a last line with the total transfers and seconds.

In the cluster, `make replay` runs it as a Job in the `replay` namespace ([job.yaml](../deploy/charts/replay/templates/job.yaml)). On k3d the Job reads `workloads/` mounted from the host; on AWS it first copies the workload from an S3 bucket ([deployment.md](deployment.md)).

## Ingestor

[ingestor.py](../src/mulechain/ingestor.py) is a bank's front door. It takes a batch of payments, checks it, and writes the bank's events to Kafka. It is one function, `handle(body) -> (status, reply)`, behind two adapters: a small HTTP server in a container on k3d (`serve`), and a Lambda function URL with `AWS_IAM` on AWS (`lambda_handler`).

**Input:** a subset of ISO 20022 **pacs.008** (the credit transfer message) as JSON, `POST /`, one batch per message. Bank identifiers are fictional BICs (`BICS`: A `TSTASGSG`, B `TSTBSGSG`, C `TSTCSGSG`). A batch to Bank B holding p06:

```json
{"FIToFICstmrCdtTrf": {
  "GrpHdr": {"MsgId": "replay-B-000001", "CreDtTm": "2022-09-05T09:10:00Z", "NbOfTxs": "1"},
  "CdtTrfTxInf": [{
    "PmtId": {"EndToEndId": "p06"},
    "PmtTpInf": {"LclInstrm": {"Prtry": "MEPS+"}},
    "IntrBkSttlmAmt": {"Ccy": "SGD", "Amt": "10000.00"},
    "AccptncDtTm": "2022-09-05T09:10:00Z",
    "DbtrAcct": {"Id": {"Othr": {"Id": "A-m0"}}}, "DbtrAgt": {"FinInstnId": {"BICFI": "TSTASGSG"}},
    "CdtrAcct": {"Id": {"Othr": {"Id": "B-m1"}}}, "CdtrAgt": {"FinInstnId": {"BICFI": "TSTBSGSG"}}
  }]
}}
```

**Output:** for each transfer, an `out` event if this bank sends it and an `in` event if it receives it; a transfer inside the bank gives both (`events`). The event has the nine fields of a bank file row. The one above becomes this record on `bank-B-transactions`, with key `B-m1`:

```json
{"payment_id": "p06", "event_time": "2022-09-05T09:10:00Z", "bank": "B", "direction": "in",
 "account": "B-m1", "counterparty_account": "A-m0", "counterparty_bank": "A",
 "amount": 10000.0, "payment_format": "MEPS+"}
```

Each record also carries CloudEvents headers (`headers`): `ce_id` is `<payment_id>-<direction>`, `ce_type` is `payment.in` or `payment.out`, `ce_source` is `/bank/<X>/ingestor`, and `ce_time` is the event time. The Kafka timestamp is when the ingestor took the batch, the point latency is measured from.

| Reply | When |
| --- | --- |
| `200 {"events": n}` | Kafka has confirmed every event of the batch |
| `400 {"error": ...}` | The batch is rejected whole and nothing is written: `NbOfTxs` doesn't match, an amount isn't a positive number in SGD, a payment isn't GIRO or MEPS+, a time isn't UTC (`Z`), a BIC is unknown, a field is missing or empty, or neither side of a transfer is this bank |
| `503 {"error": ...}` | Kafka didn't confirm every event within 25 s. Replay sends the batch again |
| `GET` → `200 {"ok": true}` | The readiness probe |

- **Validate at the boundary.** Every check is here, so every event in Kafka is well formed and the scorer trusts its fields (`test_one_bad_transfer_rejects_the_whole_batch`).
- **Acknowledge only after a durable write.** `handle` produces every event, calls `flush()`, and only then answers `200` (`test_a_batch_is_acknowledged_only_once_kafka_has_it`). The producer is idempotent, which also means `acks=all`.
- **A resent batch can write an event twice.** That is safe: the scorer recognises the repeat and changes nothing ([detection.md](detection.md#event-time-repeats-and-late-events)).
- **Round trip.** `test_replay_and_the_ingestor_rebuild_each_banks_events` checks that workload → pacs.008 → events gives each bank its own file's bank transfers back.
- **Settings:** `BANK`, `KAFKA_BOOTSTRAP_SERVERS`, `PORT` (8080). One JSON log line per batch with its status, milliseconds, Kafka timestamp and payment IDs.

Simplifications: JSON instead of the XML a real payment network uses; only the pacs.008 elements detection needs; and transfers inside one bank are sent as pacs.008 too, though a real bank would book them itself.
