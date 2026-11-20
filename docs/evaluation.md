# Evaluation

Detection quality is measured **offline** against labelled data, and the system (correctness under faults, scaling, recovery, latency) on the **cluster**. The two are tied together by comparing cases: a cluster run is expected to end with exactly the cases the offline evaluator gives for the same workload, with one exception described [below](#the-offline-evaluator).

## Rules

- **Two kinds of test data.** The planted chains in [workloads/tiny](../workloads/tiny) are a deterministic acceptance suite: they check that tracing works, never how well detection works. Detection quality is measured against Altman's labelled laundering attempts ([data.md](data.md)).
- **Dev/eval split by attempt start.** Dev is the attempts that start 1 to 5 Sep 2022, eval those that start 6 to 10 Sep. Both runs read all of `altman-small`, so an eval run has 1 to 5 Sep as rule history without counting it. Thresholds are tuned on dev only, and eval is run once.
- **Scope.** The rules target cross-bank bank transfers (GIRO and MEPS+). Recall is reported over that scope, and every other laundering transfer is accounted for by reason: same bank, another payment type, or after ordinary activity stops (`ORDINARY_END`).
- **Percentiles, not means**, for latency.
- **Open-loop load.** Load tests send at a fixed rate whatever the system's speed (`--rate`, events a second). If the sender waited for each reply instead, a slow system would receive less load and look faster than it is. `--speed 0` does wait for the banks, so it is only for filling the system quickly.
- **Failures as RTO and RPO.** RTO is the recovery time: how long until the service works again. RPO is the recovery point: how much was lost. RPO is measured by comparing a run's cases with the offline evaluator's, where any lost or extra case shows. k3d is one node, so failover there is pod-level only.

## The offline evaluator

[evaluate.py](../src/mulechain/evaluate.py) runs all three banks in one process:

```sh
uv run python -m mulechain.evaluate workloads/altman-small --split eval    # the detection report, as JSON
uv run python -m mulechain.evaluate workloads/slice-6h --cases             # the cases, to compare with a cluster
uv run python -m mulechain.evaluate workloads/altman-small --split dev --hop-limit 8
```

- **Same code as the cluster.** Each bank's `Scorer` writes into its own in-process Valkey (fakeredis). Its `Node` reads the hits, searches and delivers, and its case messages go into a real `CaseStore` (SQLite). Peer calls are direct method calls. There is no Kafka, gRPC server or network.
- **One difference.** Offline, every event is scored before any search runs. On a cluster, searches start while payments still arrive: a search is tried again while a bank on its path doesn't hold a transfer yet, and waits `INGESTION_GRACE_SECONDS` before it decides that a link stays inside its bank. A cluster run can therefore end with a long chain cut into different partial pieces, so that a case lists fewer of a bank's payments than offline. A difference of that kind is not a lost case; the runs below showed none.
- **Report.** Per Altman pattern type: attempts caught at any tier and at Tier 2. Overall: payment-level recall over the scope, laundering by scope reason, precision, ordinary payments flagged, and cases per bank per day by tier.

## Experiments

| # | Question | How to run it | Report |
| --- | --- | --- | --- |
| 1 | Does tracing work? | `make replay WORKLOAD=tiny && make cluster-check` | Every bank's cases equal the offline evaluator's |
| 2 | Detection against Altman's labels | `evaluate workloads/altman-small --split dev`, then `--split eval` once | Recall by pattern and by payment, precision, cases per bank per day |
| 3 | Thresholds | Dev only: change `CHAIN_RATIO` in [scorer.py](../src/mulechain/scorer.py) | False positives against recall |
| 4 | Hop limit | `evaluate ... --hop-limit 8` | Chains completed against search time |
| 5 | Does scaling help? | `make replay WORKLOAD=slice-6h ARGS="--rate 1500"`, with KEDA and with `maxReplicaCount` 1 | Throughput, lag (`kubectl get hpa -A`), replicas, latency p50/p95 |
| 6 | HA against a single instance | `kubectl delete pod` on a Kafka broker, the PostgreSQL primary or a privacy node, in bank A (replicas) and bank B (single) | RTO, RPO |
| 7 | Bank outage | `kubectl scale deploy/privacy-node -n bank-c --replicas=0` during a replay, then `--replicas=1` | Pending searches, time to drain, RPO |
| 8 | Clock skew | One bank's node with a shifted clock | Searches still complete: deadlines are durations |
| 9 | Bulkhead | `kubectl scale deploy/summary -n bank-a --replicas=0` | Alerts still arrive; case pages show no draft |
| 10 | Lambda | Ingestor and summary under load on AWS | Cold and warm latency; CloudWatch invocations, errors, duration |
| 11 | Valkey loss | `make rebuild BANK=a` | Time until lag is 0 (RTO); state and cases unchanged (RPO 0) |
| 12 | Key rotation | `make key-add`, five minutes, `make key-drop`, with replays between and during | No missed links: cases equal offline |
| 13 | Offline = cluster | Replay a workload with `--speed 0`, then `diff` the output of `make cases` and of `evaluate --cases` | Identical cases and tiers |
| 14 | Out of sample | Rules frozen, run once on a larger Altman dataset | Recall and false-positive rate |

## Metrics and where they come from

| Metric | From → to | Source |
| --- | --- | --- |
| End-to-end detection latency | Kafka record timestamp at ingestion → case stored at the same bank | The scorer's log line per payment that made a link or hit (`payment_id`, `kafka_ts`) and the case service's `stored` lines (`payments`, `ts`, `tier`), matched by payment ID. The repository has no script for this join |
| Peer-trace latency | Search attempt start → its result | The node's `"event": "search"` lines (`ms`, `state`, `attempt`) |
| Delivery | One `Deliver` call | The node's `"event": "deliver"` lines |
| RTO | Fault → service again | Gaps between accepted replay batches (replay's log) and between `stored` lines; for Valkey loss, until lag is 0 |
| RPO | Cases lost | `make cases` against `evaluate --cases` |
| Analyst load | Cases per bank per day, by tier | The offline evaluator |

Every service logs one JSON object per line to stdout with its own `ts` ([`log`](../src/mulechain/__init__.py)). On k3d all pods share the host clock, and latency is measured from ingestion to a case at the same bank, so no clocks on different machines are compared. Where to look while a run is going is in [deployment.md](deployment.md#monitoring).

## Results

Measured on 20 Nov 2026. The cluster is k3d (k3s, one node) in a VM with 8 vCPUs and 15 GiB of memory, with bank A running replicas. Each cluster run is a single sample.

### Cluster correctness (experiments 1, 7, 11, 12, 13)

| Run | Result |
| --- | --- |
| `make k3d` from nothing, images cached | Ready in 3 min 43 s |
| Tiny workload at the default 60× speed, then `make cluster-check` | 14 cases (A 5, B 5, C 4), equal to offline; all 57 checks pass (the cases, 45 network paths, 11 mTLS checks) |
| `slice-6h` at `--speed 0` (17,295 bank transfers) | Replayed in 11 to 13 s; scorers scale 1 → 6 on lag in every bank |
| The same replay with bank C's privacy node off, C restored afterwards, and both of bank A's node pods deleted during recovery | Pending cases show "Waiting for bank C"; after recovery 11,291 cases, identical to offline (none lost, none extra) |
| Valkey loss: bank A's Valkey pod deleted, then `make rebuild` for A and B (30,669 and 35,176 keys) | Lag back to 0 within 45 s; key counts, hits, offsets and cases identical; no duplicate searches or messages |
| Key rotation: bank B gets key 2 and tiny is replayed at once; five minutes later B drops key 1, bank A gets key 2 and `slice-6h` is replayed at once; six minutes later `slice-6h-sep6` is replayed | Cases identical to offline after each step: 14, then 11,305, then 21,002 (two six-hour slices and tiny) |

### Detection, offline (experiment 2)

`altman-small`, one run per split, about 8 minutes each (8 cores). Ranges are over the three banks.

| Split | Attempts caught, any tier | Attempts reaching Tier 2 | Payment recall (in scope) | Ordinary payments flagged | Precision | Cases per bank per day, Tier 1 / Tier 2 |
| --- | --- | --- | --- | --- | --- | --- |
| Dev (1 to 5 Sep) | 155 of 179 | 25 | 98.0% (1,441 of 1,471) | 6.33% | 0.82% | 8,883 to 9,566 / 2.0 to 2.4 |
| Eval (6 to 10 Sep) | 146 of 184 | 24 | 93.7% (963 of 1,028) | 1.54% | 4.18% | 2,170 to 2,314 / 5.4 to 6.0 |

Attempts caught by pattern, as caught / attempts (reaching Tier 2):

| Split | Cycle | Random | Stack | Fan-out | Fan-in | Scatter-gather | Gather-scatter | Bipartite |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Dev | 27 / 31 (14) | 15 / 15 (10) | 15 / 17 (1) | 19 / 22 (0) | 18 / 20 (0) | 18 / 21 (0) | 23 / 27 (0) | 20 / 26 (0) |
| Eval | 21 / 23 (12) | 18 / 26 (9) | 19 / 23 (1) | 20 / 26 (1) | 16 / 20 (0) | 18 / 23 (1) | 20 / 24 (0) | 14 / 19 (0) |

- Cross-bank chains (Tier 2) are almost all cycles and random walks, the patterns that pass money on from bank to bank. The fan and scatter patterns are caught by the single-bank rules at Tier 1.
- The dev days start with no payment history, so the new payee rule fires on every first payment. The eval run has 1 to 5 Sep as history.
- Laundering outside the scope on the eval days: 941 transfers inside one bank, 220 by cheque, card, cash or Bitcoin, and 399 after ordinary activity stops.

Results for experiments 5, 6, 8, 9, 10 and 14 are not recorded here.
