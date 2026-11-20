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
- **One difference.** Offline, every event is scored before any search runs. On a cluster, searches start while payments still arrive ([protocol.md](protocol.md#search-states)). A cluster run can therefore end with a long chain cut into different partial pieces, so that a case lists fewer of a bank's payments than offline. A difference of that kind is not a lost case; the runs below showed none.
- **Report.** Per Altman pattern type: attempts caught at any tier and at Tier 2. Overall: payment-level recall over the scope, laundering by scope reason, precision, ordinary payments flagged (the share of all payments on the split's days that were flagged and are not laundering), and cases per bank per day by tier.

## Experiments

| # | Question | How to run it | Report |
| --- | --- | --- | --- |
| 1 | Does tracing work? | `make replay WORKLOAD=tiny && make cluster-check` | Every bank's cases equal the offline evaluator's |
| 2 | Detection against Altman's labels | `evaluate workloads/altman-small --split dev`, then `--split eval` once | Recall by pattern and by payment, precision, cases per bank per day |
| 3 | Thresholds | Dev only: change `CHAIN_RATIO` in [scorer.py](../src/mulechain/scorer.py) | False positives against recall |
| 4 | Hop limit | `evaluate ... --hop-limit 8` | Chains completed, and what else changes |
| 5 | Does scaling help? | `make replay WORKLOAD=slice-24h ARGS="--rate 1500"`, and again with `--speed 0`, each with KEDA and with the scorers held at 1 (`kubectl annotate scaledobject scorer -n bank-a autoscaling.keda.sh/paused-replicas=1`) | Lag, replicas, latency p50/p95 |
| 6 | HA against a single instance | `kubectl delete pod` on a Kafka broker, the PostgreSQL primary or a privacy node, in bank A (replicas) and bank B (single) | RTO, RPO |
| 7 | Bank outage | `kubectl scale deploy/privacy-node -n bank-c --replicas=0` during a replay, then `--replicas=1` | Pending searches, time to drain, RPO |
| 8 | Clock skew | One bank's node with a shifted clock | Searches still complete: deadlines are durations |
| 9 | Bulkhead | `kubectl scale deploy/summary -n bank-a --replicas=0` | Alerts still arrive; case pages show no draft |
| 10 | Lambda | Ingestor and summary under load on AWS | Cold and warm latency; CloudWatch invocations, errors, duration |
| 11 | Valkey loss | `kubectl delete pod -l app=valkey -n bank-c` during a replay, and `make rebuild BANK=a` | Time until lag, which rises when the scorers start again from the topic's start, is back to 0 (RTO); state and cases unchanged (RPO 0) |
| 12 | Key rotation | `make key-add`, five minutes, `make key-drop`, with replays between and during | No missed links: cases equal offline |
| 13 | Offline = cluster | Replay a workload with `--speed 0`, then `diff` the output of `make cases` and of `evaluate --cases` | Identical cases and tiers |
| 14 | Out of sample | Rules frozen, run once on a larger Altman dataset | Recall and false-positive rate |

## Metrics and where they come from

| Metric | From → to | Source |
| --- | --- | --- |
| End-to-end detection latency | Kafka record timestamp at ingestion → case stored at the same bank | The scorer's log line per payment that made a link or hit (`payment_id`, `kafka_ts`) and the case service's `stored` lines (`payments`, `ts`), matched by payment ID: from the last of a case's payments to the case's first stored message |
| Peer-trace latency | Search attempt start → its result | The node's `"event": "search"` lines (`ms`, `state`, `attempt`) |
| Delivery | One `Deliver` call | The node's `"event": "deliver"` lines |
| RTO | Fault → service again | The longest gap between accepted replay batches (replay's log) and between `stored` lines at the faulted bank; for Valkey loss, until lag has risen and is back to 0 |
| RPO | Cases lost | `make cases` against `evaluate --cases` |
| Analyst load | Cases per bank per day, by tier | The offline evaluator |

[measure.py](../deploy/measure.py) computes the latencies and gaps from the collected log lines; its docstring has the commands. Every service logs one JSON object per line to stdout with its own `ts` ([`log`](../src/mulechain/__init__.py)). On k3d all pods share the host clock, and latency is measured from ingestion to a case at the same bank, so no clocks on different machines are compared. Where to look while a run is going is in [deployment.md](deployment.md#monitoring).

## Results

Measured on 20 Nov 2026. The local cluster is k3d (k3s, one node) in a VM with 8 vCPUs and 15 GiB of memory, with bank A running replicas. The AWS cluster is EKS in `us-east-1` with three nodes, the Lambdas and ElastiCache ([deployment.md](deployment.md#aws)), with every bank running replicas. Each cluster run is a single sample on a new cluster, and every run below ended with exactly the offline evaluator's cases unless it says otherwise.

### Correctness and offline = cluster (experiments 1, 12, 13)

| Run | Result |
| --- | --- |
| `make k3d` from nothing, images cached | Ready in 3 min 43 s |
| Tiny workload at the default 60× speed, then `make cluster-check` | 14 cases (A 5, B 5, C 4); all 57 checks pass (the cases, 45 network paths, 11 mTLS checks) |
| `slice-6h` at `--speed 0` (17,295 bank transfers) | Replayed in 11 to 13 s; scorers scale 1 → 6 on lag in every bank; 11,291 cases |
| `altman-1-6sep` at `--speed 0` (425,385 bank transfers, six days of data) | Replayed in 235 s; 146,897 cases, identical to offline |
| Key rotation: bank B gets key 2 and tiny is replayed at once; five minutes later B drops key 1, bank A gets key 2 and `slice-6h` is replayed at once; six minutes later `slice-6h-sep6` is replayed | Cases identical to offline after each step: 14, then 11,305, then 21,002 (two six-hour slices and tiny) |

### Latency (k3d)

`slice-6h` at 60× (17,295 bank transfers in 6 minutes), no faults:

| Measure | p50 | p95 | Max | n |
| --- | --- | --- | --- | --- |
| Detection latency, ingestion → case stored | 0.07 s | 0.13 s | 4.17 s | 11,291 cases |
| A search that ends confirmed | 28.8 ms | 29.1 ms | 29.1 ms | 3 searches |

Almost every case in a slice is a single-bank hit, so the detection latency is mostly the path Kafka → scorer → Valkey → privacy node → case service. The three cross-bank searches are the only ones this slice has.

### Does scaling help? (experiment 5)

| Load | Scorers | Peak lag per bank | Detection latency p50 / p95 / max | Time to accept the load |
| --- | --- | --- | --- | --- |
| `slice-24h` at `--rate 1500` (open loop, 69,109 bank transfers in 92 s) | 1, fixed | 1,778 to 2,521 | 0.26 / 1.14 / 3.31 s | 92 s |
| The same | KEDA, 1 → 6 in every bank | 1,484 to 2,132 | 0.28 / 1.20 / 2.47 s | 92 s |
| `slice-24h` at `--speed 0` (a burst, as fast as the ingestors accept) | 1, fixed | 3,282 to 5,523 | 12.2 / 21.5 / 24.6 s | 43 s |
| The same | KEDA, 1 → 6 in every bank | 2,917 to 4,339 | 8.1 / 16.1 / 17.7 s | 54 s |

- **At 1,500 events a second one scorer per bank keeps up**, so the extra scorers change nothing: the lag stays near 2,000 events and a case is stored about a quarter of a second after its last payment.
- **In a burst the extra scorers cut the latency by about a third at p50 and a quarter at p95**, and the lag is back to 0 about 13 s after the last batch either way. The comparison is not a clean one on one machine: every pod shares the same 8 cores, so with 18 scorers running the ingestors accept the burst more slowly (54 s against 43 s).
- Lag is sampled only every 10 to 35 s here, so the peaks are lower bounds.
- Scaling out and back moves partitions between workers while they score. Every run still ended with the offline cases (44,983), so no event was skipped or scored against the wrong state.

### Faults (experiments 6, 7, 8, 9, 11)

Pod kills during `slice-6h` at 60×. A pause is the longest gap in the bank's accepted batches or stored cases after the kill; without a fault the gap is at most 1.0 s, the replay's own rhythm at this speed.

| Fault | Pause | Cases lost (RPO) |
| --- | --- | --- |
| Bank A (replicas): a Kafka broker, the PostgreSQL primary and a privacy node killed, 50 s apart | None beyond 1.0 s for any of the three | 0 |
| Bank B (single instances): the Kafka broker killed | 25.5 s and 27.3 s in two runs: no batch accepted and no case stored | 0 |
| Bank B: the PostgreSQL primary killed | PostgreSQL serves its open connections for 180 s more and then stops (CloudNativePG's [smart shutdown](https://cloudnative-pg.io/documentation/current/instance_manager/)): no case stored for 14.0 s. Batches are accepted throughout | 0 |
| Bank B: the privacy node killed | None beyond 1.5 s: the replacement pod starts while the old one shuts down. Killed during PostgreSQL's 180 s shutdown instead, the new node can't connect until PostgreSQL is back: 104 s with no case stored | 0 |
| Bank C's privacy node off for 240 s | Bank C stores no case until it is back (the longest wait 243.6 s), then all of them. Banks A and B are not slowed: latency p95 0.11 s. The one search that needed C waited through 4 unanswered attempts and was confirmed 7.3 s after C returned | 0 |
| Bank C's privacy node with its clock 10 minutes ahead (libfaketime, wall clock only), tiny and then `slice-6h` at `--speed 0` | None. All 57 cluster checks pass and every search ends: 10 confirmed, 1 partial, 5 inside one bank, none failed or left pending. A deadline travels as a duration (`time_left` in [node.py](../src/mulechain/node/node.py)), so the banks need not agree on the time | 0 |
| Summary service off at bank A (bulkhead), tiny replayed | All 5 of A's cases arrive. A case page takes 2.7 to 4.1 ms and shows no draft. With the service back, the first page with a draft takes 1.06 s and later ones 4.7 to 5.5 ms | 0 |
| Valkey lost after six days of data (577,000 to 629,000 keys a bank): all three banks' Valkey pods deleted, then `make rebuild` for all three at once | Lag back to 0 after 150 s. Key counts and cases identical before and after | 0 |
| Bank C's Valkey pod deleted during the replay, and no command run | The scorer stops, restarts and reads the topic from the start. Bank C stores no case for 19.0 s; its latency for the run is p95 2.5 s, max 19.5 s | 0 |

### AWS (experiments 1, 10, 11, 13)

`make eks` created the cluster, the six Lambdas and the three ElastiCache groups in one command. Not run on EKS: a Valkey lost with no command, which is the k3d result above.

| Run | Result |
| --- | --- |
| Tiny at 60×, then `make cluster-check` | 14 cases; the 53 checks that apply on EKS pass |
| `slice-6h` at `--speed 0` | 17,295 bank transfers accepted in 44.3 s by the ingestor Lambdas; scorers scale 1 → 6 in every bank; 11,305 cases with tiny, identical to offline |
| Latency in that burst | Detection p50 31.9 s, p95 37.7 s: the scorers work through a backlog. A confirmed search takes 89 ms at p50 |
| Ingestor Lambda (experiment 10) | 101 to 115 invocations a bank, no errors, no throttles. Duration p50 about 262 ms, p95 0.89 to 1.15 s, max 2.7 to 5.0 s; a cold start adds 210 to 267 ms |
| Summary Lambda | 6 invocations a bank at about 1.9 ms; a cold start adds 139 to 144 ms. The first case page that calls it takes 673 to 754 ms, later ones 46 to 76 ms |
| `make rebuild BANK=a` on ElastiCache (30,669 keys) | Lag back to 0 after 48 s; key count and cases unchanged |

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

### Thresholds and hop limit, offline on dev (experiments 3, 4)

| Chain rule's amount ratio (`CHAIN_RATIO`) | Attempts caught, any tier | Attempts reaching Tier 2 | Payment recall | Ordinary payments flagged | Tier 2 cases per bank per day |
| --- | --- | --- | --- | --- | --- |
| 0.95 to 1.05 | 152 of 179 | 13 | 97.4% | 6.28% | 0.8 to 1.2 |
| 0.90 to 1.10 (used) | 155 of 179 | 25 | 98.0% | 6.33% | 2.0 to 2.4 |
| 0.85 to 1.15 | 159 of 179 | 32 | 98.3% | 6.39% | 2.8 to 3.0 |

- The band decides how many attempts become cross-bank cases. Narrowing it halves the attempts that reach Tier 2. Widening it adds seven for about 40% more Tier 2 cases to review, and it moves the flagged share by less than 0.1 points either way, because almost all flags come from the new payee rule.
- With `--hop-limit 8` the searches end with 16 partial chains where the default 4 ends with 44, and nothing else in the report changes: the same attempts caught at each tier and the same cases per bank per day. A partial chain already joins the same case and alerts like a confirmed one ([cases.md](cases.md)).

### Not measured

**Out of sample (experiment 14)** has not been run.
