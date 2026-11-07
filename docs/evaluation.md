# Evaluation

Owner: Isaac · Evidence supplied by every owner · Status: offline evaluator built; **pending work**: the evaluation runs

Every claim needs a measured indicator, and the main results need graphs. Detection quality is measured **offline**, and the system (latency, scaling, failures) on the **cluster**.

## Rules

- **Two kinds of test data.** The planted chains in the committed fixture are a deterministic **acceptance suite**: they check that tracing works, never how well detection works. Detection quality is measured against **Altman's labelled laundering attempts** ([data.md](contracts/data.md#labels-labelscsv-evaluation-only)).
- **Dev/eval split by attempt start.** Dev = attempts that start 1–5 Sep 2022, eval = attempts that start 6–10 Sep. Each run reads its own days plus the following days so attempts can finish, and counts false positives only on its own days. Days 11–18 are the generator finishing its patterns and are never used for false-positive counts. Tune only on dev; run eval **once**. Both read all of `altman-small` ([data.md](contracts/data.md#workloads)), so an eval run first reads 1–5 Sep as rule history (`last:` keys, chain windows) without counting them; starting empty on 6 Sep tripled the ordinary payments flagged in the exploratory sweep (4.80% vs 1.54%), because every payee looks new. Laundering transfers in no Altman attempt are assigned to dev or eval by the day they happen.
- **Scope.** The rules target **cross-bank bank transfers** (GIRO and MEPS+). Report recall over that scope, and account for every other laundering transfer separately: same-bank, cheque/card/cash (mostly from bank-70 hub accounts, whose laundering no rule separates from their ordinary payments), and the days after ordinary activity stops (after 10 Sep on `altman-small`, set as `ORDINARY_END` in `score.py`; a few stray ordinary payments run later).
- **Named SLIs, percentiles not means.** End-to-end detection latency (p50, p95) and alert correctness. SLO targets are chosen after the first baseline and before any comparative run. Never present an unmeasured target as a result.
- **Sound benchmarking** (Hoefler and Belli, SC'15): run each configuration at least 5 times and report the median and p95 with min/max, documenting every factor that varies.
- **Open-loop load:** replay at fixed rates (`--rate`, events/s) for load tests, which avoids coordinated omission; `--speed 60` is for the demo only.
- **Failures as RTO and RPO** (MAS TRM §8.2.1): RTO = time to the next alert, RPO = alerts lost. Define the steady state before injecting a fault. Faults: `kubectl delete pod`, `kubectl scale --replicas=0`, a temporary deny-all NetworkPolicy. k3d is one node, so failover there is pod-level only.
- Explain trade-offs and bottlenecks, including any case where scaling doesn't help (e.g. bank B's hot account, which carries 13% of its events).

## Offline detection evaluation

`apps/evaluation/` (Isaac): `uv run python -m evaluation.offline apps/dataset/out/altman-small --split dev` prints the report below as JSON. A run over `altman-small` takes about a minute and 3 GB.

- **Input:** `prepare.py`'s bank files and labels file (`altman-small`, and `altman-medium` for experiment 14), with the attempt ID and `altman_pattern` per laundering transfer.
- **Same code as the live path:** each bank's privacy node reads its bank file with the live scorer (the node's CSV mode, `LocalBank`), then searches every local detection, as the batch run on k3d does after replay. Peer `Trace` and `Deliver` calls are direct method calls between the three nodes. No Kafka, Valkey or gRPC servers.
- **Tiers and cases** follow [case.md](contracts/case.md#cases-and-alerts) until the case service has them: every rule hit is Tier 1 at its bank; a stored chain is Tier 2 at every bank on it when at least 2 banks' claims are rule-backed, otherwise Tier 1 at the rule-backed bank. At each bank, results sharing a transfer join one case at the highest tier, which promotes Tier 1 hits. Pending searches (none offline, since every bank is up) and fully internal chains (not built in the node yet) make no Tier 1 case.
- **Output:** per Altman pattern type, attempts caught at any tier and at Tier 2 (cross-bank), and the attempts with an in-scope transfer and how many of those were caught; an attempt counts as caught if any of its transfers is in a case. Overall: **payment-level** recall over the scope, which counts laundering in no attempt (about a third of HI-Small's in-scope laundering); laundering by scope reason; precision; the false-positive rate as ordinary payments flagged / all payments on the split's own days; and analyst load (cases per bank per day, by tier).
- **Where it runs:** a laptop for the full splits; CI runs it on the committed fixture as a regression test.
- **Bridge to the cluster:** replay the `altman-1-6sep` workload (1 Sep to the end of the first eval day) through k3d at `--speed 0`, so the live rules have the same history as the offline run, and compare 6 Sep's stored cases with the offline run over the same workload. They must match (experiment 13).

## Experiments

Experiments 2–4 and 14 run offline; the rest run on k3d, and 10 on AWS.

| # | Question | Setup | Report |
| --- | --- | --- | --- |
| 1 | Does tracing work? | Fixture scenarios (`m1`, `l1`, `o1`, `x1`, `v1`) end to end | Every expected link, state and tier |
| 2 | Detection vs Altman | Rules tuned on dev, eval run once | Per pattern type: Tier 1 vs Tier 2 recall (attempts), payment-level recall, precision, cases per bank per day |
| 3 | Thresholds | Dev only: chain ratio band and window, the quick-repeat window, amount floor, rule-backed banks for Tier 2 (1/2/3) (exploratory starting point: 0.90–1.10, 72 h, 24 h, no floor) | False positives vs recall; the chosen point |
| 4 | Hop limit | `HOP_LIMIT` 4/6/8 | Chain recall on Altman cycles vs search latency |
| 5 | Does scaling help? | Fixed low and high rates; fixed replicas vs KEDA; 6 partitions | Throughput, lag, replicas, CPU/memory, p50/p95 latency; KEDA reaction time (polling 30 s, cooldown 300 s) |
| 6 | HA vs single instance | The same broker, PostgreSQL or privacy-node pod kill in the HA bank and a single bank | RTO, RPO; searches complete with one node replica killed mid-replay |
| 7 | Bank outage | Take C offline for N minutes under steady load | Backlog, request rate C sees on return, time to drain, with and without backoff and jitter |
| 8 | Clock skew | One bank under libfaketime | Absolute deadlines time out; relative deadlines complete |
| 9 | Bulkhead | Disable the summary service during a replay | Alerts still appear; drafts missing |
| 10 | Lambda | Ingestor and summary under load | Cold and warm latency separately; CloudWatch invocations, errors, duration |
| 11 | Valkey loss | Delete Valkey and rebuild by replay from `earliest` | Rebuild time = RTO; RPO 0 |
| 12 | Key rotation | Rotate a pair key during a live replay | No missed links |
| 13 | Offline = live | The `altman-1-6sep` workload through k3d at `--speed 0` vs the offline run over the same workload | Identical cases and tiers on 6 Sep |
| 14 | Out of sample | Rules frozen from HI-Small, run once on `altman-medium`, counting its days with ordinary activity (1–16 Sep) ([data.md](contracts/data.md#workloads)) | Recall (attempts and payments), false-positive rate |

Security evidence (sentinel values never leave a bank; bad certificates and claims rejected; cross-bank store access denied) is listed in [security.md](contracts/security.md#tests).

## Metric definitions

| Metric | From → to | Notes |
| --- | --- | --- |
| End-to-end detection latency | Kafka record timestamp at ingestion → durable case or alert at the same bank | Headline latency |
| Peer-trace latency | Search start → search result | Report separately |
| Analyst load | Cases per bank per day, by tier | From the offline evaluator |

**Clocks:** latency is measured on k3d, where every pod shares the host clock, and from ingestion to an alert at the same bank, so no cross-machine clocks are compared. Each service logs `payment_id` or `chain_digest` with its own timestamp as JSON to stdout, and pandas joins them. Pending work for every owner, by Fri 6 Nov.

**Metric sources** (no monitoring stack): `kubectl top`, `kubectl get hpa`, `kafka-consumer-groups.sh`, CloudWatch for Lambda, and JSON logs. Results are presented as RED (rate, errors, duration) and USE (utilisation, saturation, errors).

## Run record

Record this for every run, e.g. as `results/<run-id>/run.json` (location TBC):

- dataset split and workload, seed, event rate
- rule settings
- deployment (k3d or EKS, HA or single) and resources
- software revision (git SHA)
- fault timing, if any

## Demo scenario

The scripted demo runs these steps in order:

1. Replay an Altman slice across Banks A, B and C. Show local (Tier 1) suspicion turning into a verified cross-bank (Tier 2) alert.
2. Open the bank-local cases: one case per involved bank, with the bank's own legs and other banks' tokens. Inspect a peer payload to show the disclosure boundary.
3. Repeat messages and restart a worker. The existing case is kept and not duplicated.
4. Take one bank offline. A dependent search stays pending while a separate search among the available banks continues. Restore the bank and show recovery. Kill a pod in the HA bank and show it carries on.
5. Show the load experiment results, the offline detection results, and a summary draft with its evidence.

## Results

TODO: add graphs and tables once runs exist.

### Exploratory sweep (not the offline evaluator)

Macarius, 31 Oct 2026, re-run on 7 Nov on the offline evaluator's definitions: a scratchpad script that reimplements the chain, new payee and quick repeat rules over `altman-small` and `altman-medium` (SGD, three banks, every rule hit counted as caught, history from 1 Sep, bank transfers only). Laundering counts by its attempt's start day, laundering in no attempt by its own day; scope = cross-bank bank transfers up to `ORDINARY_END` (10 Sep on `altman-small`, 16 Sep on `altman-medium`); false positives = ordinary payments flagged / all payments on the split's own days. It chose the starting thresholds in [streaming.md](contracts/streaming.md#detection-rule); the offline evaluator replaces it. On `altman-small` it flags exactly the payments in the evaluator's cases (178,954), so the two give the same numbers.

| Run | Attempts caught (in scope) | Laundering payments caught (in scope) | In no attempt | Ordinary flagged |
| --- | --- | --- | --- | --- |
| HI-Small dev (attempts starting 1–5 Sep) | 154 of 158 (97.5%) | 1,441 of 1,471 (98.0%) | 412 of 430 (95.8%) | 6.33% |
| HI-Small eval (attempts starting 6–10 Sep) | 139 of 146 (95.2%) | 963 of 1,028 (93.7%) | 358 of 410 (87.3%) | 1.54% |
| HI-Medium (attempts starting 1–16 Sep), thresholds unchanged | 2,283 of 2,459 (92.8%) | 16,461 of 17,777 (92.6%) | 4,441 of 5,352 (83.0%) | 2.98% |

Dev's false-positive rate is higher because 1–2 Sep have no payment history, so every payee is new: 11.0% and 7.2% of those days' payments are flagged as ordinary, against 1.5–3.4% on 3–5 Sep. Out of scope and accounted for separately (dev / eval): 847 / 941 same-bank, 243 / 220 cross-bank cheque, card, cash or Bitcoin, and 7 / 399 after 10 Sep laundering transfers.
