# Evaluation

Owner: Isaac · Evidence supplied by every owner · Status: experiments 1–9 and 11–13 run on k3d and offline (19 Nov, [Results](#results)); **pending work**: graphs, experiments 10 and 14

Every claim needs a measured indicator, and the main results need graphs. Detection quality is measured **offline**, and the system (latency, scaling, failures) on the **cluster**.

## Rules

- **Two kinds of test data.** The planted chains in the committed fixture are a deterministic **acceptance suite**: they check that tracing works, never how well detection works. Detection quality is measured against **Altman's labelled laundering attempts** ([data.md](contracts/data.md#labels-labelscsv-evaluation-only)).
- **Dev/eval split by attempt start.** Dev = attempts that start 1–5 Sep 2022, eval = attempts that start 6–10 Sep. Each run reads its own days plus the following days so attempts can finish, and counts false positives only on its own days. Days 11–18 are the generator finishing its patterns and are never used for false-positive counts. Tune only on dev; run eval **once**. Both read all of `altman-small` ([data.md](contracts/data.md#workloads)), so an eval run first reads 1–5 Sep as rule history (`last:` keys, chain windows) without counting them; starting empty on 6 Sep tripled the ordinary payments flagged in the exploratory sweep (4.80% vs 1.54%), because every payee looks new. Laundering transfers in no Altman attempt are assigned to dev or eval by the day they happen.
- **Scope.** The rules target **cross-bank bank transfers** (GIRO and MEPS+). Report recall over that scope, and account for every other laundering transfer separately: same-bank, cheque/card/cash (mostly from bank-70 hub accounts, whose laundering no rule separates from their ordinary payments), and the days after ordinary activity stops (after 10 Sep on `altman-small`, set as `ORDINARY_END` in `score.py`; a few stray ordinary payments run later).
- **Named SLIs, percentiles not means.** End-to-end detection latency (p50, p95) and alert correctness. SLO targets are chosen after the first baseline and before any comparative run. Never present an unmeasured target as a result.
- **Sound benchmarking** (Hoefler and Belli, SC'15): run each load configuration (experiment 5) 3 times and report the median and p95 with min/max, documenting every factor that varies. Fault and correctness experiments run once: detection is deterministic, so each run's cases are compared with the offline evaluator's for the same workload, and any lost or extra case shows.
- **Open-loop load:** replay at fixed rates (`--rate`, events/s) for load tests, which avoids coordinated omission; `--speed 60` is for the demo only.
- **Failures as RTO and RPO** (MAS TRM §8.2.1): RTO = time to the next alert, RPO = alerts lost. Define the steady state before injecting a fault. Faults: `kubectl delete pod`, `kubectl scale --replicas=0`, a temporary deny-all NetworkPolicy. k3d is one node, so failover there is pod-level only.
- Explain trade-offs and bottlenecks, including any case where scaling doesn't help.

## Offline detection evaluation

`apps/evaluation/` (Isaac): `uv run python -m evaluation.offline apps/dataset/out/altman-small --split dev` prints the report below as JSON; the privacy nodes' JSON log lines go to stderr. A run over `altman-small` takes about 8 minutes (estimated: 9 measured before the scorer's TTLs were skipped, which saves about a sixth) and 5.6 GB, mostly the scorer writing every bank transfer through fakeredis.

- **Input:** `prepare.py`'s bank files and labels file (`altman-small`, and `altman-medium` for experiment 14), with the attempt ID and `altman_pattern` per laundering transfer.
- **Same code as the live path:** each bank's scorer writes its bank transfers into its own Valkey state (fakeredis, in process, without the 7-day TTLs, which a run ends long before); its privacy node reads it through `ValkeyBank`, consumes the scorer's hits, searches from chain hits through intra-bank hops, and runs housekeeping with the ingestion grace over; its case messages go into a real `CaseStore` for that bank instead of over HTTP. That is the cluster's path after a replay settles. Peer `Trace` and `Deliver` calls are direct method calls between the three nodes. No Kafka, Valkey or gRPC servers.
- **Tiers and cases** are the case store's ([case.md](contracts/case.md#cases-and-alerts)): each bank's cases with a tier, by that bank's own payment IDs, as its analyst UI shows them; a `local` case's intra-bank hop payments count as flagged too. A cross-bank payment can be in both banks' cases, so a payment's tier is the highest of the cases holding it.
- **Output:** per Altman pattern type, attempts caught at any tier and at Tier 2 (cross-bank), and the attempts with an in-scope transfer and how many of those were caught; an attempt counts as caught if any of its transfers is in a case. Overall: **payment-level** recall over the scope, which counts laundering in no attempt (about a third of HI-Small's in-scope laundering); laundering by scope reason; precision; the false-positive rate as ordinary payments flagged / all payments on the split's own days; and analyst load (cases per bank per day, by tier).
- **Where it runs:** a laptop for the full splits; CI runs it on the committed fixture as a regression test.
- **Bridge to the cluster:** replay the `altman-1-6sep` workload (1 Sep to the end of the first eval day) through k3d at `--speed 0`, so the live rules have the same history as the offline run, and compare 6 Sep's stored cases with the offline run over the same workload. They must match (experiment 13). **Matched (19 Nov):** all 146,897 cases at the three banks are identical over the six days (6,989 on 6 Sep), tiers included, and so are the searches (1,718 CONFIRMED, 15 PARTIAL). Before the evaluator ran the live path it missed the chains the node finds through intra-bank hops: 99.6% identical, every difference a live chain.

## Experiments

Experiments 2–4 and 14 run offline; the rest run on k3d, and 10 on AWS.

| # | Question | Setup | Report |
| --- | --- | --- | --- |
| 1 | Does tracing work? | Fixture scenarios (`m1`, `l1`, `o1`, `x1`, `v1`) end to end | Every expected link, state and tier |
| 2 | Detection vs Altman | Rules tuned on dev, eval run once | Per pattern type: recall at any tier vs at Tier 2 (attempts), payment-level recall, precision, cases per bank per day |
| 3 | Thresholds | Dev only: the chain ratio band 0.85–1.15 / 0.90–1.10 / 0.95–1.05, other settings as in [streaming.md](contracts/streaming.md#detection-rule) | False positives vs recall; the chosen point |
| 4 | Hop limit | `HOP_LIMIT` 4/8 | Chain recall on Altman cycles vs search latency |
| 5 | Does scaling help? | One fixed rate above a single scorer's throughput; one fixed replica vs KEDA (1–6); 6 partitions | Throughput, lag, replicas, CPU/memory, p50/p95 latency; KEDA reaction time (polling 30 s, cooldown 300 s) |
| 6 | HA vs single instance | The same broker, PostgreSQL or privacy-node pod kill in the HA bank and a single bank | RTO, RPO; searches complete with one node replica killed mid-replay |
| 7 | Bank outage | Take C offline for N minutes under steady load | Backlog, request rate C sees on return, time to drain, with and without backoff and jitter |
| 8 | Clock skew | One bank's privacy node 10 min ahead under libfaketime | Absolute deadlines time out; relative deadlines complete |
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
| Peer-trace latency | Search start → search result | Report separately; from the node's `search` lines (`ms`) |
| Analyst load | Cases per bank per day, by tier | From the offline evaluator |

**Clocks:** latency is measured on k3d, where every pod shares the host clock, and from ingestion to an alert at the same bank, so no cross-machine clocks are compared. The scorer (each `payment_id` that writes a link or hit, with its Kafka timestamp) and the case service (each stored message's key, `chain_digest` for a chain) log JSON lines to stdout with their own timestamp (`ts`), and pandas joins them. The privacy node logs one JSON line per finished search attempt and per `Deliver` call ([protocol.md](contracts/protocol.md#failure-contract)), the source of peer-trace latency and experiment 4's search latency.

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

Every experiment except 10 (AWS only) and 14 was run once on 19 Nov 2026, locally on k3d, with the app code at `288432c` (identical to the `e30e88a` revision the runs used). Experiment 14 doesn't fit: building `altman-medium` needs ~19 GB and the VM has 16 GiB. No graphs yet.

**Setup.** Laptop host: AMD Ryzen AI 9 HX 370 (12 cores / 24 threads), 30 GiB RAM, Fedora 45. k3d runs inside a QEMU/KVM VM (`.k3d-vm`) with 8 vCPU and 16 GiB, on Ubuntu 24.04, k3s v1.35.5 and one node; bank A is HA, B and C single. Strimzi 1.2.0, CloudNativePG 1.30.1, KEDA 2.21.0, cert-manager v1.21.2. The offline evaluator runs in the same VM (Python 3.12.3, ~13–15 min and 5.2 GiB per `altman-small` run). A fresh `k3d-up.sh` took 11 min 23 s. Before each run every bank is emptied (topic, scorer group, Valkey, node and case tables). Absolute latencies are this laptop's: every pod shares the 8 vCPUs.

**Workloads.** Fault runs use the steady load `slice-6h --rate 200` (17,295 bank transfers over 173 s). Experiments 5, 7 and 12 use `slice-24h` (69,109 transfers). After every k3d run, each bank's stored cases (bank, tier, own payment IDs) are compared with the offline evaluator's for the same workload. **All 23 compared runs were identical, faults included, so no run lost an alert (RPO 0).**

**Detection (experiment 2, offline).**

| Split | Attempts caught, any tier | Attempts reaching Tier 2 | Payment recall (in scope) | Ordinary payments flagged | Precision | Cases per bank per day, Tier 1 / Tier 2 |
| --- | --- | --- | --- | --- | --- | --- |
| Dev (1–5 Sep) | 155 / 179 | 25 | 98.0% (1,441 / 1,471) | 6.33% | 0.82% | 8,883–9,566 / 2.0–2.4 |
| Eval (6–10 Sep) | 146 / 184 | 24 | 93.7% (963 / 1,028) | 1.54% | 4.18% | 2,170–2,314 / 5.4–6.0 |

Searches: 3,508 CONFIRMED, 44 PARTIAL. Eval is a re-run of the frozen rules, not tuning (see the exploratory sweep below).

**Thresholds and hop limit (experiments 3 and 4, offline).** The chain band hardly moves false positives, because the new-payee rule causes most of them. It does decide how many attempts reach Tier 2:

| Chain band | Searches (CONFIRMED + PARTIAL) | Dev recall | Dev ordinary flagged | Dev Tier 2 attempts | Eval recall | Eval ordinary flagged | Eval Tier 2 attempts |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 0.95–1.05 | 1,746 + 6 | 97.4% | 6.28% | 13 | 93.2% | 1.48% | 17 |
| **0.90–1.10** (kept) | 3,508 + 44 | 98.0% | 6.33% | 25 | 93.7% | 1.54% | 24 |
| 0.85–1.15 | 5,217 + 68 | 98.3% | 6.39% | 32 | 93.8% | 1.61% | 29 |

`HOP_LIMIT` 8 turns 28 of the 44 PARTIAL searches CONFIRMED (chains up to 10 claims); recall, tiers and cases are unchanged. Offline search latency is p50 1.44 / p95 2.35 ms against 1.48 / 2.48 ms at 4 (in process, no network).

**Tracing and offline = live (experiments 1 and 13).** Three fixture runs each matched offline: A 5, B 5, C 4 cases. `altman-1-6sep` at `--speed 0` replayed in 370 s with no failed batch, and all 146,897 cases matched at all three banks, tiers included. Searches were 1,720 CONFIRMED + 13 PARTIAL live against 1,718 + 15 offline: two searches finished CONFIRMED live but PARTIAL offline, with the same cases.

**Scaling (experiment 5).** `slice-24h` at `--rate 1500` (92 s), three runs each, median (min–max):

| Scorers | End-to-end p50 | End-to-end p95 | Scorer delay p95 | Peak lag, busiest bank | Lag 0 after replay | Scorer CPU, peak / mean |
| --- | --- | --- | --- | --- | --- | --- |
| 1, fixed | 102.7 s (101.5–108.4) | 140.8 s (139.0–146.0) | 0.19 s | 2,470 | 9.7 s | 1.13 / 0.41 cores |
| KEDA 1–6 | 105.5 s (104.1–107.8) | 142.7 s (135.0–142.8) | 0.22 s | 2,472 | 9.5 s | 1.67 / 0.56 cores |

KEDA scaled every bank to 6 scorers about 11 s after the load started, and nothing got faster: **the scorer is not the bottleneck**. One scorer keeps up at 1,500 events/s. Each bank stores about 62 cases per second, so the 45,000 cases land up to ~150 s after the replay ends. The privacy node uses ~0.1 core and the case service ~0.5 core (one Python process writing each message to PostgreSQL), so case delivery is the limit. At the steady 200 events/s, end-to-end latency (ingestion to stored case) is p50 **7.2 s** and p95 **12.3 s**.

**HA vs single (experiment 6).** One pod is killed 45 s into the steady load. RTO is measured two ways: the longest gap between accepted ingestor batches at that bank, and the longest gap between stored cases (normally ~10 s at this load).

| Killed | Bank A (HA) | Bank B (single) |
| --- | --- | --- |
| Kafka broker | ingest gap 0.6 s; case gap 11 s | ingest gap **26.1 s** (batches held for the producer's 25 s timeout); case gap 20 s |
| PostgreSQL primary | case gap 10 s | case gap **40.1 s** |
| Privacy node | case gap 10 s | case gap **20 s** |

No run lost a case.

**Bank outage (experiment 7).** Bank C's privacy node is off from +30 s to +270 s of `slice-24h --rate 500`. The shipped backoff is compared with an image that retries every 10 s with no growth or jitter:

| Retry | Pending searches, peak (A / B) | Attempts during the outage (A / B) | Drain after C returns |
| --- | --- | --- | --- |
| Capped backoff + jitter | 16 / 10 | 65 / 35 | 256 s |
| Fixed 10 s | 16 / 10 | 77 / 45 | 31 s |

With this few pending searches, backoff saves only 15–20% of retries and delays recovery by ~4 min (a retry can wait up to 300 s). The thundering herd it guards against needs far more pending searches than these workloads make.

**Clock skew (experiment 8).** Bank C's node ran 10 min ahead under libfaketime (wall clock only; monotonic unchanged). Cases were identical and all 3 searches CONFIRMED, so relative deadlines complete. The old absolute deadlines weren't rebuilt for a contrast run.

**Bulkhead (experiment 9).** Bank A's summary service was off from +30 s. Every alert still arrived. Case pages showed no draft and took 1.02 s (the summary call timing out), against 4.7 ms with drafts once it was back.

**Valkey loss (experiment 11).** After experiment 13 (six days of state, ~0.65 M keys per bank), every bank's Valkey was deleted and rebuilt from Kafka with a fresh consumer group, KEDA scaling to 6. RTO was **143 s / 146 s / 153 s** (A / B / C). Key counts, hits and cases were identical afterwards (RPO 0, no duplicate cases).

**Key rotation (experiment 12).** During `slice-24h --rate 500`, every bank introduced k2 at +30 s, switched to it at +120 s and retired k1 at +210 s. All 44,983 cases and 79 CONFIRMED searches matched offline, so no link was missed. Introducing new key files changes the pod spec, which restarts the scorer, node and case pods; switching and retiring are live file updates.

**Security checks on `288432c`** (fresh `k3d-up.sh`, 14 min 28 s, with #63/#65's policies and #61's Secrets encryption): `check-netpol.sh` 47/47 PASS, `check-mtls.sh` 35/35 PASS. Three more fixture runs on that cluster found an order race: in 2 of the 3, bank A's case for the `v1` chain (longer than the hop limit) lists 3 of its 4 own payments instead of 4 (`p20` missing). Same tier, same case count. Each bank's search of that chain returns a different partial window, and live the searches finish in any order, while offline they always run A, B, C. None of the other 23 runs showed it.

**Still to do.** Graphs; experiments 10 and 14; a look at the `v1` order race. The fault experiments are single runs: the case comparisons are exact, but the RTO gaps are single samples.

### Exploratory sweep (not the offline evaluator)

Macarius, 31 Oct 2026, re-run on 7 Nov on the offline evaluator's definitions: a scratchpad script that reimplements the chain, new payee and quick repeat rules over `altman-small` and `altman-medium` (SGD, three banks, every rule hit counted as caught, history from 1 Sep, bank transfers only). Laundering counts by its attempt's start day, laundering in no attempt by its own day; scope = cross-bank bank transfers up to the end of ordinary activity (10 Sep on `altman-small`, `ORDINARY_END` in `score.py`; 16 Sep on `altman-medium`); false positives = ordinary payments flagged / all payments on the split's own days. It chose the starting thresholds in [streaming.md](contracts/streaming.md#detection-rule); the offline evaluator replaces it. On `altman-small` and `altman-medium` it flagged exactly the payments in the evaluator's cases (178,954 and 895,133) before the evaluator ran the live path, which adds chains through intra-bank hops and their hop payments, so that equality needs re-checking; `score.py` still cuts `altman-medium` at 10 Sep, so its HI-Medium row needs `ORDINARY_END` set to 16 Sep.

| Run | Attempts caught (in scope) | Laundering payments caught (in scope) | In no attempt (part of laundering payments caught) | Ordinary flagged |
| --- | --- | --- | --- | --- |
| HI-Small dev (attempts starting 1–5 Sep) | 154 of 158 (97.5%) | 1,441 of 1,471 (98.0%) | 412 of 430 (95.8%) | 6.33% |
| HI-Small eval (attempts starting 6–10 Sep) | 139 of 146 (95.2%) | 963 of 1,028 (93.7%) | 358 of 410 (87.3%) | 1.54% |
| HI-Medium (attempts starting 1–16 Sep), thresholds unchanged | 2,283 of 2,459 (92.8%) | 16,461 of 17,777 (92.6%) | 4,441 of 5,352 (83.0%) | 2.98% |

Dev's false-positive rate is higher because 1–2 Sep have no payment history, so every payee is new: 11.0% and 7.2% of those days' payments are flagged as ordinary, against 1.5–3.4% on 3–5 Sep. Out of scope and accounted for separately (dev / eval): 847 / 941 same-bank, 243 / 220 cross-bank cheque, card, cash or Bitcoin, and 7 / 399 after 10 Sep laundering transfers.
