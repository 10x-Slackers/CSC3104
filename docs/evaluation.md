# Evaluation

Owner: Isaac · Evidence supplied by every owner · Status: **draft**

> [!WARNING]
> **Outdated (30 Oct 2026).** Under review after a full codebase walkthrough; don't build against this doc until it's updated. Expected to change:
> - **Questions**, **Metric definitions**: the mock intake and AI report items
> - **Demo scenario**: step 5 (AI draft report); HA-bank failover comparison

Every claim needs a measured indicator, and the main results need graphs.

## Rules

- Freeze a small deterministic **acceptance suite** first. Then evaluate on a separate, **held-out** labelled workload. Tune only on dev data.
- Report results for ordinary activity and for planted chains.
- Choose load levels and latency targets after the first baseline and before any comparative runs. Never present an unmeasured target as a result.
- Explain trade-offs and bottlenecks, including any case where scaling doesn't help.

## Questions

| Question | Method | Evidence required |
| --- | --- | --- |
| Does tracing work? | Replay known three-bank chains, benign lookalikes, loops, absent links and limit cases | Every deterministic case matches its expected links and state; loops and deadlines terminate |
| Does collaboration help? | Compare local-only rules with cooperative tracing on the same held-out workload | Transaction-level and chain-level measures defined separately; precision, recall, false positives, sample counts |
| Is disclosure bounded? | Inspect representative serialised peer messages, app logs and report inputs against the allowlist | No forbidden raw fields; permitted metadata and residual linkability documented |
| Are peers and claims checked? | Send unauthenticated requests, and altered or wrongly signed claims | Rejected without creating confirmed evidence |
| Is one alert maintained? | Replay duplicates; launch overlapping searches; restart workers around case and alert writes | One persistent alert per bank per agreed chain identity; required alerts appear after recovery |
| Does failure stay local? | Take one bank offline; restart a privacy node or scorer and replay | Dependent traces stay partial; independent work continues; recovery time and unresolved work recorded |
| Does scaling help? | Replay fixed low and high loads with fixed workers, then with autoscaling | Throughput, queue lag, replica count, resource use, median and p95 latency, over repeated runs |
| Is the analyst output grounded? | Review case views and generated drafts against signed evidence, including a partial case | No invented identities or unsupported conclusions; uncertainty shown; a report failure doesn't block detection |

## Metric definitions

| Metric | From → to | Notes |
| --- | --- | --- |
| End-to-end detection latency | Event ingestion → durable alert | Headline latency |
| Peer-trace latency | Search start → search result | Report separately |
| Intake response time | Durable alert → mock intake arrival | A post-alert metric. **Never** substitute it for detection latency |

TODO (Isaac, Cindy, Jia Rui): agree which component emits each timestamp, and how clocks are compared across pods.

## Run record

Record this for every run, e.g. as `results/<run-id>/run.json` (location TBC):

- dataset and workload, seed, event rate
- rule settings
- deployment (k3d or EKS) and resources
- software revision (git SHA)
- fault timing, if any

## Demo scenario

The scripted demo runs these steps in order:

1. Replay a labelled mule chain alongside ordinary payments across Banks A, B and C. Show local suspicion turning into verified cross-bank links.
2. Open the bank-local cases: one alert per involved bank. Inspect a peer payload to show the disclosure boundary.
3. Repeat messages and restart a worker. The existing alert is kept and not duplicated.
4. Take one bank offline. A dependent trace stays partial while a separate search among the available banks continues. Restore the bank and show recovery.
5. Show the load experiment results and an AI draft report with its evidence.

## Results

TODO: add graphs and tables once runs exist.
