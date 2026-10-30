# Docs

CSC3104 Cloud and Distributed Computing, Group 08: **Privacy Preserving Detection of Cross-Bank Money Mule Chains**.

> [!WARNING]
> **Outdated (30 Oct 2026).** Every doc below is under review and partly outdated; each one lists the sections expected to change.

Start with [overview.md](overview.md). Its requirements section is fixed by the module and overrides every other doc.

| Doc | Covers | Owner |
| --- | --- | --- |
| [overview.md](overview.md) | Requirements, goals, scope, architecture, trust model, stack, deadlines | Group |
| [contracts/data.md](contracts/data.md) | Dataset, bank split, labels, workloads, fixture, replay | Macarius |
| [contracts/streaming.md](contracts/streaming.md) | Kafka events, detection rule, scorer, scaling | Cindy |
| [contracts/protocol.md](contracts/protocol.md) | Peer messages, search algorithm, failure handling | Zane |
| [contracts/security.md](contracts/security.md) | Tokens, signing, disclosure allowlist, mTLS, NetworkPolicy intent | Caleb |
| [contracts/case.md](contracts/case.md) | Cases, alert identity, analyst UI, AI report | Jia Rui |
| [contracts/deployment.md](contracts/deployment.md) | k3d/EKS, Helm, per-service settings, monitoring | Isaac |
| [evaluation.md](evaluation.md) | Evaluation questions, metrics, run records, demo scenario | Isaac |

## Conventions

- Mark undecided items **draft** and unwritten sections TODO.
- When a decision is agreed, change **draft** to **agreed** in the owning doc and add the reviewer's name and the date.
- Never present numbers we haven't measured as results.
