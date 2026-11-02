# Docs

CSC3104 Cloud and Distributed Computing, Group 08: **Privacy Preserving Detection of Cross-Bank Money Mule Chains**.

> [!NOTE]
> **Target design decided 30 Oct 2026.** Each doc opens with the **pending work** that changes it; the rest of the doc describes today's code until that work lands. The target architecture is [architecture.drawio.png](architecture.drawio.png).

Start with [overview.md](overview.md). Its requirements section is fixed by the module and overrides every other doc.

| Doc | Covers | Owner |
| --- | --- | --- |
| [overview.md](overview.md) | Requirements, goals, scope, architecture, trust model, stack, deadlines | Group |
| [contracts/data.md](contracts/data.md) | Dataset, bank split, labels, workloads, fixture, replay, ingestor | Macarius |
| [contracts/streaming.md](contracts/streaming.md) | Kafka events, detection rules, scorer, Valkey state, scaling | Cindy (event schema: Macarius) |
| [contracts/protocol.md](contracts/protocol.md) | Peer messages, search algorithm, failure handling, delivery to the case service | Zane |
| [contracts/security.md](contracts/security.md) | Tokens, key agreement and rotation, signing, disclosure boundary, mTLS, NetworkPolicy intent | Caleb |
| [contracts/case.md](contracts/case.md) | Cases, alert tiers, read-only analyst UI, summary service | Jia Rui |
| [contracts/deployment.md](contracts/deployment.md) | k3d/EKS, Helm, per-service settings, network paths, monitoring | Isaac |
| [evaluation.md](evaluation.md) | Method, offline detection evaluation, experiments, metrics, demo | Isaac |

## Conventions

- **pending work** marks the decided target design that isn't built yet. **draft** marks anything still undecided, and TODO an unwritten section.
- **Contract first:** the producer of an interface writes it in their contract doc with one sample message, the consumer approves it, and both sides' tests load that sample. Changing it later needs the consumer's approval.
- When a decision is agreed, change **draft** to **agreed** in the owning doc and add the reviewer's name and the date. When pending work lands, fold it into the doc's body and drop it from the banner.
- Markers: `**draft**`, `**agreed** (reviewer, date)`, `**pending work:**` before the text it marks (`**pending work (owner):**` to lead a section), and `(**pending work**)` as a tag.
- Amounts are in SGD, and sources are Singapore-first (MAS, STRO, SPF, PDPC).
- Never present numbers we haven't measured as results.

### Contract layout

Every doc in `contracts/` follows the same frame; the sections in between are the owner's.

1. `# <Area> contract`, then one line: `Owner · Partners · Code · Status` (status is **draft** or **agreed**, with who must review it).
2. A `> [!NOTE]` banner, **Pending work (decided 30 Oct 2026)**, listing what changes today's design. Remove it when it's empty.
3. `Scope:` in one sentence.
4. The owner's sections, describing what exists today, with inline **pending work** where it changes.
5. `## Interfaces`: one row per interface the owner produces or consumes (interface, from → to, what it pins down, status, agree-by date). Both sides list it.
6. `## Open`: a checklist, unresolved items first, each ending with its owner in parentheses; resolved items stay ticked with the decision.
