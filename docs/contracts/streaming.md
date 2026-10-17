# Streaming and scoring contract

Owner: Cindy · Partners: Macarius (events), Zane (search handoff), Caleb (tokens), Isaac (scaling, baseline comparison) · Status: TODO

Scope: bank-local Kafka, the ingestor, token-index population, the scorer's rule windows, KEDA scaling and the local-only baseline.

## Kafka events

| Item | Decision |
| --- | --- |
| Topic names | TODO |
| Event identity / dedup key | TODO |
| Partitioning key and ordering guarantee | TODO |
| Replay, late and out-of-order events | TODO |

## Detection rule

The rule indicates suspicion, not proven laundering. Start with a single-currency linear-chain fixture, and label unsupported cases explicitly.

| Parameter | Value | Notes |
| --- | --- | --- |
| Incoming-to-outgoing value ratio | TODO | |
| Time window | TODO | |
| Minimum chain length | TODO | |
| Hop limit | TODO | Shared with [protocol.md](protocol.md) |
| Branching cap | TODO | |
| Fees, currencies, insufficient history | TODO | |

## Token index

TODO: when and how events are tokenised; where the index lives; how the privacy node looks tokens up. The index never leaves the bank.

## Search handoff

TODO: what the scorer passes to the privacy node to start a search.

## Scaling

TODO: KEDA trigger (Kafka consumer-group lag), min and max replicas, and the metrics exported for [evaluation.md](../evaluation.md).

## Local-only baseline

TODO: the same rules without peer queries. Isaac specifies the comparison.

## Open

- TODO
