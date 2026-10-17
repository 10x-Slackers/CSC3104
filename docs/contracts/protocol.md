# Privacy node protocol contract

Owner: Zane · Partners: Caleb (security), Jia Rui (result delivery, cases), Isaac (fault tests) · Status: TODO

Scope: protobuf messages, gRPC service, onward matching, hop and deadline bounds, visited state, stored replies, retries, result delivery and failure handling.

## Messages

Keep versioned `.proto` files and example messages next to the code. Caleb reviews every field against the disclosure allowlist.

| Message | Fields | Notes |
| --- | --- | --- |
| Query | TODO | Query ID, token, hop count, deadline |
| Reply | TODO | Match result and signed claim |
| Result delivery | TODO | Sent to every involved bank |
| Acknowledgement | TODO | Must be durable |

## Search algorithm

| Item | Decision |
| --- | --- |
| Query ID and idempotency | TODO |
| Hop accounting and limit | TODO |
| Deadline | TODO |
| Visited-token handling (loop prevention) | TODO |
| Bounded retries | TODO |
| Which bank assembles the result | TODO |
| Delivery to all involved banks without a global coordinator | TODO. A per-search initiator must not become a dependency for the whole system |

## Result states

| State | Meaning |
| --- | --- |
| confirmed | TODO |
| no-match | TODO |
| partial | TODO |
| timed-out | TODO |
| invalid-claim | TODO. Never becomes confirmed evidence |

## Failure contract

- A missing bank can't confirm an unknown link. Searches that need it become partial or pending; independent searches keep going.
- TODO: resuming after recovery, how long retained state lives, behaviour when the initiating bank restarts.
- Concurrent replicas share durable query state. Scale replicas only once idempotency works.

## Open

- TODO
