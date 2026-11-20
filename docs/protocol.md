# The privacy node protocol

The privacy node is the only part of a bank that talks to other banks. When its bank's rules link an incoming transfer to an outgoing one, the node asks the banks on either side whether their rules move the same money on. Every bank on the path answers with a signed claim and asks the next bank. The result is a **chain**: a list of signed claims that every bank on it stores.

Code: [node.py](../src/mulechain/node/node.py) (the node), [protocol.proto](../src/mulechain/node/protocol.proto) (messages), [claims.py](../src/mulechain/node/claims.py) (checking a chain), [store.py](../src/mulechain/node/store.py) (durable state), [bank.py](../src/mulechain/node/bank.py) (the bank's own payments, read from Valkey).

## Messages

Banks speak gRPC over mutual TLS on port 50051. No message can carry an account, an amount or a payment ID: a transfer is named only by its **token**, 16 bytes that only the two banks on the transfer can derive ([security.md](security.md)).

| RPC | Request → reply | Purpose |
| --- | --- | --- |
| `Keys` | `Empty` → `PublicKeys` | This bank's public keys, oldest first. Peers fetch them every 10 s (`KEY_SECONDS`) |
| `Trace` | `TraceRequest` → `TraceReply` | "Do you hold this transfer, and does your rule move it on?" |
| `Deliver` | `Chain` → `Ack` | Push a finished chain to a bank that signed part of it (a server-to-server callback) |

| Message | Fields |
| --- | --- |
| `TraceRequest` | `token`; `direction` (`DOWNSTREAM`: follow the money forward, `UPSTREAM`: backward); `hops_remaining` (transfers the search may still add); `visited` (tokens already on the chain) |
| `TraceReply` | `claims`, nearest bank first. Empty means a bank on the path doesn't hold its side of the transfer (yet), or the caller may not ask about it |
| `LinkClaim` | `signer`, `in_from`, `in_token`, `out_to`, `out_token`, `cut`, `signature` |
| `Chain` | `claims` in money-flow order |

A claim is one bank's signed statement: "I received `in_token` from `in_from`, and my rule links it to `out_token`, which I sent to `out_to`". An empty side means the chain starts or ends at the signer. `cut` means the signer knows of more that it didn't follow. The Ed25519 signature covers every field (`claim_bytes`).

**Example.** Bank A sends 10,000 to bank B, and B sends 9,500 on to bank C. Only B's rule links the two. The chain B assembles (tokens shortened):

| # | signer | in_from | in_token | out_to | out_token | Says |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | A | | | B | `f4c6…` | "I sent `f4c6` to B" |
| 2 | B | A | `f4c6…` | C | `94f7…` | "My rule links `f4c6` from A to `94f7` to C" |
| 3 | C | B | `94f7…` | | | "I received `94f7` from B" |

Each transfer is confirmed from both ends: claim 1's `out_token` is claim 2's `in_token`, and the two claims name each other. `valid_chain` checks this and every signature.

A chain has no ID or state field. Every bank computes both from the claims, so they can't disagree:

- **ID** (`digest`): SHA-256 over the sorted set of claim bytes. The same claims give the same ID whichever bank assembled them and from whichever end.
- **State** (`complete`): `confirmed` if nobody cut it and both ends are a natural start or end, or close a loop. Otherwise `partial`.

### Who is calling, and errors

The caller's bank is read from its client certificate (`caller`), never from a message field. A bank answers a `Trace` only if the caller is the bank on the other side of that transfer (`Bank.lookup`); anyone else gets the same empty reply as for an unknown token. Shown by [test_mtls.py](../tests/test_mtls.py) and `test_a_node_answers_only_the_bank_on_the_other_side_of_a_transfer` in [test_cluster.py](../deploy/test_cluster.py).

Failures are gRPC status codes, not fields:

| Code | Meaning | What the caller does |
| --- | --- | --- |
| `OK`, empty `TraceReply` | The bank doesn't hold the transfer, isn't asked by its counterparty, or the next bank doesn't hold its side | No chain from this attempt |
| `UNAVAILABLE` | The bank can't be reached or the TLS handshake failed (no certificate, or one from another authority). From `Trace`: a bank further on gave no answer. From `Deliver`: a signer's keys can't be fetched yet | Try again later |
| `DEADLINE_EXCEEDED` | The caller's timeout passed | Try again later |
| `UNAUTHENTICATED` | On `Trace` and `Deliver`: the certificate is from the lab CA but doesn't name exactly one bank | Try again later |
| `INVALID_ARGUMENT` | `Deliver` only: the chain doesn't verify, or the sender or receiver didn't sign it | Never send it again (dead letter) |

## How a search runs

1. **Hit.** The scorer writes rule hits to a Valkey stream ([detection.md](detection.md)). `take_hits` reads them. Every hit becomes a message for the case service, and a `chain` hit also becomes a row in `searches`. The row's ID is the hash of its two payment IDs, so a repeated hit adds nothing. The rows and the stream position commit in one transaction.
2. **Attempt.** `run_searches` takes the searches that are due, up to 64 a pass, and runs `attempt` on each, eight side by side.
3. **Inside the bank first.** A transfer between two accounts of the same bank has no token and no peer. `Bank.ends` walks through such hops (at most `MAX_INTERNAL_HOPS = 4`) to the cross-bank payments at each end. If the search started on an inner link, a new search takes over from those ends and this one is `moved`. If one end never leaves the bank, the search ends as `local` once the ingestion grace (below) has passed.
4. **Ask both ways.** `search` signs this bank's own claim, then asks the receiving bank `DOWNSTREAM` and the sending bank `UPSTREAM`.
5. **Each bank answers** (`Trace`). It looks up the token, finds the transfers its rules link onward, signs its claim, and asks the next bank with one hop fewer. Its reply is its own claim followed by the claims it got back.
6. **Verify.** The bank that started the search puts the chain together (upstream claims reversed, its own, downstream claims) and checks it with `valid`. If a signature fails, it fetches the signers' keys once more and checks again.
7. **Save and deliver.** `save` stores the chain with everything it owes in one transaction: a copy for every other signer and a message for this bank's case service ([cases.md](cases.md)). `Deliver` then sends the copies. Each receiver verifies the chain again and stores it the same way.

Any bank can start a search, and each delivers its own chains, so no bank is a hub. [test_system.py](../tests/test_system.py) runs all of this on the tiny workload (`test_every_bank_ends_with_its_own_view_of_the_same_chains`).

### Limits on a search

| Limit | Value | How it works |
| --- | --- | --- |
| Hop limit | `HOP_LIMIT = 4` | A search follows at most 5 transfers (4 hops). The starting bank knows 2 of them, so the two directions share a budget of 3 more. Downstream runs first; upstream gets what is left. A bank with no hops left names the next transfer in a `cut` claim and doesn't follow it, so a chain can name one more transfer at each end than its search followed |
| Loops | `visited` | The request carries every token already on the chain. A bank whose next transfer is in it signs the closing link and stops |
| Branches | Follow one | If a bank's rules link more than one transfer onward, it follows the first by payment ID and sets `cut` |
| Deadline | `SEARCH_SECONDS = 10` | Set once, by the bank that starts the search. See below |

The hop limit bounds the work and the disclosure one search can cause, like the time-to-live of an IP packet ([RFC 791](https://www.rfc-editor.org/rfc/rfc791.txt)). Longer chains aren't lost: searches started at different banks each return a partial chain, and the case service joins them into one case.

**Deadline propagation** ([gRPC](https://grpc.io/docs/guides/deadlines/), [patterns.md](patterns.md#4-deadline-propagation)). Every call's timeout is what is left of the starting bank's 10 seconds. A bank answering a `Trace` reads its caller's remaining time (`time_left`) and gives its own onward call that, minus `REPLY_MARGIN = 0.25` s to send its reply. gRPC carries the timeout as a duration, not a clock time, so banks need not agree on the time. The 10 seconds cover the `Trace` calls; checking the signatures on the answer afterwards is the starting bank's own work. `Keys` and `Deliver` use a fixed 5 s (`CALL_SECONDS`), since no search waits on them.

### Search states

| State | Meaning | Final |
| --- | --- | --- |
| `pending` | A bank didn't answer, or doesn't hold its side of the transfer yet. Tried again later, with backoff, for 24 hours | no |
| `moved` | The link is inside this bank; another search took over | yes |
| `local` | The money starts or stays inside this bank: a case for this bank only | yes |
| `confirmed` | A complete chain, signed by every bank on it | yes |
| `partial` | A chain some bank cut short (hop limit or a branch). Stored and delivered like a confirmed one | yes |
| `failed` | The answers didn't verify, or the payments have expired from Valkey | yes |

**Late ingestion.** Banks ingest the same transfer at different moments, and a scorer can be behind. An empty answer therefore never ends a search: it stays `pending` and is retried like an unanswered one, until the bank holds the transfer or the search expires. Only `local` cannot wait that long. A link whose money seems to stay inside the bank is given `INGESTION_GRACE_SECONDS` (60 in the cluster) for the bank's own scorers to catch up, and is then final.

## Failure handling

**A bank is down.** Its neighbours' calls fail, so every search that needs it stays `pending`, and their case services show "Waiting for bank X". X is the neighbour the search asked, which can be a healthy bank passing the failure on from the bank behind it. Searches that don't need that bank carry on. When it returns, the searches are retried and the chains stored at all banks. Nothing is lost, only delayed: `test_a_bank_that_is_down_delays_cases_but_loses_none`.

**Retries with backoff.** A pending search waits `random(0, min(300 s, 10 s × 2^attempts))` before its next attempt (`backoff`: capped, exponential, with full jitter), so a bank that comes back isn't hit by every waiting search at once. Only the starting bank retries, and it retries the whole search. After 24 h (`EXPIRY`) a search is no longer tried. See `test_backoff_doubles_up_to_the_cap_with_full_jitter` and `test_a_pending_search_is_retried_until_it_expires` in [test_node_store.py](../tests/test_node_store.py).

**Outbox.** A chain and the messages it owes are written in one transaction (`save_chain`), then sent until acknowledged (`deliver`). A crash between storing and sending can't lose a message; it can only send one twice. Unacknowledged messages back off the same way as searches, and a bank that failed once isn't called again in the same pass.

**Repeats are harmless.** A chain is stored under its digest with `ON CONFLICT DO NOTHING`, so a second `Deliver`, or a second search that finds the same chain, changes nothing and owes nothing new: `test_a_forged_chain_is_refused_and_a_repeated_one_is_harmless`.

**Dead letters.** A `Deliver` refused with `INVALID_ARGUMENT` is marked `dead` and never sent again, because the receiver read it and sending it again can't help. Every other failure is retried: `test_a_refused_delivery_becomes_a_dead_letter_and_others_are_retried`. A receiver that can't fetch a signer's keys answers `UNAVAILABLE` instead, so a good chain isn't dead-lettered just because a third bank is down.

**Replicas.** With `ha` a bank runs two node replicas behind one Service, sharing the store in PostgreSQL. Both answer RPCs and both fetch keys. The background work (reading hits, searching, delivering) is done by one replica at a time: each pass takes the PostgreSQL advisory lock `HOUSEKEEPING_LOCK` (`Store.housekeeping`) and skips if the other replica holds it. PostgreSQL releases the lock by itself if the holder dies. A node keeps no state of its own, so a restart loses nothing.

## Durable state

One PostgreSQL database per bank (SQLite in tests and the offline evaluator, same SQL):

| Table | Holds |
| --- | --- |
| `searches` | Every search this bank started: state, attempts, when to try next |
| `chains` | Every verified chain this bank is part of, by digest, with its signed claims |
| `outbox` | Messages still owed: chains to peer banks, case messages to this bank's case service |
| `peer_keys` | The public keys each peer last published |
| `hit_cursor` | How far into the scorer's hit stream this bank has read |

## Logs

The node prints one JSON line per search attempt and per `Deliver` call, for timing them ([evaluation.md](evaluation.md#metrics-and-where-they-come-from)).

```json
{"ts": 1795140803.82, "service": "node", "bank": "B", "event": "search", "search": "6316…", "state": "confirmed", "attempt": 1, "ms": 4.5}
{"ts": 1795140803.83, "service": "node", "bank": "B", "event": "deliver", "chain": "f120…", "peer": "A", "outcome": "OK", "ms": 0.5}
```

`outcome` is `OK` or the gRPC status. `ms` for a search runs from its start to its stored result.

## Known limits

- A case that says "Waiting for bank X" keeps saying so if its search later ends without a chain: failed, or no longer tried after 24 h. Nothing tells the case service that the wait is over.
- During a key rotation the two banks on a transfer can name it by different tokens ([security.md](security.md)). Loop detection compares tokens, so it can miss a loop then; the hop limit still bounds the chain.
- A bank's rules can link several transfers onward, but a search follows one. The others are found by the searches their own hits start.
- What a signature proves, and what tokens and claims reveal, is in [security.md](security.md#residual-disclosure).
