# Privacy node protocol contract

Owner: Zane · Partners: Caleb (security), Jia Rui (result delivery, cases), Isaac (fault tests), Cindy (search handoff) · Code: `apps/privacy_node/` · Status: **draft**, needs review by Caleb

> [!NOTE]
> **Pending work (decided 30 Oct 2026).** The sections below describe what exists today; these items change it:
> - **Retries:** only at the initiator, with capped exponential backoff and full jitter (`random(0, min(300 s, 10 s × 2^attempts))`, stored as `attempts` and `next_at`), and expiry: after 24 h a search becomes **unresolved**. Fresh `query_id` per retry stays.
> - **Status mapping:** `RESOURCE_EXHAUSTED` and `ABORTED` become pending, not failed. Rejected deliveries (`INVALID_ARGUMENT`) are dead-lettered instead of retried forever.
> - **State and replicas:** `store.py` moves to PostgreSQL; token index and local links are read from Valkey ([streaming.md](streaming.md#search-handoff)); 2 replicas, with housekeeping under a PostgreSQL advisory lock; a gRPC health service (not serving until state is ready).
> - **Delivery to the case service** through the same outbox ([Result delivery to the case service](#result-delivery-to-the-case-service)); shared `check_result` validation is implemented at both receivers.

Scope: protobuf messages, gRPC service, onward matching, hop and deadline bounds, visited state, stored replies, retries, result delivery and failure handling.

## How a search works

1. A bank's scorer flags a local link: an incoming transfer forwarded on as an outgoing transfer ([streaming.md](streaming.md)). That bank becomes the **initiator** of this one search.
2. The initiator signs its own claim linking the two transfer tokens. It then runs two **legs**:
   - **downstream**: it asks the bank that received the outgoing transfer, "do you hold this token, and does your rule move it on?"
   - **upstream**: it asks the bank that sent the incoming transfer, "do you hold this token, and did it arrive from somewhere you also flagged?"
3. Each bank that holds the token signs a claim and, if its own rule links the transfer onward, forwards the same query one hop further. Replies come back along the same path with every claim collected.
4. The initiator checks every claim, assembles the chain in money-flow order, stores it, and pushes it to every bank that signed a claim (`Deliver`).

```mermaid
sequenceDiagram
  participant A as Bank A
  participant B as Bank B (initiator)
  participant C as Bank C
  Note over B: scorer flags p06 -> p07
  B->>C: Trace(token p07, DOWNSTREAM)
  C-->>B: CONFIRMED [C: in p07, end]
  B->>A: Trace(token p06, UPSTREAM)
  A-->>B: CONFIRMED [A: start, out p06]
  Note over B: assemble A -> B -> C, store
  B->>A: Deliver(chain)
  B->>C: Deliver(chain)
```

Searches start from the scorer's detection, wherever it is, because a chain is often detected in the middle. In the fixture, `m1` (A→B→C) is detected only at B. A detection with an intra-bank leg is not searched: intra-bank transfers have no pairwise token.

## Messages

Source: [`apps/privacy_node/src/privacy_node/privacy_node.proto`](../../apps/privacy_node/src/privacy_node/privacy_node.proto). The descriptor test recursively requires the exact per-message `APPROVED_FIELDS` in `claims.py` and forbids every `security.FORBIDDEN_FIELDS` name. Receivers reject unknown wire fields recursively with `INVALID_ARGUMENT`, before cached replies or result storage. The JSON field walker remains for untyped data only. The shared [sample request](samples/trace_request.textproto) is used by producer and consumer tests.

| Message | Fields | Notes |
| --- | --- | --- |
| `TokenRef` | `pair`, `token` | A pair-scoped token, e.g. pair `A-B`; `token` is exactly 16 raw bytes. Lookups use `(pair, key_version, token)` ([security.md](security.md)) |
| `TraceRequest` | `query_id`, `sender`, `key_version`, `ref`, `direction`, `hops_remaining`, `visited` | `hops_remaining`, not `hop`: `hop` is a forbidden evaluation label. Field 7 (former absolute `deadline`) is reserved; gRPC carries the relative timeout |
| `LinkClaim` | `signer`, `recipient`, `query_id`, `key_version`, `in_ref`, `out_ref`, `signature`, `signing_key_version`, `truncated` | "I hold `in_ref` and `out_ref` and my local rule links them." An empty ref means the chain starts or ends here. `key_version` scopes tokens; `signing_key_version` selects the trusted Ed25519 public key. Signed `truncated=true` records a hop/branch cap. At the hop limit the bank still names its onward transfer |
| `TraceReply` | `state`, `claims` | Claims in path order, nearest bank first |
| `ChainResult` (result delivery) | `chain_digest`, `sender`, `state`, `claims` | `chain_digest`, not `chain_id`: `chain_id` is a forbidden evaluation label |
| `Ack` | `chain_digest` | Returned by `Deliver` once the receiver has stored the result |

Nothing else leaves a bank: no payment IDs, accounts, amounts, timestamps or rule values.

### Signed claim bytes

The signature covers `claims.claim_bytes`: concatenate the following fixed-order fields,
with **each field preceded by its 4-byte unsigned big-endian byte length**:

```text
UTF8("csc3104.link-claim"), UTF8("v1"), UTF8(signer), UTF8(recipient),
UTF8(query_id), UTF8(key_version), UTF8(signing_key_version), UTF8(in_ref.pair),
raw in_ref.token, UTF8(out_ref.pair), raw out_ref.token, ASCII("1" if truncated else "0")
```

Missing refs use empty pair and zero token bytes. Tokens are never encoded as hex/base64
inside the application. The domain prevents cross-message signature reuse; recipient,
query ID and both key versions are signed. This explicit framing avoids depending on
protobuf serialization order as a cross-implementation canonical form.

Verification keys come only from trusted configuration keyed by `(signer, signing_key_version)`.
The signer must be an endpoint of every canonical pair it vouches for; each nonempty token
is exactly 16 bytes. Adjacent claims must share the same token identity and token version.

`check_result(result, public_keys, bank)` is the sole result validator used by `Deliver`
and case `/results`. It returns a verified copy or raises `ValueError`. It requires a
confirmed/partial delivered state, this bank represented as a signer, exactly one sender
self-addressed anchor claim, exact digest, valid signatures/ownership/continuity, and
query-ID/recipient binding along each leg outward from the anchor. Unknown fields fail.
A cut endpoint or any signed `truncated` flag normalizes the returned state to `PARTIAL`;
consumers store the returned copy. A sender cannot remove a signed cap flag without
invalidating the signature.

## Search algorithm

| Item | Decision |
| --- | --- |
| Query ID and idempotency | `query_id` is a UUID per leg. Each bank stores its reply under `(query_id, sender, ref)` and returns the stored reply to a retry. Only final states are stored; `PENDING` and `TIMED_OUT` are recomputed |
| Search ID | `sha256("<bank>:<incoming payment>:<outgoing payment>")`, bank-local only. The same detection always maps to the same search row |
| Hop accounting and limit | `HOP_LIMIT = 4` (matches `prepare.py`): a chain holds hops 0–4, so at most 5 transfers. The initiator already knows 2, so the legs share a budget of 3 extra transfers. The downstream leg runs first; the upstream leg gets what is left. `hops_remaining` drops by one each time a bank adds a transfer |
| Deadline | `SEARCH_SECONDS = 10`. The initiator alone keeps a monotonic local deadline. No wall-clock timestamp appears in protobuf. A receiver starts from `context.time_remaining()` and forwards only its remaining relative budget; the upstream leg shares the initiator's remaining budget. Measuring p99.9 to tune these constants remains Zane's integration work |
| Visited-token handling (loop prevention) | The request carries every token already in the search, including those found by the other leg. A bank whose next token is already visited signs the closing link and stops |
| Branching cap | `BRANCH_CAP = 1`: linear chains only. If a bank's rule finds more than one onward link, it follows the first (lowest payment ID) and the result becomes `PARTIAL` |
| Retries | One attempt per peer call. `UNAVAILABLE` → pending and `DEADLINE_EXCEEDED` → timed-out, both re-run by housekeeping; any other peer error → failed, not retried. **pending work:** `RESOURCE_EXHAUSTED` and `ABORTED` → pending; backoff with jitter and 24 h expiry |
| Which bank assembles the result | The initiator of that search: up-leg claims reversed, then its own claim, then down-leg claims |
| Delivery to all involved banks without a global coordinator | Each initiator delivers its own results. The result row and one outbox row per other signer are written in one transaction; `Deliver` is sent once after a batch of searches and retried until acknowledged (housekeeping). The receiver re-verifies every claim, the digest, and that each leg's claims were signed for one query and the bank that asked; it accepts only confirmed or partial results, and stores a chain whose end is a cut as partial whatever the sender says. Any bank can be an initiator, so no single bank is a dependency for the whole system |
| Chain identity | `chain_digest` = SHA-256 of sorted distinct `(pair, raw token)` identities, each pair and token framed by its 4-byte big-endian length, including a transfer named at a cut. Searches started at different banks over the same transfers produce the same digest (tested on loop `o1`, started from B and C). Adopted as the case identity ([case.md](case.md)) |

## Result states

When results combine, the worst state wins, in this order: confirmed < partial < no-match < pending < timed-out < failed < invalid-claim.

| State | Meaning |
| --- | --- |
| confirmed | Both legs reached a natural start/end or closed a loop, and every claim verified |
| no-match | A queried bank does not hold the token for that pair and direction (or the sender is not in the pair) |
| partial | Cut by the hop limit or the branching cap. **Final**: stored and delivered as partial evidence |
| failed | A peer rejected or failed the query (any gRPC error other than unavailable or deadline). Final, not retried, not stored |
| pending | A needed bank was unreachable. Claims from available banks are kept; the search is retried by housekeeping |
| timed-out | The deadline passed. Retried by housekeeping |
| invalid-claim | A signature, signer, token continuity, query ID or recipient check failed. Never stored as a chain or delivered; its search row keeps no digest |

Only `confirmed` and `partial` results become chain rows and are delivered. A later `confirmed` result upgrades a `partial` row with the same digest; nothing downgrades a confirmed row.

## Durable state (per bank)

`apps/privacy_node/src/privacy_node/store.py`, SQLite for M2. The same SQL moves to the bank's PostgreSQL for M3.

| Table | Key | Purpose |
| --- | --- | --- |
| `replies` | `(query_id, sender\|ref)` | Stored final replies for idempotent retries |
| `searches` | `search_id` | Every search this bank started and its latest state |
| `chains` | `chain_digest` | One row per chain at each bank; the seed for one alert per bank ([case.md](case.md)) |
| `outbox` | `(chain_digest, bank)` | Results not yet acknowledged by an involved bank |

## Failure contract

- A missing bank can't confirm an unknown link. A search that needs it becomes **pending**; it keeps the claims other banks gave, and no chain row is created. Searches that don't need that bank keep going.
- **Resuming after recovery:** housekeeping (every 10 s) re-runs every pending or timed-out search, then redelivers every unacknowledged outbox row. An error in one pass is logged and the next pass runs as normal. Tested: C offline → pending; C back → confirmed and stored at A, B and C.
- **Initiator restart:** pending searches and outbox rows are durable, so they resume after a restart, provided the store file survives (`--db`); M3 moves it to PostgreSQL.
- **Retained-state lifetimes (**pending work**):** stored replies expire after the search deadline plus a margin; pending searches expire after 24 h as unresolved; outbox rows are kept until acknowledged or dead-lettered.
- **"Exactly one alert"** holds for eventual recovery with intact storage, not permanent data loss. The protocol side gives one `chains` row per digest at each bank, tested under repeated searches and redelivery.
- Concurrent replicas will share query state in PostgreSQL (`INSERT ... ON CONFLICT DO NOTHING`). Until then, run one replica per bank. **pending work:** 2 replicas; each housekeeping pass takes a session-level `pg_try_advisory_lock`, so one replica works at a time and the lock is released if its session dies. Duplicates are harmless because receivers are idempotent.

## Result delivery to the case service

**pending work (Zane, with Jia Rui):** Nothing calls the case service's `POST /results` today. The node becomes the **only writer** to its own bank's case service, through the transactional outbox (a `case` destination row), so a case-service outage can't stall the scorer or searches. Each payload is idempotent:

| Payload | Key | Contents |
| --- | --- | --- |
| Chain | `chain_digest` | The `ChainResult`, plus this bank's own legs (payment ID, account, amount) from the token index. Sent when the `chains` row is inserted or upgraded |
| Tier 1 hit | hit ID | The scorer's hit from Valkey: rule, account, payments |
| Pending search | search ID | "Waiting for bank X", upgraded when the retry confirms |
| Fully internal chain | hit ID | A local-only chain, with no peer search |

Own-leg details travel only inside the bank, never in a peer message. The case service never reads Valkey.

## Running and testing

```sh
uv run pytest apps/privacy_node
uv run python -m privacy_node.demo          # three banks on localhost, prints each trace
uv run python -m privacy_node.keys .secrets A --key-version k1 # Bank A local keys only
```

For a deployed bank, use `--key-directory /run/bank-keys` and mount this bank's
`active.json` alongside its key tree (`--key-config` can choose another path):

```json
{
  "active_versions": ["k1", "k2"],
  "query_version": "k1",
  "signing_key_version": "k2",
  "peer_versions": {"A": ["k1", "k2"], "C": ["k1", "k2"]}
}
```

This example is for B. The manifest and public keys are trusted deployment inputs,
never supplied by a peer. Every housekeeping pass reopens the manifest and files:
`Node.reload_key_directory(root, config)` introduces fresh versions, gates the query
switch on peer readiness, retires versions omitted from `active_versions`, and reloads
versioned signing keys. Newly ingested payments are indexed under every active version;
old in-flight requests keep their request's version even after the query switch. Retire
only after old index/in-flight lifetimes expire. Retained old signing public keys still
verify historical evidence. Mount directories (no Kubernetes `subPath`); no restart needed.
Explicit `--x25519-private-key`, `--peer-x25519-public-key PEER PATH`,
`--signing-key` and repeated `--public-key BANK VERSION PATH` are also supported and
reread, but discovering new active versions uses the directory manifest.

mTLS requires `--tls-cert`, `--tls-key`, `--tls-ca`; the authenticated URI SAN is
`spiffe://csc3104/bank/{BANK}/privacy-node` and must match `sender`. TLS renewal files
are reread on new connections, with server/client chain verification and client-side
hostname verification intact. `--insecure-demo` is explicit lab-only transport.

Regenerate the stubs after editing the `.proto` (from the repo root):

```sh
uv run python -m grpc_tools.protoc -Iapps/privacy_node/src --python_out=apps/privacy_node/src --pyi_out=apps/privacy_node/src --grpc_python_out=apps/privacy_node/src apps/privacy_node/src/privacy_node/privacy_node.proto
```

Tiny-fixture results (`demo.py`):

| Search from | Detection | State | Chain |
| --- | --- | --- | --- |
| B | p06 → p07 | confirmed | `m1`: A → B → C |
| B, C | p10 → p11, p11 → p12 | confirmed | `o1`: one digest from both starts |
| A, B, B, C, C | `v1` links | partial | 5 transfers followed, the 6th named at the cut; one digest from all starts |
| — | `l1`, `x1`, ordinary | no search | no local detection |

Each bank stores 3 chains (2 confirmed, 1 partial) after all searches.

## Interfaces

Agreed contract first ([conventions](../README.md#conventions)): the producer adds one sample message here, and both sides' tests load it.

| Interface | From → to | Pins down | Status | Agree by |
| --- | --- | --- | --- | --- |
| `.proto` v1 ([protocol.md](protocol.md#messages)) | Zane ↔ Caleb | `bytes` token, no absolute deadline, unknown fields rejected, the schema test's allow and forbid lists | **implemented**, shared sample above | Wed 4 Nov |
| Node → case service ([protocol.md](protocol.md#result-delivery-to-the-case-service)) | Zane → Jia Rui | Payloads (chains with own legs, Tier 1 hits, pending and internal cases), keys, retry and dead letter | **draft**, sample TODO | Wed 4 Nov |

Also part of: [Token and key API](security.md#interfaces), [Valkey layout](streaming.md#interfaces), [Alert tiers](case.md#interfaces), [`check_result`](security.md#interfaces).

## Open

- [x] Claim bytes, approved schema fields, digest and protocol hop names reviewed and enforced by tests (Caleb).
- [x] mTLS CLI requires certificate/key/CA, verifies peer bank URI SAN against `sender`, and rereads renewal files. `--insecure-demo` is explicitly localhost-only; chart Secret mounts remain Isaac integration.
- [ ] Move `store.py` to PostgreSQL, then test two replicas per bank (Zane; **pending work**).
- [x] `BRANCH_CAP = 1` and `HOP_LIMIT = 4`, checked by a 4/6/8 sweep; shared state moves to Valkey and PostgreSQL.
- [x] `chain_digest` is the case identity; the case service merges overlapping partial chains by shared transfers.
- [x] Retention lifetimes: see the [failure contract](#failure-contract).
- [x] Late events: keep the ingestion grace (`--ingestion-grace-seconds`), with its default set from measured ingest lag and `retry_until` stored in the `searches` row.
