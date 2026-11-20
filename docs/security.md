# Security and privacy

How banks cooperate without showing each other customer data, and how each bank's pods are locked down. The messages themselves are in [protocol.md](protocol.md).

## What crosses between banks

Only the privacy node talks to other banks, and its three calls ([protocol.proto](../src/mulechain/node/protocol.proto)) can carry only this:

| Crosses | Never crosses |
| --- | --- |
| Public keys (`Keys`) | Private keys and pair keys |
| Tokens: 16 opaque bytes naming a transfer | Payment IDs, accounts, amounts, times, customer details |
| Signed link claims: signer, the two neighbouring banks, two tokens, a `cut` flag | Which rule fired, and anything about a bank's other transfers |
| Search limits: direction, hops left, tokens already visited | Evaluation labels |

Payment IDs, accounts and amounts are read from Valkey by the node ([bank.py](../src/mulechain/node/bank.py)) and go only to the same bank's case service. `test_nothing_but_tokens_crosses_between_banks` ([test_system.py](../tests/test_system.py)) records every peer message of a full run and checks that none holds an account, and that the message types have exactly the approved fields, so a new field cannot slip in unnoticed.

## Tokens

A token is `HMAC-SHA256(pair key, "mulechain token " + payment ID)`, cut to 16 bytes (`token` in [crypto.py](../src/mulechain/crypto.py)). Both banks on a transfer know its payment ID and hold the same pair key, so both compute the same token. A third bank holds neither, so it cannot compute the token or turn one back into a payment ID. The scorer writes each cross-bank transfer's token to Valkey as it scores it ([detection.md](detection.md)).

## Key agreement with no dealer

No party hands out keys. Each bank makes its own and shares only the public halves.

1. Each bank's Helm release generates 64 random bytes per key version into the Secret `bank-keys` ([keys.yaml](../deploy/charts/bank/templates/keys.yaml)): 32 for key agreement (X25519) and 32 for signing (Ed25519). Files are named `1`, `2`, ... oldest first (`load_keys`).
2. A node answers the `Keys` call with its public keys. Every node asks its peers every 10 seconds (`refresh_keys` in [node.py](../src/mulechain/node/node.py)), stores the answers in PostgreSQL (`peer_keys`) so a restart has them, and mirrors them to the Valkey hash `keys` for its scorer.
3. Each side derives the pair key on its own: `HKDF(X25519(my private key, your public key))`, with both bank names in the HKDF context (`pair_keys`). The scorer re-derives them every 10 seconds (`PairKeys` in [scorer.py](../src/mulechain/scorer.py)).

`test_two_banks_derive_the_same_token_and_a_third_cannot` ([test_crypto.py](../tests/test_crypto.py)) shows the result.

## Key rotation

A bank changes its keys alone, while payments flow, and nothing already stored is re-indexed.

```sh
make key-add BANK=b KEY=2     # step 1: add a key version
# wait five minutes
make key-drop BANK=b KEY=1    # step 2: remove the first one
```

Three rules make every token findable at each step:

- **Index under every pair.** The scorer stores a transfer's token for every (own key, peer key) pair it knows.
- **Name with own oldest, peer's newest.** When a bank asks a peer about a transfer it uses the token from its own oldest key and the peer's newest key (the first entry `pair_keys` returns). The answering bank uses the asker's token in its claim.
- **Publish late.** A node tells peers about a change to its own keys only after the change has lasted `KEY_SETTLE_SECONDS` (120 s). A bank's pods see a changed Secret up to a minute apart. The wait means every scorer already uses a new key before any peer can name a transfer with it, and every scorer has stopped using a removed key before peers forget that key.

The five-minute wait covers the Secret reaching every pod, the settle time, the peers' next fetch and their scorers' next reload. A node signs with its oldest key, and a signature verifies under any key the signer has published (`verify`).

Tests: `test_a_rotation_never_leaves_a_token_the_peer_has_not_indexed` ([test_crypto.py](../tests/test_crypto.py)) and `test_a_key_change_is_published_only_after_it_has_settled` ([test_system.py](../tests/test_system.py)).

Limits:

- The settle timer is in memory. A node that restarts inside the two minutes publishes the changed keys at once, so a transfer scored in that window may get a token the peer has not indexed.
- Once a key is dropped and unpublished, a chain signed with it can no longer be verified. A delivery of such a chain that is still being retried is refused for good.
- During the overlap the two banks on a transfer may name it with different tokens, so the same chain can be stored under more than one ID. Each bank's case service merges them by payment ID ([cases.md](cases.md)). Loop detection compares tokens, so it can miss a loop then; the hop limit still bounds the chain ([protocol.md](protocol.md#known-limits)).

## Signed link claims

Each bank on a chain signs one claim with Ed25519: "I received `in_token` from `in_from`, and my rule links it to `out_token`, which I sent to `out_to`". The signature covers every field, each length-prefixed (`claim_bytes` in [claims.py](../src/mulechain/node/claims.py)).

`valid_chain` accepts a chain only if every claim verifies under its signer's published keys and each transfer is confirmed from both ends: the sender's claim and the receiver's claim must name each other and the same token. One bank cannot vouch for both sides. A chain's ID is a hash of its claims (`digest`), so no bank can relabel it.

A signature proves who made a claim and that nobody altered it. It does not prove that the bank's records are true or that the money is criminal. Banks are assumed to apply the rules honestly.

`Deliver` refuses a chain that fails these checks or that does not include both the sender and the receiver as signers (`test_a_forged_chain_is_refused_and_a_repeated_one_is_harmless`).

## mTLS between privacy nodes

Every call between nodes uses mutual TLS: both sides present a certificate.

- **Certificates.** cert-manager issues one per bank from the lab CA ([ca.yaml](../deploy/charts/platform/templates/ca.yaml), [certificate.yaml](../deploy/charts/bank/templates/certificate.yaml)) with a DNS name, `privacy-node.bank-<x>.svc`, and a URI, `spiffe://csc3104/bank/<X>/privacy-node`.
- **Identity.** The URI is the bank's identity. `caller()` in [node.py](../src/mulechain/node/node.py) reads it from the client certificate and refuses a `Trace` or `Deliver` call as `UNAUTHENTICATED` unless it names exactly one bank. `Keys` returns only public keys and answers any certificate from the lab CA. No message has a sender field, so a bank cannot claim to be another.
- **Renewal.** `Mtls` reads the certificate files again for every connection, as client and as server, so a renewed certificate is used without a restart.

Checked in [test_mtls.py](../tests/test_mtls.py) with real gRPC connections, and on a cluster by the mTLS checks in [test_cluster.py](../deploy/test_cluster.py): a caller with no certificate or a self-signed one cannot connect, a lab certificate that names no bank is `UNAUTHENTICATED`, and a renewed certificate is served by the same pods with no restart.

## Who may ask what

A valid certificate is not enough to ask about any transfer. Tokens travel inside claims, so a third bank on a chain can learn one. `Bank.lookup` ([bank.py](../src/mulechain/node/bank.py)) answers a `Trace` only when the caller is the bank on the other side of that transfer; anyone else gets the same empty reply as for an unknown token (`test_the_certificate_decides_what_a_caller_may_ask`, and `test_a_node_answers_only_the_bank_on_the_other_side_of_a_transfer` on the cluster).

## Network paths

Every bank namespace starts with no network access in either direction, apart from DNS and the Kubernetes API server (`bank-baseline`). Each path a service needs is then allowed by name at both ends: the client may send and the server may receive ([networkpolicy.yaml](../deploy/charts/bank/templates/networkpolicy.yaml)).

| Path | Port | Who else is refused |
| --- | --- | --- |
| Replay Job → ingestor | 8080 | Everything inside the bank |
| Ingestor, scorer, KEDA → Kafka | 9092 | The privacy node, other banks, other namespaces |
| Kafka brokers ↔ each other and the Strimzi operator | any | Everything else |
| Scorer, privacy node → Valkey | 6379 | The case service, other banks |
| Privacy node, case service → PostgreSQL | 5432 | The scorer, other banks |
| PostgreSQL instances ↔ each other and the CloudNativePG operator | any | Everything else |
| Privacy node → its case service | 8000 | The scorer, other namespaces |
| Case service → summary service | 8080 | The privacy node, other namespaces |
| Privacy node ↔ the other banks' privacy nodes | 50051 | A node in its own bank, any other pod in any bank |

`make cluster-check` tries each of these from inside the pods. Every blocked target is also shown open from a pod that may reach it, so a missing service cannot pass as a blocked one. The AWS-only paths are in [deployment.md](deployment.md#aws).

## Pod hardening

- Bank namespaces enforce the **restricted Pod Security Standard** ([namespaces.yaml](../deploy/charts/platform/templates/namespaces.yaml)): Kubernetes rejects a pod that is not non-root, drops all capabilities and uses the default seccomp profile. The shared settings are in [_helpers.tpl](../deploy/charts/bank/templates/_helpers.tpl), with a read-only root filesystem.
- None of the chart's service pods mounts a Kubernetes API token.
- Keys and certificates are mounted from Secrets as whole directories. On k3d, Secrets are encrypted at rest (`--secrets-encryption` in [k3d.yaml](../deploy/k3d.yaml)); the Terraform for EKS configures no Secrets encryption of its own.
- The image runs as user 65532 and holds no state.

## Residual disclosure

What a bank still learns about others:

- **Who and in what order.** A chain names every bank on it and their order, and it is delivered to every bank that signed part of it.
- **That a rule fired.** A claim shows that the signer's rules link two transfers, and `cut` shows that it knows of more.
- **Linkable tokens.** A token is the same every time the same two banks name the same transfer, so a bank can recognise a transfer it has seen before. It sees the tokens of other pairs on its chains but cannot turn them into payment IDs or ask about them.
- **Timing.** When a peer asks, and how quickly a bank answers.

## Trust limits

- **One shared lab CA** signs every bank's certificate, so whoever holds it can impersonate any bank. Real banks would each bring a certificate from an authority the others trust.
- **Namespaces simulate banks.** They share a cluster, its nodes and its operators, and cluster DNS answers for other banks' service names. This is not a production security boundary.
- **Published public keys are trusted** as received over mTLS; nothing else vouches for them.
- **On AWS** some isolation is weaker, because the lab account offers only one shared role: the case service pod can take the whole node role to sign its summary calls, each bank's Kafka listener for its ingestor Lambda admits the whole VPC, and pods in a namespace without a default-deny policy can reach every bank's ElastiCache.
