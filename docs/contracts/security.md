# Security and privacy contract

Owner: Caleb (Role 4). Interfaces: Cindy (tokens/index), Zane (peer protocol), Jia Rui
(cases/reports), Isaac (deployment). This describes the implemented coursework code.
Cluster and AWS verification still requiring teammates is identified below.

## Threat model and disclosure boundary

Banks and their local runtimes are trusted to apply their rule honestly. Trusted public
key provisioning and the shared lab CA are trusted third parties. A compromised bank
exposes its own raw records, X25519/signing private keys and derived pair keys; the
cluster administrator can access all namespaces. Signatures prove authorship and
integrity, not truthful records, freshness or laundering. Participating banks can abuse
lookups they are authorized to make; no rate limiter or independent audit system is supplied.

Raw payment IDs, accounts, amounts, customer data, full histories and private key material
never enter a **cross-bank** message. Evaluation labels `chain_id`, `hop`, `scenario`,
`altman_laundering` are also forbidden. The protocol metadata `chain_digest` and
`hops_remaining` are approved and are distinct from those evaluation labels.

An analyst may see their **own** bank's payment ID/account/amount snapshots. Other banks'
legs remain minimal signed tokens. `privacy_node.case_delivery.own_case_legs` joins only
own-bank signed refs to bank-local records. The internal multipart `/results` interface
and report boundary are specified in [case.md](case.md#internal-own-bank-enrichment).
The receiver rejects another bank's details, a reference to another bank's claim, and
unapproved fields before writing. The node is trusted to label its own records truthfully;
these checks cannot identify a foreign value deliberately mislabeled as local. The HTTP
case endpoint is bank-internal, restricted by NetworkPolicy, without separate app auth.
It never reads Valkey. Zane's durable internal delivery outbox remains an integration item.

Logs should contain IDs, banks, state and timing, without raw payload dumps. Hex tokens
are allowed only at human-facing display/report boundaries. Peer protobuf, index keys,
visited references, digest/signature input and SQLite token columns retain raw bytes.
Existing coursework SQLite databases need to be recreated after this schema change.

## Token and key API

```python
derive_token(
    payment_id: str, bank_a: str, bank_b: str,
    shared_key: bytes, key_version: str,
) -> bytes
```

`shared_key` is the 32-byte X25519/HKDF pair key below. Bank IDs are exact/case-sensitive,
distinct and sorted canonically. Payment ID is used unchanged (no trim or case folding).
`key_version` matches `[A-Za-z0-9_-]+` and is included in the HMAC input. The existing
ASCII JSON encoding, no spaces, is unchanged:

```text
["csc3104.payment-token","v1",["A","B"],"k1","p01"]
```

Compute full HMAC-SHA256 and return its **leftmost 16 raw bytes**. There is no textual
prefix. `TokenRef.token` is protobuf `bytes`; the protocol package is
`csc3104.privacy.v1`. Token version is carried separately in `TraceRequest.key_version`
and `LinkClaim.key_version`. Index lookup is `(canonical_pair, key_version, token)`;
case transfer identity also includes that version. A/B and B/A derive identical bytes;
changing payment, pair, key or version changes the token.

Frozen interoperability vector (public, deterministic **test-only** key):

```python
key = b"A" * 32
expected = b"\x86\xef\x4e\x3f\x81\x1c\x07\x7d\xcd\xe1\x0d\x9c\xef\x4d\x80\x7c"
# Readable hex: 86ef4e3f811c077dcde10d9cef4d807c
assert derive_token("p01", "A", "B", key, "k1") == expected
assert derive_token("p01", "B", "A", key, "k1") == expected
```

### X25519 plus HKDF

Each bank generates a fresh X25519 key pair for **each active token version**. A owns
only A's private material; authenticated provisioning supplies trusted peer public files.
For A-B at k1, both endpoints compute:

```text
HKDF(SHA256, length=32, salt=None,
     info=b"csc3104 pair-key A-B k1").derive(X25519(local_private, peer_public))
```

Pair order and version are canonical context. Pair secrets are derived locally and never
transported or written as deployment artifacts. The normal CLI generates **one bank**:

```sh
uv run python -m privacy_node.keys .secrets A --key-version k1
```

Run independently at B/C; exchange only authenticated `public/` artifacts, never
`private/`. The localhost fixture demo simulates these independent generations and
explicitly copies public files only. It is not a trusted pair-secret dealer.

Exact PEM layout (PKCS8 private; SubjectPublicKeyInfo public), shown for A:

```text
.secrets/A/private/x25519/k1.pem
.secrets/A/private/signing/k1.pem
.secrets/A/public/x25519/k1.pem
.secrets/A/public/signing/k1.pem
.secrets/A/trusted/x25519/B/k1.pem
.secrets/A/trusted/x25519/C/k1.pem
.secrets/A/trusted/signing/B/k1.pem
.secrets/A/trusted/signing/C/k1.pem
.secrets/A/active.json
```

Own public signing keys are loaded from `public/signing/<version>.pem`. Peer trust
files are addressed by `(peer bank, version)`; k2 uses sibling `k2.pem` files. Keep all
private material outside git (`.secrets/` is ignored), in read-only Secret directory
mounts. Public key distributions must be authenticated by the trusted operator.

```python
load_pairwise_keys(
    bank_id: str, key_version: str, local_private_key: str | Path,
    peer_public_keys: Mapping[str, str | Path],
) -> dict[tuple[str, str], bytes]
```

`AgreementFiles(private_key, peer_public_keys)` packages those paths for
`TokenIndex(bank, key_version, files)`. `privacy_node.keys.pair_paths(root, bank,
version)` discovers trusted peer X25519 files. No raw pair-secret loader/generator remains.

### Signing keys and purpose separation

Ed25519 signs `claim_bytes`; X25519 derives pair HMAC keys; TLS keys authenticate the
connection. These are separate key pairs, files and purposes. `LinkClaim.key_version`
is the **token** version; explicit `LinkClaim.signing_key_version` is the signing version.
`valid_claim` looks up trusted `public_keys[(signer, signing_key_version)]`; no public key
is taken from the claim. Old/new verification keys coexist. Type-specific PEM loaders
reject keys of another purpose. Exact canonical signed bytes and the signed `truncated`
flag are documented in [protocol.md](protocol.md#signed-claim-bytes).

Case verification mounts use `<CASE_PUBLIC_KEYS>/<bank>/<signing_version>.pem`, including
this bank's own public key. The CLI re-reads that directory per `/results` request.

## Rotation and Secret reload

1. Begin with k1 active. Generate genuinely fresh X25519 material at each endpoint for k2;
   `make_keys` refuses to overwrite existing files. Reusing local or peer public material
   under another active version is rejected by the index.
2. Publish trusted k2 public files and introduce k2 at both banks. `introduce_version`
   backfills retained local token references into k2, then new events index under **both**.
   Every overlapping version must cover the same configured bank pairs, even before any
   payment for one pair has arrived; missing public files cannot silently drop a peer.
   Capacity is checked before mutating the active index. Backfill prevents an old pending
   search from missing its previously ingested payment after the query-version switch.
3. Keep queries at k1 until trusted readiness configuration lists k2 for every required peer.
   `set_query_version` rejects premature switch. Then switch queries to k2.
4. In-flight k1 requests continue to lookup and forward refs in their requested version;
   signing version is independent. Old signing public keys remain available for evidence.
5. Once old work/index retention has drained, retire k1. Only k1 entries/material are removed;
   k2 continues working. Retention/expiry in the future Valkey implementation is Cindy/Zane's
   integration work; the current in-memory index does not auto-expire entries.

`Node.reload_key_directory(root, config)` re-reads mounted files. CLI
`--key-directory ROOT --key-config PATH` enables reload every `--housekeeping-seconds`
(default 10); the default config path is `ROOT/<bank>/active.json`. Example for B:

```json
{"active_versions":["k1","k2"],"query_version":"k1","signing_key_version":"k2",
 "peer_versions":{"A":["k1","k2"],"C":["k1","k2"]}}
```

Update trusted mounted configuration to introduce/switch/retire versions. The readiness
manifest is an operator assertion of deployment readiness, not an unauthenticated peer
announcement. Publish all required files before the manifest. Mount directories without
`subPath`, so Kubernetes Secret updates are visible. No daemon or pod restart is needed.
The simpler explicit-path CLI mode re-reads its configured PEM paths on housekeeping;
use directory mode to discover new versions. Same-chain digests change across token
versions; cross-version case deduplication is not implemented (Jia Rui integration choice).

### Live demo

```sh
uv run python -m privacy_node.rotation_demo
```

This runs a fixture replay on k1, generates fresh per-bank k2, dual-indexes a new replay
segment, proves both versions lookup, switches only when ready, verifies the same
A -> B -> C link (including a pre-rotation payment), then retires k1 and confirms k2
again. Keys live only in a temporary directory. Its explicitly insecure localhost
transport isolates rotation evidence; mTLS evidence is tested separately below.
For the cluster replay use the same five stages through mounted `active.json`; do not
retire k1 until old work has drained. Capture the demo output as experiment 12 evidence.

## Schema privacy and shared result verification

The descriptor test recursively visits **every RPC input/output and nested message**.
Each actual field set must exactly match the approved per-message set in
`claims.APPROVED_FIELDS`; no field may intersect unchanged `security.FORBIDDEN_FIELDS`.
A field addition fails CI until explicitly reviewed. There is no runtime protobuf-to-dict
allowlist walk. Receivers reject unknown wire fields recursively with `INVALID_ARGUMENT`
(case HTTP returns 422). `check_peer_payload` remains for minimal untyped JSON; approved
own-bank report details use their separate internal policy, never a weakened forbidden set.

Approved fields:

| Message | Fields |
| --- | --- |
| TokenRef | pair, token |
| TraceRequest | query_id, sender, key_version, ref, direction, hops_remaining, visited |
| LinkClaim | signer, recipient, query_id, key_version, in_ref, out_ref, signature, signing_key_version, truncated |
| TraceReply | state, claims |
| ChainResult | chain_digest, sender, state, claims |
| Ack | chain_digest |

Field 7 of TraceRequest is reserved: the former absolute deadline is removed. Nested
calls forward the remaining **relative gRPC timeout** from `context.time_remaining()`;
the initiator uses a local monotonic deadline. No second timeout field is introduced.
The shared producer/consumer example is [trace_request.textproto](samples/trace_request.textproto).

`check_result(result, public_keys, bank) -> ChainResult` lives only in `claims.py`.
It raises `ValueError` on rejection and returns a **copy** on success. Deliver and case
`/results` must store the returned copy. It verifies:

1. Only CONFIRMED/PARTIAL are deliverable, and the receiving bank signed a claim.
2. The digest exactly matches claims and every claim verifies with its trusted versioned key.
3. Signers are endpoints of their canonical 16-byte refs; every interior join has a nonempty matching transfer and token version. Empty refs cannot splice disconnected chains.
4. Exactly one sender-authored self-recipient anchor exists.
5. Each upstream/downstream leg is bound to one query ID and the correct successive requester.
6. Open/cut endpoints or any **signed** `truncated` hop/branch flag force PARTIAL, even if
   labeled CONFIRMED. Removing the flag invalidates its signature. The input is not mutated.

No duplicated case `verify_result` remains. Malformed, unknown, tampered, unbound,
uninvolved, missing-key and wrong-version results cannot create confirmed evidence.

## mTLS bank identity and deployment review

cert-manager's existing `bank-ca` ClusterIssuer supplies both client/server EKUs, DNS SANs
`privacy-node.<namespace>.svc[.cluster.local]`, and URI
`spiffe://csc3104/bank/A/privacy-node` (bank derived from bank-a/b/c namespace). The server
maps authenticated `x509_subject_alternative_name` to exactly one approved bank URI,
then checks `sender` **before cache access or state writes**. A CA-trusted certificate
without that identity is insufficient. Clients verify CA and DNS hostname; no override.

CLI requires `--tls-cert /tls/tls.crt --tls-key /tls/tls.key --tls-ca /tls/ca.crt` unless
explicit `--insecure-demo` is used locally. Mount `privacy-node-tls` as a directory.
`TlsFiles` rereads client credentials for each new call; gRPC dynamic server credentials
reread renewed material for each new connection. Existing TLS sessions retain their
credentials. Real local handshake tests cover renewal of both ends/CA without restart.
One shared lab CA can impersonate any bank: this is an explicit coursework trust limitation.

Isaac must review the small chart/policy changes. Default deny with DNS, selected bank-local
Kafka/PostgreSQL/Valkey/case/intake paths and cross-bank **privacy-node pods only** on
TCP 50051 replace whole-namespace allowances. Cross-bank Kafka/PostgreSQL and unauthorized
namespaces stay blocked. See [deployment.md](deployment.md#namespaces-and-network-paths)
for exact selectors, operator paths and API CIDRs. Replay/summary ports, app Deployments,
Secret mounts and EKS identity paths are still Isaac-owned; no guessed broad path is open.

Rendered Helm assertions test allowed/blocked pairs; these are configuration evidence,
not proof the CNI enforces traffic. k3d verification commands (setup was attempted; probes did not run):

```sh
bash deploy/k3d-up.sh
bash deploy/check-netpol.sh | tee netpol-evidence.txt
```

Expect all PASS: bank-local store/case OPEN, privacy-node cross-bank 50051 OPEN; scorer
cross-bank peer, cross-bank Kafka/PostgreSQL, unauthorized namespace and wrong local app
BLOCKED. The script first proves blocked destinations OPEN for an authorized source.
Run under an enforcing CNI and retain output. Temporary app listeners do not join Service
endpoints and are deleted on exit; this tests TCP policy, not TLS. Repeat on EKS with
correct API CIDRs/VPC CNI enforcement. Narrow the current k3d node-subnet API range.
The [live attempt record](../evidence/role4-k3d-attempt.txt) documents that the task cluster
and operators started, but the API/Docker engine failed during bank-store readiness, before
probes or issued-certificate inspection. This is an integration/environment gap, not live
policy pass evidence; the automated rendered assertions and local TLS tests remain valid.

## Lambda Function URL review

There is **no** deployed Lambda or Function URL caller in this repo; reports currently use
local `TemplateClient` and legacy mock intake. The summary will receive own-bank customer
details. Isaac must create an `AWS_IAM` Function URL (never `NONE`), scope
`lambda:InvokeFunctionUrl` and `lambda:InvokeFunction` to the case workload's Pod Identity
role and its summary function, without public grants or static AWS keys. Jia Rui must
SigV4-sign requests using refreshable default SDK credentials from EKS Pod Identity.
The future summary HTTPS/credential network paths must be explicitly allowed by Isaac.

```sh
aws lambda get-function-url-config --function-name "$SUMMARY_FUNCTION" --query AuthType --output text
```

It must print `AWS_IAM`; unsigned HTTPS and another workload role must fail, while the
case Pod Identity caller succeeds. These cloud tests remain blocked on Isaac/Jia Rui's
actual deployment/client. The configuration regression test guards against future
`AuthType: NONE` or static AWS credentials; it does not claim absent infrastructure is secure.

## Residual disclosure and integration limits

Tokens are pseudonymous: stable within pair/version, linkable across repeated queries,
and guessable by a pair-key holder for predictable payment IDs. Intermediaries connect
their own incoming/outgoing legs. Bank identities, timing, query frequency, chain order
and claim outcomes remain visible. A 128-bit token has a nonzero collision probability;
this coursework has no collision-resolution mechanism. TLS/labels/Secret provisioning
rely on the trusted lab operator; this is not production PKI or tenant isolation.

Remaining external work: Zane's durable own-case delivery and shared Postgres/Valkey state;
Cindy's live Valkey TTL/index integration; Jia Rui's summary caller, UI redesign and
cross-version case reconciliation if desired; Isaac's app/chart mounts, enforcing-CNI
probe evidence, final certificate runtime deployment and Pod Identity/Lambda IAM.
Caleb's cluster/cloud acceptance evidence can be captured after those integrations exist.

## Tests

From the repository root (Python version and dependencies are pinned by workspace files):

```sh
uv sync --all-packages --locked
uv run ruff check .
uv run ruff format --check .
uv run pytest
uv run pytest apps/security apps/scorer apps/privacy_node apps/case
```

Install Helm 3.19.0 (or set `HELM` to its path) to execute rendered deployment tests;
without Helm those tests explicitly skip. `.github/workflows/checks.yml` installs Helm
and runs the full suite, including recursive schema approval, plus exact protobuf
regeneration/diff verification. No runtime keys or test certificates are committed.

Reproducible focused demonstrations:

```sh
uv run pytest apps/security/tests/test_security.py -k token -v
uv run python -m privacy_node.rotation_demo
uv run pytest apps/privacy_node/tests/test_claims.py -k 'attack or cut or signing' -v
uv run pytest apps/case/tests/test_security_boundary.py -v
uv run pytest apps/privacy_node/tests/test_transport.py -v
uv run pytest apps/privacy_node/tests/test_deployment_security.py -v
```

The TLS tests run actual trusted/no-client-cert/untrusted-CA/wrong-bank/wrong-SAN/hostname
handshakes, sender mismatch through real Node RPCs, and credential renewal. The case tests
prove false confirmed cuts stay partial, all receiver attacks reject, raw tokens remain
SQLite BLOBs, and only own-bank details reach internal reports. Full results and exact
environment constraints are recorded in [security-delivery.md](../security-delivery.md).

Implementation references: [X25519](https://cryptography.io/en/latest/hazmat/primitives/asymmetric/x25519/),
[HKDF](https://cryptography.io/en/latest/hazmat/primitives/key-derivation-functions/#hkdf),
[gRPC credentials](https://grpc.github.io/grpc/python/grpc.html),
[cert-manager Certificates](https://cert-manager.io/docs/usage/certificate/),
[Kubernetes NetworkPolicy](https://kubernetes.io/docs/concepts/services-networking/network-policies/),
[Lambda IAM invocation](https://docs.aws.amazon.com/lambda/latest/dg/urls-invocation.html).
