# Security and privacy contract

Owner: Caleb (Role 4). This contract covers tokens, key agreement, signatures,
shared result validation, mTLS and the cross-bank privacy boundary. Deployment,
protocol scheduling and case enrichment remain with their respective owners.

## Token and key API

```python
derive_token(payment_id: str, bank_a: str, bank_b: str,
             shared_key: bytes, key_version: str) -> bytes
load_pairwise_keys(bank_id: str, key_version: str, local_private_key,
                   peer_public_keys) -> dict[tuple[str, str], bytes]
```

Tokens are the **leftmost 16 raw bytes of HMAC-SHA256**, without a textual prefix.
The input is compact ASCII JSON, with exact payment IDs and sorted, distinct bank IDs:

```text
["csc3104.payment-token","v1",["A","B"],"k1","p01"]
```

`key_version` uses ASCII letters, digits, `_` or `-`. Payment IDs are neither trimmed
nor case folded. Public test vector: `derive_token("p01", "A", "B", b"A" * 32, "k1")`
is `bytes.fromhex("86ef4e3f811c077dcde10d9cef4d807c")`; reversing A/B is identical.
Changing the payment, pair, version or key separates tokens.

Each bank holds its own X25519 private key and authenticated peer public keys.
For A-B/k1, both endpoints derive the same 32-byte HMAC key:

```text
HKDF(SHA256, length=32, salt=None,
     info=b"csc3104 pair-key A-B k1").derive(X25519(local_private, peer_public))
```

Pair order and version are part of the HKDF context. Derived pair secrets stay local;
only public artifacts are exchanged. `AgreementFiles(private_key, peer_public_keys)`
passes those paths to `TokenIndex(bank, version, files)`. Ed25519, X25519 and TLS
use separate key pairs and purposes; PEM loaders enforce the expected key type.

Generate one bank locally, then authenticate/distribute its public files:

```sh
uv run python -m privacy_node.keys .secrets A --key-version k1
```

Paths relative to the key root, shown for A:

```text
A/private/{x25519,signing}/k1.pem
A/public/{x25519,signing}/k1.pem
A/trusted/{x25519,signing}/{B,C}/k1.pem
A/active.json
```

Private PEMs use PKCS8; public PEMs use SubjectPublicKeyInfo. Generation refuses to
overwrite existing versions. The first run also writes a single-version
`active.json` (that version active, queried and signing, with both peers ready); later
versions leave it unchanged. Never commit private material; `.secrets/` is ignored.

## Versions, rotation and reload

`TokenRef.token` is protobuf `bytes`. Token identity is `(canonical_pair, key_version,
raw_token)` in peer messages and the index. At the case persistence/model boundary,
`Claim.from_pb` converts tokens and signatures once to stable hex text. Case transfer
identity is `(canonical_pair, key_version, token_hex)`; display/report paths use that text. Recreate existing coursework
case databases when adopting the new schema.

1. Introduce k2 at both endpoints using `introduce_version(version, files)`.
   Retained references are backfilled and new events index under both k1 and k2.
   Overlapping versions must cover the same configured pairs. Capacity failures
   leave the previous index intact.
2. Switch with `set_query_version(version, peer_versions)` only when trusted
   configuration confirms every required peer supports that version. In-flight
   searches keep the version they started with.
3. Retire k1 only after old requests and retained work drain. Lookup never falls
   back to another version. Old signing verification keys remain for stored evidence.

A new X25519 key pair at each bank is required for each rotated version; merely
relabeling old material is not a key rotation. The index does not compare
public material across versions: HKDF and HMAC already bind the version. Changing
derived keys within an existing version is rejected because it would invalidate
retained token references. Index retention/TTL integration remains Cindy's work.

`--key-directory ROOT` is the only node key mode and reads `ROOT/<bank>/active.json`. Example for B:

```json
{"active_versions":["k1","k2"],"query_version":"k1","signing_key_version":"k2",
 "peer_versions":{"A":["k1","k2"],"C":["k1","k2"]}}
```

Publish key files before updating this trusted readiness manifest. Mount read-only
Secret directories without `subPath` so renewals are visible. Directory mode discovers
new versions; the manifest is the single configuration source. Housekeeping starts
at most one background reload. Filesystem reads, key parsing and pair-key derivation
happen outside the local state lock; only validated state is applied under that lock.
Cheap material/readiness checks run before copying. Changed configuration uses a
shallow index copy with fresh key/entry dictionaries; unchanged configuration reuses
the index. Failed reloads retain the previous
working key set, and pending searches/deliveries continue while a read is slow.

The security rotation demonstration uses temporary keys and localhost demo transport:

```sh
uv run python -m privacy_node.rotation_demo
```

## Signing and shared result validation

`LinkClaim.key_version` scopes tokens; `signing_key_version` selects the trusted
Ed25519 public key at `(signer, signing_key_version)`. Old/new verification keys
coexist; claims never supply their own trusted key. The case app uses `CASE_KEY_DIRECTORY` plus `CASE_BANK` and the same
`keys.public_keys(root, bank)` loader as the node. It combines this bank's
`public/signing/<version>.pem` with `trusted/signing/<peer>/<version>.pem`, reread
per `/results` request. `create_app` always receives a loader, including in tests.

`claim_bytes` concatenates these fields, each preceded by a four-byte unsigned
big-endian byte length. Text uses UTF-8 and tokens use raw bytes:

```text
"csc3104.link-claim", "v1", signer, recipient, query_id, key_version,
signing_key_version, in_ref.pair, in_ref.token, out_ref.pair, out_ref.token,
ASCII("1" if truncated else "0")
```

Missing references have an empty pair and token. The signature covers versions,
query/recipient binding and truncation. Chain digests hash sorted distinct transfer
identities, framing each UTF-8 pair and raw token in the same length-prefixed form.
The matching peer schema and framing are documented in [protocol.md](protocol.md).

`claims.check_result(result, public_keys, bank)` is the single validator used by
PrivacyNode `Deliver` and case `/results`. It raises `ValueError` or returns a
verified copy; receivers store that returned copy. It guarantees:

- Only CONFIRMED/PARTIAL results involving the receiving bank are delivered.
- Digest, trusted versioned signatures, canonical pairs and 16-byte refs verify.
- Each signer owns its refs; a non-self recipient shares a claimed pair.
- Adjacent claims share a nonempty transfer and token version, with **different
  signers**. Since each pair has two endpoints, both banks must independently sign
  that shared transfer. One bank cannot satisfy both sides of an A-B relationship;
  one claim also cannot repeat its incoming token as its outgoing token. Every repeated
  transfer, including a closing loop reference, must have both endpoint banks as signers.
- Exactly one sender-authored self-recipient anchor exists. Each outward leg has
  consistent query IDs and successive requester/recipient bindings.
- Cut/open endpoints and signed `truncated` flags force PARTIAL without mutating input.

Legitimate A-B -> B-C chains and loops with independent adjacent banks remain valid.
Signatures prove authorship/integrity, not truthful records or laundering.

## Privacy boundary and schema review

Raw payment IDs, accounts, amounts, customer identifiers, full histories and private
keys must never enter cross-bank peer messages. Evaluation labels `chain_id`, `hop`,
`scenario` and `altman_laundering` are forbidden too. Approved protocol metadata
`chain_digest` and `hops_remaining` are distinct from those labels. Avoid payload dumps
in logs. Tokens are pseudonymous and linkable within their pair/version.

**Own-bank enrichment may occur internally, but raw own-bank details must never enter
cross-bank peer messages.** Another bank's raw details must never be requested or
forwarded peer-to-peer. Enrichment implementation and its internal API belong to
Zane/Jia Rui; this PR supplies only the security rule.

The schema security test recursively checks every RPC input/output and nested message
against exact per-message `APPROVED_FIELDS` and unchanged `FORBIDDEN_FIELDS`.
Caleb must review schema field additions before updating that approval list.
Zane's #17 supplies the integrated `has_unknown_fields` helper: Trace rejects unknown
fields before cache access, and shared `check_result` rejects them for both result
receivers. Outgoing calls reject unreadable replies, including undefined states.
Zane's transient/dead-letter behaviour is retained. The absolute deadline was removed
on 18 Nov (relative deadlines, [protocol.md](protocol.md#search-algorithm)): one fewer
`TraceRequest` field in `APPROVED_FIELDS`.

## mTLS and deployment requirements

The normal CLI requires `--tls-cert PATH --tls-key PATH --tls-ca PATH`.
`--insecure-demo` is only for explicit local demonstrations. Clients verify the trusted
CA and target DNS hostname; servers require a trusted client certificate. Both Trace
and Deliver bind `sender` to exactly one authenticated bank URI SAN **before** reading
cached replies or writing state:

```text
spiffe://csc3104/bank/A/privacy-node
```

Use A, B or C for the bank named by the certificate. A trusted certificate without
that bank identity is insufficient. Certificate provisioning must include the peer
service DNS names and both server/client authentication usages.

Mount certificate, private key and CA as a read-only directory (for example
`/tls/tls.crt`, `/tls/tls.key`, `/tls/ca.crt`), without `subPath`. `TlsFiles.read()`
rereads cert/key/CA bytes for every new client channel. The node opens and closes a
channel per peer call so connection-level Service balancing can select another
replica; the extra handshake cost is an accepted design trade-off. Server dynamic
credentials reread the mounted files for each handshake. Existing in-flight calls
finish using their established session; new calls use renewed material. Verification
and bank identity checks remain enabled. See the [Kubernetes gRPC balancing explanation](https://kubernetes.io/blog/2018/11/07/grpc-load-balancing-on-kubernetes-without-tears/).

Isaac owns chart changes and k3d/CNI verification. Required paths:

| Path | Requirement |
| --- | --- |
| Privacy node -> other bank's privacy node, TCP 50051 | OPEN, restricted to privacy-node workloads |
| Required bank-local Kafka, PostgreSQL, Valkey, case/intake and DNS paths | OPEN for the appropriate local clients |
| Any cross-bank PostgreSQL or Kafka path | BLOCKED |
| Unauthorized namespace/workload -> bank services or privacy peers | BLOCKED |

Isaac must collect live enforcing-CNI evidence for each allowed/blocked path, with
positive controls proving blocked destinations are otherwise reachable. Also inspect
issued bank URI/DNS SANs, usages and mounts; demonstrate valid peer handshakes,
wrong-bank/sender rejection and certificate renewal without restart. Local TLS tests
are application evidence; they do not establish chart correctness or CNI enforcement.

Live evidence (Isaac, k3d with bank A in HA, 20 Nov 2026; EKS not yet run):
`deploy/check-netpol.sh` passed 47/47 and `deploy/check-mtls.sh` 35/35:

- Each bank's `privacy-node` Certificate is Ready from `ClusterIssuer/bank-ca`, carries
  exactly its DNS and URI SAN with server and client auth, verifies against the lab CA,
  and each node pod mounts that Secret (read-only, as all Secret volumes are) at `/tls`
  with no `subPath`.
- Each node (A→B, B→C, C→A) calling the next bank's node with its own certificate gets a
  Trace answer (NO_MATCH). The same call is rejected with PERMISSION_DENIED ("sender
  differs from bank URI SAN") when `sender` names another bank. Right after another
  accepted A→B call each time, it is rejected with UNAUTHENTICATED ("trusted bank URI
  SAN required") for a lab-CA certificate with no bank URI SAN, and the server closes the
  handshake (UNAVAILABLE, "Socket closed") with no client certificate or a self-signed
  certificate carrying bank A's URI SAN.
- Renewing bank A's certificate (cert-manager's `Issuing` condition, as `cmctl renew`
  sets it) issues a new certificate into the same Secret; within about a minute
  both of bank A's pods serve the new serial in a real handshake, all three calls keep
  succeeding, and pod UIDs and restart counts are unchanged.

Not shown live: the node's own outgoing calls using a renewed client certificate (the
post-renewal calls come from a probe that reads `/tls` itself; the node's per-channel
reread is covered by the local TLS tests), and in-flight calls finishing on the old session.

For any future summary Function URL, Isaac/Jia Rui must use AWS_IAM and SigV4 requests,
without public grants or static AWS credentials. The Learner Lab account can't create IAM
roles, so the permissions are the shared `LabRole` taken from the node's metadata service,
not a scoped Pod Identity role; the report states this as a limitation.
No Lambda deployment/client or cloud acceptance evidence is supplied here.

## Interfaces and verification

Cindy integrates the raw-byte pair/version API and overlap lifecycle into Valkey/TTLs.
The consume-only scorer CLI uses `--key-version k1 --x25519-private PATH` and repeated
`--peer-public-key PEER PATH` options. Ingestor events pass through unchanged.
Zane owns durable case delivery; his #17 checks use the shared result validator. Jia Rui keeps
`/results` on `check_result`, owns enrichment, and consumes the updated case fields
`signing_key_version` and `truncated`. Cross-version case reconciliation is not
implemented. Isaac owns deployment/mounts and the live evidence above.

Banks, trusted public-key provisioning and the lab CA/operator are trusted. A compromised
bank exposes its own keys/records; the shared CA can impersonate any bank. Timing,
bank identities and token reuse remain visible. These are coursework limitations.

Run from the repository root:

```sh
uv sync --all-packages --locked
uv run ruff check .
uv run ruff format --check .
uv run pytest
```

Security tests cover the frozen HMAC vector, X25519/HKDF, overlapping versions,
versioned signing, shared result attacks, independent bank participation, schema privacy,
reload progress/rollback and real mTLS handshakes/per-call renewal. Test keys and
certificates are generated in temporary directories; no runtime secrets are committed.

API references: [X25519](https://cryptography.io/en/latest/hazmat/primitives/asymmetric/x25519/),
[HKDF](https://cryptography.io/en/latest/hazmat/primitives/key-derivation-functions/#hkdf),
[gRPC credentials/channels](https://grpc.github.io/grpc/python/grpc.html).
