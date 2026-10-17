# Security and privacy contract (Role 4)

Owner: Role 4. Reviewers requested: Roles 2 and 3 for tokens/signatures, Roles 3
and 5 for disclosure, Role 6 for deployment. **Review is pending; no teammate
approval is implied.** Implementation: `apps/security/security.py`.

## Status and decisions

This implements the Role 4 primitives for the coursework's simulated Banks A, B
and C. It follows the shared `payment_id` in [the data contract](data-contract.md).
Dataset preparation, replay and fixtures are unchanged. There is no peer schema,
transport service, case service or Kubernetes deployment in this repository yet.
The checks below are working locally; end-to-end privacy, mTLS and NetworkPolicy
enforcement still need integration evidence.

| Choice implemented by Role 4 | Rationale | Required reviewer / status |
|---|---|---|
| Exact payment ID; sorted, case-sensitive bank IDs | Both endpoints use the existing shared ID without lossy normalization | Roles 2 and 3 / pending |
| Independent HMAC-SHA256 key for AB, AC and BC | Only each pair shares a matching secret; no bank-wide key can match a peer's arbitrary key | Roles 2 and 3; Role 6 provisioning / pending |
| Domain-separated ASCII JSON input; `ppmc:v1` token | Unambiguous encoding and explicit protocol/key versions | Roles 2 and 3 / pending |
| Ed25519 over caller-supplied bytes using `cryptography` | Standard implementation independent of the unfinished protobuf schema | Role 3 / pending signed-field agreement |
| Raw binary HMAC files and PEM signing files mounted read-only | Simple Kubernetes Secret interface; no custom secret-management framework | Role 6 / pending mount and rollout agreement |
| Caller-supplied field allowlists plus forbidden-field checks | Review each real message without inventing Role 3's schema | Roles 3 and 5 / pending final policies |
| Separate claim-signing keys and TLS keys, tied to the same bank registry | Distinguishes transported messages from forwarded signed evidence | Roles 3 and 6 / pending certificate identity mapping |

Record reviewer names, review date and agreed changes here after the team reviews
these choices. Detection thresholds, final message fields, case identity, retries
and recovery remain with their existing role owners.

## Threat model and assumptions

This is a laboratory proof of concept, not a deployed banking system. Banks keep
their raw records locally and are assumed to apply the agreed scoring rule
honestly. Automated primitive tests cover modified claims, wrong signing keys,
forbidden payload fields and mismatched pair keys. Unauthenticated network peers
remain a planned integration test once mTLS exists. A participant
can still fabricate a claim about its own records or abuse an authorized lookup;
the primitives cannot establish that a bank is truthful.

Assume trusted key provisioning, a trusted certificate authority and protected
bank runtimes. Compromise of a bank exposes that bank's records, signing key and
its two pairwise keys. Namespace separation simulates separate banks; it does not
protect data from the cluster administrator. Rate limiting and signed audit logs
are deferred until the core protocol exists, as secondary project-plan work.

## Privacy boundary and field review

Customer identifiers, account numbers, raw amounts, raw `payment_id`, full payment
records and complete histories stay bank-local. The local token index maps a
token to this bank's own records; it must never be transmitted. Ground-truth
`chain_id`, `hop`, `scenario` and `altman_laundering` remain evaluation-only, as
specified by Role 1; do not load them into scoring or peer messages.

| Surface | Forbidden | Potentially permitted, subject to review |
|---|---|---|
| Peer query | All private fields above, local index, secrets | Pair-scoped token, authenticated bank identity, query/context IDs, versions and bounded protocol metadata |
| Peer claim / assembled result | Accounts, raw amounts, payment IDs, records, histories, evaluation labels, secrets | Token references, signer identity, signature and minimal local-rule assertion; exact assertion/disclosed links need agreement |
| Application logs / exported telemetry | Raw bank rows, accounts, customer IDs, amounts, payment IDs, keys, complete payload dumps | Reviewed request IDs, bank identity, status, timing and error category; avoid tokens unless explicitly needed and approved |
| Report requests / external AI input | Raw customer data, partner records, full histories, evaluation labels, secrets | Role 5's reviewed minimal case evidence; fields and destination need Roles 4/5 agreement |

These are disclosure categories, **not an approved protobuf schema**. Role 3 must
agree exact fields, types, nested paths and values separately for each message
type with Roles 4/5. Protocol hop accounting is distinct from Role 1's forbidden
ground-truth `hop` label; its field name and semantics belong to Role 3.

`check_peer_payload(payload, allowed_fields)` recursively checks every dictionary
key, including dictionaries inside lists, against the caller's reviewed field
names. Known forbidden names in `FORBIDDEN_FIELDS` are rejected even if passed in
the allowlist. Unknown names are rejected. It raises `ValueError` on a violation
and otherwise returns `None`. It does not redact or transmit anything.

This helper is only for plain dict/list representations. It is not a protobuf
validator, does not enforce types or context-specific nesting, and cannot detect
an account hidden in an allowed `query_id` or metadata value. The forbidden-name
set is a baseline, not a catalogue of every alias. Role 3's serializer adapter
must enforce the approved schema and values, cover protobuf maps/unknown fields,
and reject a failed check before sending. No live sending/logging path is wired
to this helper yet.

## Pairwise HMAC keys and provisioning

| Pair | Same secret held by | Not provided to |
|---|---|---|
| A-B | A and B | C |
| A-C | A and C | B |
| B-C | B and C | A |

Generate three **independent random 32-byte keys**, one per pair and active key
version, outside git (for example with `secrets.token_bytes(32)`). The API accepts
at least 32 bytes. Each endpoint must receive identical raw bytes and key version
for its pair. Arbitrary per-bank keys will not match. Do not derive these keys
from public bank names, payment IDs, passwords or claim-signing keys.

`load_pairwise_keys(bank_id, key_paths)` accepts an explicit mapping from a pair
tuple to a mounted path. It canonicalizes pair order, rejects pairs not involving
the bank, duplicate canonical pairs, short keys and reused bytes across supplied
pairs. It does not require every pair to be supplied, so a two-bank test can load
only A-B. It checks the supplied configuration, not actual cluster filesystem
isolation. It cannot audit keys omitted from the call or different versions;
Role 6 must verify complete independent provisioning and mount isolation.

Load each active version separately and retain the mapping of version to keys
locally. Pair/version metadata is public; the key material is secret. All
predictable keys in automated tests are test-only.

## Exact token contract, version 1

API:

```python
derive_token(payment_id, bank_a, bank_b, shared_key, key_version) -> str
```

1. Bank IDs are nonempty, distinct, exact strings sorted lexicographically by
   Python string ordering. For this workload use `A`, `B`, `C`. `A,B` and `B,A`
   produce the same pair. No trimming, case folding or bank-name aliasing occurs.
   Same-bank transfers do not need peer tokens and are rejected by this API.
2. Use `payment_id` exactly as supplied by Role 1, including the fixture's `p01`
   identifiers. Do not lowercase, trim, parse as a number or replace it with an
   account. The upstream data contract guarantees the shared identifier.
3. `key_version` is a nonempty ASCII label matching `[A-Za-z0-9_-]+`, e.g. `k1`.
4. The HMAC input is exactly the ASCII encoding of this JSON array, produced with
   `json.dumps(..., ensure_ascii=True, separators=(",", ":"))`:

   ```text
   ["csc3104.payment-token","v1",["A","B"],"k1","p01"]
   ```

   The domain is `csc3104.payment-token`; `v1` fixes this encoding. Unicode is
   JSON-escaped with no Unicode normalization. The array positions, quoting,
   escaping and lack of whitespace are part of the contract. The public helper
   `canonical_token_input` returns these exact bytes.
5. Compute `HMAC-SHA256(shared_key, canonical_input)` with standard-library
   `hmac`/`hashlib`, preserving all 32 digest bytes. Return:

   ```text
   ppmc:v1:<key_version>:<64 lowercase hexadecimal digest characters>
   ```

The bank pair is bound into the HMAC input; it is not repeated in the visible
prefix. Role 2 must scope lookups by `(canonical_pair, key_version, token)`.
Role 3 must carry or unambiguously establish the pair/version context; final
field names remain open. Do not compare tokens across pairs or versions.

Frozen interoperability vector (the repeated-byte key is **test-only**):

```text
key = b"A" * 32
input = ["csc3104.payment-token","v1",["A","B"],"k1","p01"]
token = ppmc:v1:k1:86ef4e3f811c077dcde10d9cef4d807c1b039afda3d794d72174c5d4a92cb444
```

Changing either payment ID, pair, key or key-version changes the input/key and
therefore the token (apart from negligible cryptographic collision probability).
Changing pair text is not a substitute for independent pair secrets.

`v1` is the protocol/encoding version; `k1` is a provisioning label. A new key
requires a new label at both endpoints. Changing encoding requires a new protocol
version and team review. For a coursework rotation, agree a pause/reindex/restart
with Roles 2/3/6, or explicitly agree retention of old versions for in-flight
queries. Do not silently fall back to a different key, compare versions as equal
or reuse a version label for different bytes. Rotation is not automated here.

## Ed25519 signing identity

Each bank has its own Ed25519 claim-signing key. Only that bank receives its
private key. Peers use a trusted configuration mapping bank ID and signing-key
version to an Ed25519 public key. Do not trust a key supplied by the claim being
verified. This registry and the key version must be agreed with Roles 3/6.

- `generate_signing_key()` delegates key generation to `cryptography`.
- `private_key_pem(key)` produces unencrypted PKCS8 PEM for a protected mount;
  `public_key_pem(key)` produces SubjectPublicKeyInfo PEM.
- `load_signing_key(path)` and `load_verification_key(path)` load those files and
  reject non-Ed25519 keys. An X.509 certificate is not a public-key PEM file.
- `sign(message_bytes, private_key)` signs exactly the supplied bytes and returns
  the library's 64-byte Ed25519 signature. There is no custom crypto/prehashing.
- `verify(message_bytes, signature, public_key)` returns `True` for a valid
  signature, `False` for an invalid/tampered/malformed signature. Role 3 must
  reject `False` without creating confirmed evidence. Key-loading errors surface
  normally and must not trigger an unsigned fallback.

Ed25519 signatures are deterministic for the same key and message. Role 3 owns
the versioned canonical bytes of a claim; it must agree a claim-specific domain,
origin signer, recipient/context binding, query/claim references, token and key
versions, and any expiry/freshness data before integration. Sign all agreed
security-relevant fields and verify identical bytes, excluding only the signature
itself. A canonical JSON token input does not define canonical claim protobufs.

A valid signature establishes integrity of exact bytes under the configured
bank's signing key. It does **not** prove truthful records, lawful activity,
laundering, freshness, replay prevention, authorization, chain completeness or
that a scoring rule was applied correctly. Freshness, retries, deadlines and
replay handling are Role 3's protocol responsibilities.

## Mounted-file integration for Role 6

Use binary pair keys and signing PEMs as read-only Kubernetes Secret volume
files. The helpers accept paths; they do not read environment variables or assume
Secret names. A service may obtain its configured paths from its own environment.
The following are illustrative mount paths, not deployment resource names:

```python
from apps.security.security import (
    derive_token,
    load_pairwise_keys,
    load_signing_key,
    load_verification_key,
)

# Run/import from the repository root, following the existing script workspace.
keys = load_pairwise_keys(
    "A",
    {
        ("A", "B"): "/run/secrets/pairs/ab-k1.key",
        ("A", "C"): "/run/secrets/pairs/ac-k1.key",
    },
)
token = derive_token(local_payment_id, "A", "B", keys["A", "B"], "k1")
signing_key = load_signing_key("/run/secrets/signing/a-s1.pem")
bank_b_public_key = load_verification_key("/run/trust/b-s1-public.pem")
```

Role 6 creates one random pair key centrally for this simulation, then supplies
copies only to its two banks in their respective namespaces. Bank A mounts AB/AC
and its own signing private key; B mounts AB/BC and B's private key; C mounts AC/BC
and C's private key. Public keys may be distributed to every verifier through
trusted configuration. HMAC keys and Ed25519/TLS keys have separate purposes.

Use `kubectl create secret generic ... --from-file=...` from local, uncommitted
files or Role 6's chosen provisioning mechanism; do not put raw or base64 secrets
in manifests, Helm values or git. Local `.secrets/` is ignored for this purpose.
The helper reads exact mounted bytes: no `.strip()` or base64 decoding. Kubernetes
Secret volume files already contain decoded data. Base64 in an API Secret is an
encoding, not encryption. Restrict Secret access and mounts to the intended bank
service. Secret names, paths, permissions and distribution commands need Role 6
agreement; there is no complete deployment manifest in this change.

Helpers load once per call, not through a watcher. Role 6 must coordinate a
reload/restart on rotation and Role 2 must rebuild affected indexes. A Secret
mounted with `subPath` does not receive automatic updates; do not assume replacing
a Secret changes keys already loaded into a running process.

## mTLS identity and current blockers

Every peer service must have an authenticated certificate identity under the
team's trusted laboratory CA (cert-manager is a project-plan option). Both ends
validate the certificate chain and validity. Clients verify the server SAN
against the expected service identity; servers require a valid client
certificate and authorize its SAN through an explicit certificate-to-bank map.
Do not accept a bank merely because it has any CA-signed certificate, or trust a
client-supplied bank field in place of certificate authentication. Do not disable
hostname checks. Exact DNS/SAN names remain for Roles 3/6 to agree.

Map the authorized transport identity to the sender bank in the message envelope
and use the trusted bank-to-signing-key registry for claims. Reject sender
mismatches. For a directly authored claim the sender and signer must agree. For
an unchanged forwarded claim the authenticated immediate sender can differ from
the original signer; verify the origin's signature and separately authenticate
the forwarding envelope. Role 3 must define whether/how forwarding is permitted.
TLS key material is separate from the Ed25519 claim-signing keys.

No transport service or deployment exists to attach mTLS to safely. Integration
steps and blockers are:

| Step / blocker | Owner | Acceptance evidence |
|---|---|---|
| Add actual peer client/listener and transport configuration hooks | Role 3 | Real two-bank request over the selected transport |
| Agree CA, SAN-to-bank map, trust bundle and TLS Secret mounts | Roles 4/6 with 3 | Both peers authenticate as the configured bank |
| Configure mutual verification and authorize certificate identity before dispatch | Roles 3/4 | Missing, untrusted, expired or wrongly named certificates rejected; sender mismatch rejected |
| Tie signer/version lookup to trusted public keys and define forwarding | Roles 3/4 | Wrong signer rejected; permitted forwarded original claim still verifies |
| Deploy and repeat in k3d/EKS | Role 6 with 4 | Authenticated peer succeeds; authentication failures create no confirmed evidence |

This is preparation, not a working mTLS demonstration. No new CA, server, gRPC
schema or deployment stack is invented to stand in for the missing services.

## NetworkPolicy intent for Role 6

Use default-deny ingress **and** egress in each bank namespace, then add only
needed paths. Final selectors and ports follow actual Role 6 deployments.

| Source | Destination | Intended allowance |
|---|---|---|
| Approved bank peer service | Other approved bank's peer service | Only the agreed protocol TCP port, with mTLS |
| Bank-local ingestor/scorer | Same bank's Kafka | Only required broker ports; no cross-bank Kafka |
| Authorized bank-local scorer/peer/case service | Same bank's database | Only required database port; no cross-bank database |
| Bank workloads requiring name resolution | Cluster DNS | Only required TCP/UDP DNS traffic |
| Agreed monitoring/operational services | Explicit selected endpoints | Only reviewed health/metrics paths and ports |
| Unrelated namespaces/services | Bank services | Denied unless separately reviewed |

Peer ingress should match both the approved namespace and the peer pod selector
in the same rule; the peer namespace alone is too broad. Permit corresponding
source egress and destination ingress. Policies are additive: a broad allow in
another policy can defeat intent. NetworkPolicy requires an enforcing CNI and
does not itself authenticate TLS, verify claims or inspect private payloads.
Role 6 owns final policies and must demonstrate enforcement in both k3d and EKS.
Test allowed peer connectivity, denied unrelated-namespace access, and denied
cross-bank database/Kafka access from a peer pod and other bank-local pods.

## Handoffs and required agreements

| Owner | Required interface / decision |
|---|---|
| Role 2 | Read only its own bank events; skip intra-bank peer tokenization; call `derive_token` with shared `payment_id`, counterparty pair and active key version; keep token-to-local-record index private. Agree call points, storage and reindex/retention on rotation. |
| Role 3 | Agree final schemas, per-message field/path allowlists and canonical signed bytes; map authenticated peer identity and trusted signer key; reject failed signatures/policies; own query IDs, replay/deadline/hop state, forwarding and partial results. |
| Role 5 | Agree case/result/report disclosures with Roles 3/4, including assembled-chain detail and AI destinations. Own case identity and durable alert deduplication. |
| Role 6 | Agree Secret mounts, pair-key provisioning, signing registry, CA/SAN map, enforcing CNI, namespaces/ports and coordinated rotation. Run cluster authentication and isolation tests with Role 4. |

## Residual disclosure and linkability

Tokens are pseudonymous, not anonymous or encrypted payment records. A stable
pair/version token reveals repeated references to the same payment within that
pair/version. Pair, bank identity, request frequency, timing, query context and
claim outcomes reveal metadata. A bank possessing a pair key can compute guesses
for candidate payment IDs; this matters especially for tiny predictable fixture
IDs. Independent pairs prevent simple global token equality, not collusion.

Intermediate banks necessarily connect their own incoming/outgoing records.
Assembled claims can reveal participating banks, order and inferred links, even
without accounts or amounts. A bank seeing both ends or colluding with peers may
link more. Signatures and mTLS do not conceal these facts or prevent a malicious
authorized peer from lying. Report the actual assembled-chain fields and these
limits with the integration evidence; do not claim zero disclosure.

## Automated tests and later evidence

From the repository root, with `uv` available:

```sh
uv sync --all-packages --locked
uv run pytest apps/security/test_security.py
uv run pytest
uv run ruff check .
uv run ruff format --check .
```

### Temporary directories on Windows and in the sandbox

The root `conftest.py` gives each pytest run a fresh temporary directory under
ignored `.cache/` and removes it after pytest finishes. This avoids Windows
ownership conflicts when the normal terminal and the sandbox use different
accounts. The root pytest configuration also applies when running just the
security tests or starting inside `apps/security/`.

Pytest's optional persistent cache is disabled so runs do not share a
permissions-restricted `.pytest_cache`. All tests still run; cache-based features
such as `--last-failed` are unavailable. This follows pytest's supported
[plugin-disable option](https://docs.pytest.org/en/stable/how-to/usage.html#disabling-plugins).

No per-terminal temporary-directory environment variable is needed. The existing
system-temp folder is left untouched. An explicitly supplied `--basetemp` still
takes precedence; use only a dedicated test directory because pytest clears that
path. See [pytest temporary-directory behavior](https://docs.pytest.org/en/stable/how-to/tmp_path.html#temporary-directory-location-and-retention).

If using this workspace's local uv executable in PowerShell, run:

```powershell
& .\.cache\tools\uv.exe run pytest apps/security/test_security.py -v
```

Tests independently read `bank_A.csv` and `bank_B.csv`, assert the five shared
IDs `p01`, `p06`, `p08`, `p10`, `p13`, and compare tokens for every shared ID.
Further tests cover the frozen encoding/vector, changed IDs/keys/pairs/versions,
reversed bank order, isolated bank-scoped mounts, reused/short keys, PEM round
trips, non-Ed25519 rejection, deterministic signing, tampered content/signatures,
wrong signers, malformed signatures and nested field allowlists.

Once real serializers/logging/report paths exist, Roles 3/5/6 should supply this
additional privacy test evidence with Role 4:

1. Give each bank distinct sentinel customer IDs, account numbers, raw payment
   IDs and amount representations; also run the committed tiny fixture. Capture
   outbound query, claim, result and report inputs plus application logs for
   successful, invalid, partial and timed-out searches.
2. Decode each outbound protobuf through its actual schema and check its approved
   fields, nesting and values. Pass its plain representation through
   `check_peer_payload` using that message's reviewed allowlist. Check serialized
   bytes and logs for the sentinel values and fixture account/payment IDs, not
   just suspicious field names. Inspect amounts as decoded numeric values too;
   byte substring checks alone miss numeric protobuf/JSON encodings.
3. Inject a forbidden field at top level and inside a repeated/nested claim, and
   put a private sentinel into an otherwise allowed metadata value. Confirm the
   real sender/report/log path rejects disclosure. The generic field-name helper
   alone cannot pass the latter test; add schema/value rules in the owner service.
4. Confirm malformed/tampered/wrongly signed claims and certificate-identity
   failures never become confirmed evidence. Verify real Secret mount isolation
   and NetworkPolicy denial using unauthorized workloads.
5. Save the software revision, reviewed field policy, redacted decoded examples,
   test commands and pass/fail output. Do not save private keys or real records.

Passing primitive tests is not evidence that an unimplemented network or logging
path preserves privacy. Complete the above before claiming end-to-end security.

## Primary references

- [Python 3.12 HMAC](https://docs.python.org/3.12/library/hmac.html)
- [cryptography Ed25519](https://cryptography.io/en/stable/hazmat/primitives/asymmetric/ed25519/)
- [cryptography key serialization](https://cryptography.io/en/stable/hazmat/primitives/asymmetric/serialization/)
- [uv workspace environment and lockfile](https://docs.astral.sh/uv/concepts/projects/workspaces/)
- [Kubernetes Secrets](https://kubernetes.io/docs/concepts/configuration/secret/)
- [Kubernetes NetworkPolicy](https://kubernetes.io/docs/concepts/services-networking/network-policies/)
- [Python certificate verification](https://docs.python.org/3.12/library/ssl.html)
- [RFC 9525 service identity verification](https://www.rfc-editor.org/rfc/rfc9525.html)
