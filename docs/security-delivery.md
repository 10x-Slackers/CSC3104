# Role 4 delivery report - 3 Nov 2026

Subsequent integration review: [301 passing tests and two rotation fixes](integration-review.md).
The current local review branch is `role4-integration-review`; the original report below records the implementation delivery.

Work is uncommitted on `role4-security-final`, created from fetched `origin/main`
`8654e8b697041d471125f93bf6531dc9879f0214`, in `.worktrees/role4-security-final`.
The original `calebwee` checkout's staged changes and unfinished merge were preserved.
Nothing was committed, pushed or merged.

## Files changed

Every changed/created file is listed below. Paths are relative to this worktree.

| File | Purpose |
| --- | --- |
| `.github/workflows/checks.yml` | Run locked sync, lint, format, all tests (including schema/Helm), and protobuf drift check in CI |
| `apps/security/src/security/__init__.py` | 16-byte HMAC; X25519/HKDF; purpose-specific PEM and versioned signing loaders |
| `apps/security/tests/test_security.py` | Frozen vector, canonicalization, key agreement and signature/privacy properties |
| `apps/scorer/src/scorer/token_index.py` | Raw-byte/version-scoped indexing, fresh overlapping keys, backfill, readiness and retirement |
| `apps/scorer/src/scorer/stream.py` | Replace pair-secret CLI arguments with local X25519 private/trusted peer public paths |
| `apps/scorer/tests/test_score.py` | Byte tokens, overlap/version isolation, old-reference backfill and atomic capacity failure |
| `apps/privacy_node/src/privacy_node/keys.py` | Generate only one bank's local material; load versioned trusted public artifacts |
| `apps/privacy_node/src/privacy_node/claims.py` | One authoritative result validator; signed framing; trusted versions; unknown fields; reject disconnected joins |
| `apps/privacy_node/src/privacy_node/node.py` | Shared validation, relative budget, mTLS, version-preserving queries and mounted-key reload |
| `apps/privacy_node/src/privacy_node/local_state.py` | Version-specific refs and internal own-bank snapshots |
| `apps/privacy_node/src/privacy_node/privacy_node.proto` | Bytes token, reserve removed deadline, independent signing version and signed truncation |
| `apps/privacy_node/src/privacy_node/privacy_node_pb2.py` | Regenerated schema implementation |
| `apps/privacy_node/src/privacy_node/privacy_node_pb2.pyi` | Regenerated schema type declarations |
| `apps/privacy_node/src/privacy_node/demo.py` | Lab-only independent bank key generation and explicit public exchange |
| `apps/privacy_node/src/privacy_node/rotation_demo.py` | Runnable k1/overlap/backfill/k2/retirement demonstration |
| `apps/privacy_node/src/privacy_node/transport.py` | Trusted URI-SAN identity and reloadable mTLS credentials |
| `apps/privacy_node/src/privacy_node/case_delivery.py` | Internal-only own-bank enrichment producer helper |
| `apps/privacy_node/tests/test_node.py` | Migrate wire/key contracts; relative timeout and end-to-end rotation/retry tests |
| `apps/privacy_node/tests/test_claims.py` | Recursive schema approval, shared fixture, validator attacks, cut and disconnected-chain regressions |
| `apps/privacy_node/tests/test_keys.py` | Local ownership/public-only distribution, signing versions and reload |
| `apps/privacy_node/tests/test_transport.py` | Eight real TLS handshake/reload/Node identity tests |
| `apps/privacy_node/tests/test_deployment_security.py` | Actual rendered policy/certificate assertions and future insecure Lambda/static-key guards |
| `apps/case/src/case/evidence.py` | Re-export the authoritative validator and re-read versioned trusted files |
| `apps/case/src/case/app.py` | Store normalized results; internal enrichment; human-only token display; refreshed verification keys |
| `apps/case/src/case/case_store.py` | BLOB tokens/version-scoped transfers; own-bank snapshots; preserve signing/truncation metadata |
| `apps/case/src/case/enrichment.py` | Bind raw internal details to the receiving bank's signed refs |
| `apps/case/src/case/report.py` | Explicit own-bank report fields; minimal untyped boundary; human-only hex display |
| `apps/case/tests/test_evidence.py` | Shared validator exceptions, bytes and versioned public-key layout |
| `apps/case/tests/test_scenarios.py` | Use authoritative validator on real fixture outcomes |
| `apps/case/tests/test_security_boundary.py` | HTTP attack rejection, cut demotion, enrichment/report isolation, BLOB/bool roundtrip |
| `deploy/charts/bank/templates/certificate.yaml` | DNS plus bank URI SAN, both EKUs, new TLS private key on renewal |
| `deploy/charts/bank/templates/networkpolicy.yaml` | Default deny, selected real local/operator paths and privacy-node-only cross-bank path |
| `deploy/check-netpol.sh` | Open/blocked probes with authorized positive controls; no Service endpoint pollution |
| `docs/contracts/security.md` | Final implemented design, APIs, vector, rotation, review limits and reproducible evidence |
| `docs/contracts/protocol.md` | Exact new schema/signature/timeout/validation/reload contracts |
| `docs/contracts/streaming.md` | Token/key CLI, overlap lifecycle and retained-reference backfill |
| `docs/contracts/case.md` | Shared validation, internal multipart sample, own-bank report boundary and byte storage |
| `docs/contracts/deployment.md` | Isaac review: selectors, mounts, mTLS, exact probe/IAM followups |
| `docs/contracts/samples/trace_request.textproto` | One frozen raw-token example shared by producer and consumer tests |
| `pyproject.toml` | Declare PyYAML directly in the root development group for Helm tests |
| `uv.lock` | Updated only through uv; existing PyYAML version retained |
| `docs/evidence/role4-k3d-attempt.txt` | Actual isolated cluster attempt, API/engine failure and cleanup evidence |
| `docs/integration-review.md` | Subsequent independent integration review, fixes and 301-test result |
| `docs/security-delivery.md` | This inventory, status, handoffs, evidence and demonstrations |

All three protobuf outputs were regenerated using the exact header command; the gRPC
stub's generated contents are unchanged, so `privacy_node_pb2_grpc.py` has no diff.

## Requirements status

| Requirement | Status | Result / remaining dependency |
| --- | --- | --- |
| A - 16-byte HMAC | DONE | Leftmost 16 raw bytes, no prefix, unchanged input, vector, reverse-pair and input/key/version separation |
| B - X25519/HKDF | DONE | Per-bank/per-version fresh keys, canonical HKDF context, local-only generation/public provisioning; no transported pair secret |
| C - Rotation (1-9) | DONE | Dual index plus retained-reference backfill, trusted readiness gate, k2 switch, old in-flight and pending work, explicit retirement, versioned signing and Secret reload |
| D - Protobuf/schema | DONE | Bytes, relative timeout, reserved old field, recursive exact descriptor allowlist in CI, unknown-field rejection, shared example |
| E - Shared check_result (1-6) | DONE | All delivered-state/involvement/digest/signature/ownership/linkage/anchor/binding checks; signed hop/branch truncation and open cuts normalize to PARTIAL; both consumers reject attacks |
| F - Ed25519 versions | DONE | Explicit signing_key_version separate from token version; trusted tuple lookup; old/new coexist and unavailable/wrong versions reject |
| G - mTLS | DONE (implementation/local evidence) | Real trusted/untrusted/missing-cert/URI/sender/hostname/reload tests; actual app Deployment/mounts remain Isaac integration |
| H - NetworkPolicy | PARTIAL | Least-privilege patch, actual Helm assertions and probe script complete; k3d setup attempted; API/Docker engine failed before live probes; EKS unavailable |
| I - Enriched cases | DONE (security interface) | Own-bank producer/receiver/store/report boundary tested; other-bank raw details reject; durable node-to-case outbox remains Zane work |
| J - Lambda IAM | PARTIAL | Actual repo reviewed, insecure-config guard and exact handoff complete; no Lambda deployment or caller exists to prove live AWS_IAM/Pod Identity |
| K - security.md (1-20) | DONE | All requested design, limitations, reproducible procedures and attributed integration items documented |
| L - Test evidence | PARTIAL | All implemented automated properties pass; live NetworkPolicy/AWS evidence awaits external integrations |
| M - Generation/dependencies | DONE | Exact protoc regeneration reproduced, no manual generated edits, uv-only dev dependency/lock update |
| N - Full checks | DONE | Locked sync/lint/format/pytest passed in Linux; see exact totals below |

Working rules 1-12 were followed: relevant contracts/code and official API docs were read,
latest main fetched and isolated, changes kept within requested security interfaces,
no real keys/credentials committed, and no commit/push/merge. No unrelated role redesign
(PostgreSQL/Valkey migration, alert tiers, UI removal, retry redesign or AWS infrastructure)
was attempted. The internal multipart encoding and explicit signing/truncation fields are
small concrete interfaces implementing the already requested design.

## Copy-paste teammate handoffs

### Cindy - token/key API

> `derive_token(payment_id: str, bank_a: str, bank_b: str, shared_key: bytes, key_version: str) -> bytes`
> returns HMAC-SHA256(...).digest()[:16], with the existing canonical ASCII JSON and no textual prefix.
> `p01/A-B/k1`, test key `b"A"*32`, gives hex `86ef4e3f811c077dcde10d9cef4d807c` (16 raw bytes).
> Derive pair keys using `load_pairwise_keys(bank, version, local_X25519_private_path, {peer: trusted_public_path})`.
> Index keys are `(canonical_pair, token_key_version, raw_bytes)`. `AgreementFiles` configures TokenIndex.
> Introduce fresh k2 at both banks; retained refs are backfilled, new events index both versions, readiness gates query switch,
> and k1 retires only after old work expires. Carry these exact semantics into your Valkey index/TTL implementation.
> Full layout and API: `docs/contracts/security.md`.

### Zane - protobuf, result validation, signing and mTLS

> Use regenerated protobuf: TokenRef.token is bytes16; TraceRequest field7 is reserved (no absolute deadline).
> Forward `context.time_remaining()` as relative gRPC timeout. LinkClaim.key_version is token version;
> signing_key_version selects trusted `(bank, signing_version)` Ed25519 keys. `truncated` is signed and marks local hop/branch cuts.
> `check_result(result, public_keys, bank)` returns a verified normalized copy or raises ValueError. Store its return value.
> Both Deliver and case already use it. It checks anchor, signatures/ownership/nonempty chain joins, digest and query/recipient binding;
> incomplete or signed-truncated results become PARTIAL. Unknown fields reject recursively; schema additions require allowlist approval.
> mTLS maps URI `spiffe://csc3104/bank/A/privacy-node` to sender before cache/store access; use verified DNS peer targets.
> `--key-directory ROOT` reloads version manifests/PEMs; TLS directories reload on new connections.
> Wire `own_case_legs(result, local)` ONLY into your own bank's durable case outbox, using the multipart interface in case.md.
> Shared producer/consumer sample: `docs/contracts/samples/trace_request.textproto`.

### Isaac - NetworkPolicy, certificates and Lambda

> Please review the prepared deploy/ patch. Default deny plus matching source/destination app selectors replaces namespace-wide local access.
> Cross-bank access is ONLY privacy-node -> privacy-node50051 with namespace AND pod selector. Kafka/Postgres/Valkey/case stay bank-local.
> Current node->Kafka remains because the node still consumes Kafka; remove it after the Valkey handoff. Add runtime labels/mounts/app Deployments,
> agreed replay/ingestor/summary ports and EKS credential paths. Verify operator labels and narrow API-server CIDRs.
> Certificates include server/client EKUs, DNS SANs and bank URI SAN; mount Secret directories without subPath and pass TLS CLI paths.
> Run `bash deploy/check-netpol.sh | tee netpol-evidence.txt` under enforcing k3d/EKS; rendered config assertions already pass.
> No Lambda deployment exists. Create summary Function URL with AWS_IAM, scoped case Pod Identity permissions for InvokeFunctionUrl AND InvokeFunction,
> no public grants/static keys; prove unsigned/wrong-role403 and authorized case success. Network paths must include only the actual summary/identity endpoints.
> Details and exact AWS query: deployment.md/security.md. Shared lab CA can impersonate any bank; this limitation is documented.

### Jia Rui - shared verification and own-bank case/report privacy

> case.evidence re-exports the ONE `privacy_node.claims.check_result`; no duplicate verify_result remains.
> Store the returned normalized copy. Invalid signatures/versions/anchor/bindings/digest/disconnected joins reject; false CONFIRMED cuts store only PARTIAL.
> Trusted public mounts use `<CASE_PUBLIC_KEYS>/<bank>/<signing_version>.pem`, re-read per result; retain old/new public keys during overlap.
> Case tokens are raw BLOBs; hex only in analyst JSON/HTML/template input. Signing/truncation metadata is retained.
> Internal `/results` accepts binary result or multipart binary `result` plus `own_legs` JSON, each `{bank,claim_index,direction,payment_id,account,amount}`.
> Only the receiving bank's signed nonempty refs can be enriched. Never request/attach another bank's raw leg.
> `report_input(case, bank)` permits approved own-bank details; foreign raw details reject. Future summary stays within this bank and uses IAM/SigV4 Pod Identity.
> Durable node delivery, summary caller/function and read-only UI changes remain your/Zane's existing work.
> Token-version changes alter digests; cross-version case reconciliation is not implemented. Fresh demo databases are required for the new schema.

## Test evidence

Final Linux acceptance result: **298 passed, 0 failed, 0 skipped**.

The current source was copied read-only into an isolated Linux container, with Git file
modes restored (Windows bind mounts otherwise make every Python file executable).
No source behavior was changed for the Linux test copy. Official uv image:
`ghcr.io/astral-sh/uv:0.13.7-python3.12-trixie-slim`, Python3.12.16; Helm3.19.0 installed
only in temporary tooling. Commands ran from `/tmp/check-workspace`, its repository root:

```sh
uv sync --all-packages --locked
uv run ruff check .
uv run ruff format --check .
uv run pytest
helm lint deploy/charts/bank
uv run python -m privacy_node.rotation_demo
uv run python -m grpc_tools.protoc -Iapps/privacy_node/src --python_out=apps/privacy_node/src --pyi_out=apps/privacy_node/src --grpc_python_out=apps/privacy_node/src apps/privacy_node/src/privacy_node/privacy_node.proto
```

- Sync: 63 locked packages resolved; no package versions changed by the PyYAML declaration.
- Ruff: check passed; format passed (59 files).
- Helm lint: 1 chart passed, 0 failed; all 46 deployment security tests executed (none skipped).
- TLS: 8 real handshake/Node tests passed, including no client cert, wrong/untrusted identity, hostname and renewal.
- Protoc: all three generated outputs match the canonical Linux checkout after exact regeneration.
- Shell probe syntax and `git diff --check`: passed. Live probes were **not** executed: the isolated cluster API and Docker engine failed during bank-store startup. See [the attempt record](evidence/role4-k3d-attempt.txt).
- Rotation demo: k1 replay, dual 16-byte lookups, pre-rotation backfill, k2 same link, and retirement all CONFIRMED.

Security scenarios include the frozen bytes vector/canonical reverse pair, X25519 symmetry,
wrong-key/version separation, fresh rotation/retirement/readiness, old pending and in-flight
searches, old/new signing coexistence, every shared-result attack, unknown nested wire fields,
recursive descriptor privacy, own-bank-only enrichment/reporting, and selected open/blocked
rendered network paths. Regression tests additionally reject signed disconnected fragments
and preserve both true/false truncation flags across database reload.

Windows locked sync and token/scorer/case boundary tests also ran. The native gRPC DLL
was blocked by Windows Application Control (even escalated), so full acceptance was run
in Linux without changing/disabling that policy. Early failing integration runs found
issues that were fixed; the final result above supersedes those development runs.
GitHub CI is configured but has not run remotely because nothing was pushed.

A live k3d attempt followed the passing automated suite. The task-only cluster and all four
pinned operators started, all bank releases installed, and operator labels matched the policies.
During bank-store readiness the Kubernetes API timed out and Docker Desktop returned HTTP 500.
No CNI probes or issued-certificate inspection could run. The cause was not verified; resource
pressure is possible. The user authorized a Docker Desktop restart for task-cluster cleanup.
See [the actual command/outcome record](evidence/role4-k3d-attempt.txt) for recovery and cleanup status.
No unrelated container definitions or data were removed.

## Exact demonstration procedure

Run from the isolated worktree root with uv and Helm available:

```sh
uv sync --all-packages --locked
# 16-byte vector, reverse pair, all changed inputs
uv run pytest apps/security/tests/test_security.py -k token -v
# k1 replay -> fresh k2 -> dual index/backfill -> ready-query switch -> retire k1; no missed link
uv run python -m privacy_node.rotation_demo
# tamper/bad signatures/binding/signing versions; cut CONFIRMED becomes PARTIAL
uv run pytest apps/privacy_node/tests/test_claims.py -v
uv run pytest apps/case/tests/test_security_boundary.py -v
# actual mTLS handshakes: accepted trusted A; reject missing cert/wrong URI/wrong bank/sender/CA/hostname
uv run pytest apps/privacy_node/tests/test_transport.py -v
# rendered policy assertions (does not replace actual CNI probes)
uv run pytest apps/privacy_node/tests/test_deployment_security.py -v
```

For the actual cluster: Isaac deploys the reviewed labels/certificates/Secret directory
mounts and peer DNS targets; bank operators begin with k1 then publish fresh k2 publics and
`active.json` with both active/query k1, wait for both endpoints ready, change query k2,
let old work drain, then remove k1 from active_versions. See exact manifest and key tree in
security.md. For network evidence:

```sh
bash deploy/k3d-up.sh
bash deploy/check-netpol.sh | tee netpol-evidence.txt
```

Expect every PASS: peer 50051 OPEN, required local Kafka/Postgres/Valkey/case OPEN; cross-bank
Kafka/Postgres, unauthorized namespace and wrong local app BLOCKED. Temporary listeners test
the app paths before app deployments exist; real store probes require the cluster stores.
AWS IAM verification waits for the actual function/client: the documented get-function-url-config
query must return AWS_IAM; unsigned/wrong role requests fail and authorized Pod Identity succeeds.

## Remaining team dependencies and diff review

- Caleb: implementation and local automated security evidence complete; collect/review live
  cluster/cloud acceptance evidence once teammates provide the integrated environment.
- Cindy: adopt versioned raw-byte/backfill/overlap behavior in shared Valkey and agreed TTLs.
- Zane: persistent shared state and durable internal case outbox using the supplied boundary;
  existing unrelated retry/health/replica work remains his.
- Isaac: approve deployment patch, mount/workload/identity integration, real CNI probes and AWS IAM resources.
- Jia Rui: actual summary function/client, approved UI changes and any cross-version case reconciliation.

No unrelated changes were found in this isolated diff. Its source changes are security
contracts/interfaces and required consumer updates; deployment changes are explicitly for
Isaac review. The original checkout's unrelated staged/merge changes are not included.
All three generated files were regenerated, never manually edited. The lockfile was updated
through uv. Changed files were reviewed for PEM private blocks, static AWS keys and generated
credentials; none were found. The intentionally public frozen test key/vector is documented;
all runtime signing/agreement/TLS test material is generated only in temporary folders.
