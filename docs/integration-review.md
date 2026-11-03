# Integrated Role 4 review - 3 Nov 2026

## Overall result

**PASS WITH INTEGRATION ISSUES.** The reviewed code passes all 301 tests after two
rotation integration fixes. Deployment evidence and unfinished teammate integrations
remain separate from this code result.

Scope confirmed by Caleb: `.worktrees/role4-security-final`, now on local branch
`role4-integration-review`, branched from freshly fetched `origin/main`
`8654e8b697041d471125f93bf6531dc9879f0214` while preserving the uncommitted Role 4 changes.
The main IDE checkout still has its pre-existing merge conflicts in dataset/prepare.py
and security/pyproject.toml; it was not the review target and was not changed.
No commit, push or merge was performed.

## Exact final checks

| Check | Result |
| --- | --- |
| `uv sync --all-packages --locked` | PASS, 63 packages resolved |
| `uv run ruff check .` | PASS |
| `uv run ruff format --check .` | PASS |
| `uv run pytest -ra` | **301 passed, 0 failed, 0 skipped, 0 warnings** |
| Exact protobuf-header regeneration | All three outputs MATCH the canonical source checkout |
| `helm lint deploy/charts/bank` | 1 chart passed, 0 failed |
| `uv run python -m privacy_node.rotation_demo` | k1, overlap, old-reference backfill, k2 and retirement confirmed |
| Merge-marker / credential / obsolete-token scans | No active obsolete token path, conflict markers or actual secrets found in the reviewed worktree |

The final suite ran with Python 3.12.16, uv 0.13.7 and Helm 3.19.0 in an Ubuntu 24.04
filesystem held entirely in WSL `/run` RAM. Source files were copied with Git-compatible
Linux modes. This avoided Windows native-gRPC policy restrictions and Docker's disk errors.
No security behavior or dependencies were changed to make that environment pass.
The earlier implementation baseline had 298 passing tests; this review adds three tests.

## Role 4 integration status

| Area | Status | Evidence / limitation |
| --- | --- | --- |
| 16-byte HMAC | DONE | Frozen bytes vector, reverse pair, input/pair/key/version separation; no active `ppmc:` path |
| X25519/HKDF | DONE | Actual loaded files, pair symmetry, wrong peer, fresh version and local-only key ownership tests |
| Rotation | DONE (current implementation) | Dual index/backfill, version lookup, readiness, retirement, pending/in-flight work; incomplete-peer and signing-reload defects fixed |
| Ed25519 | DONE | Trusted `(bank, signing_key_version)` lookup; tamper, wrong bank/version and malformed signatures reject |
| Protobuf privacy | DONE | Bytes field, no absolute deadline, regenerated outputs, recursive descriptor approval and unknown-field rejection |
| Shared check_result | DONE | One function used by Deliver and case; all attacks, disconnected joins and false-confirmed cuts tested |
| Own-bank enrichment | PARTIAL | Producer/helper, receiver/store/report boundary and real interface test pass; automatic durable node-to-case delivery remains Zane-owned |
| mTLS | DONE (code and local live transport) | Actual TLS/Node handshakes, CA/hostname/client/bank binding and renewal pass; app deployment/mounts remain Isaac-owned |
| NetworkPolicy | PARTIAL | 46 rendered deployment/security tests pass; live CNI probes blocked by Docker storage failure |
| Lambda IAM | PARTIAL | No `NONE` URL/static keys found; AWS_IAM/Pod Identity contract reviewed, actual resources/caller absent |

The standalone scorer has the overlapping TokenIndex library interface. The current
integrated node consumes/scorers bank-local events; Cindy/Zane's future shared Valkey
handoff is still documented work, not a feature silently claimed complete here.

## Files fixed during this review

| File | Integration defect / change |
| --- | --- |
| `apps/scorer/src/scorer/token_index.py:39` | A k2 configuration could omit a previously configured peer with no retained records, bypass that peer's readiness and later stop valid ingestion. Overlapping versions now require identical configured pair coverage before any mutation |
| `apps/scorer/tests/test_score.py:711` | Reproduces the missing-peer transition with an empty index, proves k1 survives rejection, readiness still includes C, and B-C k2 ingestion works |
| `apps/privacy_node/src/privacy_node/node.py:140` | A concurrent reload could mix one signing-version label with another private key. The claim snapshots both together under the existing lock |
| `apps/privacy_node/tests/test_node.py:765` | Deterministic mid-encoding rotation proves the old snapshot and next new-version signature both verify |
| `apps/case/tests/test_scenarios.py:52` | Real scorer/index tokens match both peers, trace successfully, post through validated multipart case delivery, retain bytes, and enrich only Bank B |
| `docs/contracts/security.md` | Records identical peer coverage for overlapping versions |
| `docs/contracts/case.md` | Repairs 16 misdecoded Unicode characters without changing the design |
| `docs/evidence/role4-k3d-attempt.txt` | Records actual storage failure, authorized recovery and cleanup limitations |
| `docs/integration-review.md` | This review and handover |

No dependency or lockfile regeneration was needed for this review. Existing Role 4
implementation changes in the working tree remain uncommitted; the table above identifies
this review's changes, rather than attributing the entire earlier implementation anew.

## Remaining team dependencies

- **Cindy:** shared Valkey index/TTL implementation must preserve byte tokens, identical
  pair coverage, version-scoped lookup and overlap/backfill behavior.
- **Zane:** durable node-to-case outbox and shared-state/health/replica work. Use the one
  validator and own-case helper; retain the manifest/signing snapshot semantics.
- **Jia Rui:** actual summary function/caller and already agreed UI changes. Keep own-bank
  details internal. Cross-version case reconciliation is not currently implemented.
- **Isaac:** application Deployments, matching labels, Secret directory mounts, agreed
  replay/summary paths, enforcing-CNI evidence, and actual AWS_IAM/Pod Identity resources.
- **Caleb:** recheck live cluster/cloud acceptance after those integrations and a healthy
  environment are available; no remaining local Role 4 security-code failure was found.

## What actually ran live

- **Ran:** real localhost gRPC searches across three banks; actual TLS client/server
  handshakes and renewal; scorer-to-peer-to-case interface with own-bank details;
  live in-memory replay k1 -> dual indexing/backfill -> k2 -> retire k1, with no missed link.
- **Automated configuration only:** rendered Helm policies/certificates and AWS/static-key
  guards. These are not live CNI or deployed AWS authorization evidence.
- **Cluster attempt:** task k3d cluster and four pinned operators started; bank releases
  installed and operator labels matched. API/Docker failed during store readiness, before
  open/blocked probes or issued-certificate inspection. No live enforcement claim is made.
- **AWS:** no deployed Function URL or SigV4 caller exists here, so no cloud invocation
  test was possible. The required result remains AWS_IAM, authorized Pod Identity success,
  unsigned/wrong-role rejection.

The exact demo commands remain in `docs/contracts/security.md`. For live CNI evidence on
a healthy host, run `bash deploy/k3d-up.sh`, then `bash deploy/check-netpol.sh`; preserve
all authorized OPEN and unauthorized/cross-bank store BLOCKED results.

## Security findings

The two fixed rotation defects could interrupt legitimate ingestion or invalidate legitimate
signed claims during rotation. Existing fail-closed verification remained in place. No raw
cross-bank data leak, weaker result verifier, trusted-message public key, TLS-verification
bypass or committed credential was found. The only PEM-header scan match is an assertion
on a temporary generated key in a unit test; the frozen HMAC key is explicitly public/test-only.

Demo-only insecure channels remain in the explicitly documented localhost helper and
`--insecure-demo` path. The deployment CLI requires TLS files and normal CA/hostname
verification; none was disabled for this review.

Environment limitation: C: was found with **0 free bytes** during Docker recovery.
Task-only caches, archives and the generated Windows venv were removed to regain space.
Docker continued to report filesystem I/O errors until a clean backend restart. Final cleanup
is confirmed: task containers/network/volume/images removed, Mongo restored and verified running.
No user database, container definition, volume or Docker data file was deleted/reset. C: remains
low on free space; do not repeat the cluster attempt until more space is available. See the evidence file.

## Copy-paste handover

> Caleb's completed Role 4 worktree passes the integrated review with 301 tests passed,
> zero failures/skips/warnings, plus clean lint/format and matching regenerated protobuf.
> Two rotation integration bugs were fixed: incomplete k2 peer provisioning and a signing
> key/version reload race. Token bytes, X25519/HKDF, versioned Ed25519, shared result checks,
> schema privacy, own-bank case boundaries and real local mTLS are verified. It is safe to
> take these reviewed code changes forward without weakening any checks. Live NetworkPolicy
> enforcement, actual node-to-case durable delivery and AWS IAM deployment remain explicit
> teammate/environment dependencies; do not describe those as completed deployment evidence.
