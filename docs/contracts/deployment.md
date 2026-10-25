# Deployment contract

Owner: Isaac · Partners: every owner (per-service settings), Caleb (NetworkPolicy, Secrets, mTLS) · Code: `deploy/` · Status: **draft**

Scope: one-command k3d setup, the per-bank Helm chart, image builds, EKS, monitoring and the evaluation harness.

## Environments

| | Local | Cloud |
| --- | --- | --- |
| Cluster | k3d | Amazon EKS |
| Setup command | `deploy/k3d-up.sh` | TODO |
| Teardown | `k3d cluster delete csc3104` | TODO |
| Check | `deploy/check-netpol.sh` | TODO |
| Tested on | 4 CPU, 8 GB VM, Docker 29: setup about 3 min, about 3.8 GB memory in use with no bank services yet | |
| Cost controls | — | TODO: budget and shutdown routine |

## Platform components

| Component | Choice | Status |
| --- | --- | --- |
| Kafka | Strimzi 1.2.0, KRaft mode | k3d: one single-node cluster per bank, bootstrap `kafka-kafka-bootstrap:9092` (plaintext) |
| PostgreSQL | CloudNativePG 1.30.1 | k3d: one instance per bank, `postgres-rw:5432`, credentials in Secret `postgres-app` |
| Autoscaling | KEDA 2.21.0 | k3d: operator installed |
| Certificates | cert-manager v1.21.2 | k3d: lab CA `ClusterIssuer/bank-ca`; Secret `privacy-node-tls` per bank (**draft**: SAN `privacy-node.bank-<x>.svc`, with Zane and Caleb) |
| CNI enforcing NetworkPolicy | k3d: k3s's built-in network policy controller. EKS: TODO | k3d: enforced; `check-netpol.sh` passes |
| Registry | ECR or GHCR | TODO |
| Image architectures | Only the ones that are needed | TODO |
| CI | GitHub Actions | TODO |
| Monitoring | Prometheus / Grafana | TODO |
| Fault injection | Chaos Mesh, if it makes things simpler | TODO |

## Per-service settings

Each owner fills in their own row.

| Service | Owner | Image | Ports | Readiness / liveness | Resources | Config / Secrets |
| --- | --- | --- | --- | --- | --- | --- |
| Replay Job | Macarius | | | | | |
| Ingestor | Cindy | | | | | |
| Scorer | Cindy | | | | | |
| Privacy node | Zane | `privacy-node:dev` from `apps/privacy_node/Dockerfile` (build context: repo root) | gRPC 50051 (Service `privacy-node`) | TCP probe on 50051 (readiness 5 s, liveness 10 s) | requests 50m CPU / 128Mi; limit 256Mi | Secret `privacy-node-keys` (`<pair>.key` for its two pairs, `signing.pem`); ConfigMap `bank-public-keys` (`A.pem`, `B.pem`, `C.pem`); `emptyDir` `/data` for SQLite (M2). Enabled with `--set privacyNode.enabled=true`; setup in `deploy/privacy-node-up.sh` |
| Case service / UI | Jia Rui | `case-service:dev` from `apps/case/Dockerfile` (build context: repo root) | HTTP 8000 (`PORT`): UI, `/api/cases`, `POST /results`, `POST /notifications` | TODO (no Helm template yet) | TODO | `CASE_BANK`, `CASE_DB` (SQLite path), `CASE_PUBLIC_KEYS` (folder with `A.pem`…; can mount ConfigMap `bank-public-keys`), `CASE_INTAKE_URL`, `CASE_MIN_TRANSFERS`, `CASE_MIN_BANKS`, `CASE_NOTIFY_MAX_ATTEMPTS` |
| Report service | Jia Rui | None: template reports run inside the case service ([decisions/0001](../decisions/0001-template-only-report.md)) | | | | |
| Mock intake | Jia Rui | Same image, command `python -m apps.case.intake` | HTTP 8001 (`PORT`) | TODO | TODO | `INTAKE_DB` (SQLite path) |

## Namespaces and network paths

One namespace per bank: `bank-a`, `bank-b`, `bank-c`, each a release of the `deploy/charts/bank` Helm chart.

Current baseline (`bank-baseline` policy): deny all, then allow traffic within the bank's own namespace, DNS, the Kubernetes API (TCP 443/6443 to the chart's `apiServerCIDRs` only; `k3d-up.sh` pins the k3d node subnet to match, and EKS needs its own value), and ingress from the `strimzi`, `cnpg-system` and `keda` operators. k3s applies a new pod's egress policy a few seconds after the pod starts, so its first connections can bypass it. Strimzi's own generated policies are turned off so that they can't open Kafka to other namespaces. Cross-bank traffic is blocked.

The privacy node adds `privacy-node-peers` (in `templates/privacy-node.yaml`): ingress and egress between `app: privacy-node` pods in the other bank namespaces on TCP 50051 only. Needs review by Isaac and Caleb, and an enforcement check on k3d and EKS.

TODO with Caleb: narrow this to the per-service paths in [security.md](security.md#mtls-and-networkpolicy), and allow privacy node ↔ privacy node once the protocol port is known. Test enforcement in both environments.

## Open

- TODO
