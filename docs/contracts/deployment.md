# Deployment contract

Owner: Isaac · Partners: every owner (per-service settings), Caleb (NetworkPolicy, Secrets, mTLS) · Code: `deploy/` · Status: **draft**

> [!WARNING]
> **Outdated (30 Oct 2026).** Under review after a full codebase walkthrough; don't build against this doc until it's updated. Expected to change:
> - **Platform components**: adds Valkey; one bank runs with replicas (Kafka, PostgreSQL, Valkey, privacy node) on k3d, the others single-instance; all banks HA on EKS
> - **Per-service settings**: the service list (ingestor, report service and mock intake change)
> - **Namespaces and network paths**: privacy node, replay and analyst UI paths

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
| Privacy node | Zane | | | | | |
| Case service / UI | Jia Rui | | | | | |
| Report service | Jia Rui | | | | | |
| Mock intake | Jia Rui | | | | | |

## Namespaces and network paths

One namespace per bank: `bank-a`, `bank-b`, `bank-c`, each a release of the `deploy/charts/bank` Helm chart.

Current baseline (`bank-baseline` policy): deny all, then allow traffic within the bank's own namespace, DNS, the Kubernetes API (TCP 443/6443 to the chart's `apiServerCIDRs` only; `k3d-up.sh` pins the k3d node subnet to match, and EKS needs its own value), and ingress from the `strimzi`, `cnpg-system` and `keda` operators. k3s applies a new pod's egress policy a few seconds after the pod starts, so its first connections can bypass it. Strimzi's own generated policies are turned off so that they can't open Kafka to other namespaces. Cross-bank traffic is blocked.

TODO with Caleb: narrow this to the per-service paths in [security.md](security.md#mtls-and-networkpolicy), and allow privacy node ↔ privacy node once the protocol port is known. Test enforcement in both environments.

## Open

- TODO
