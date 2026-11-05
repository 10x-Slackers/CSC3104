# Deployment contract

Owner: Isaac · Partners: every owner (per-service settings), Caleb (NetworkPolicy, Secrets, mTLS) · Code: `deploy/` · Status: **draft**, needs review by Caleb

> [!NOTE]
> **Pending work (decided 30 Oct 2026).** The sections below describe what exists today; these items change it:
> - **Topic:** a declared `KafkaTopic` per bank (6 partitions, key = account, `retention.ms -1`, auto-create off).
> - **HA flag:** one values flag runs a bank with replicas (Kafka 3 with `min.insync.replicas` 2, PostgreSQL 3, privacy node 2). One bank uses it on k3d, which is a one-node PoC, so failover there is pod-level only; every bank uses it on EKS.
> - **New components:** Valkey per bank; KEDA `ScaledObject` for the scorer; the ingestor and summary service as containers locally and Lambda function URLs (`AWS_IAM`) on EKS; replay as a Job in a `replay` namespace.
> - **Hardening:** per-path and per-store NetworkPolicies; `automountServiceAccountToken: false`; requests, memory limits and readiness probes; PSS `warn: restricted`; k3s secrets encryption; one Dockerfile pattern.
> - **EKS:** scripted cluster, VPC CNI network policy, Pod Identity, ElastiCache, a Kafka listener for the ingestor Lambda.

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
| Kafka | Strimzi 1.2.0, KRaft mode | k3d: one single-node cluster per bank, bootstrap `kafka-kafka-bootstrap:9092` (plaintext). **pending work:** declared topic; 3 brokers under the HA flag |
| PostgreSQL | CloudNativePG 1.30.1 | k3d: one instance per bank, `postgres-rw:5432`, credentials in Secret `postgres-app`. **pending work:** `instances: 3` under the HA flag; separate databases for the privacy node and the case service |
| Valkey | One instance per bank (ElastiCache on EKS) | **pending work:** Derived state, rebuilt from Kafka; only the scorer and privacy node may connect |
| Autoscaling | KEDA 2.21.0 | k3d: operator installed. **pending work:** `ScaledObject` on consumer lag, `maxReplicaCount` 6, `minReplicaCount` 1, `offsetResetPolicy: earliest` |
| Serverless | AWS Lambda function URLs, `AuthType: AWS_IAM` | **pending work:** Ingestor and summary service; plain containers in the namespace on k3d |
| Certificates | cert-manager v1.21.2 | k3d: lab CA `ClusterIssuer/bank-ca`; Secret `privacy-node-tls` per bank (**draft**: SAN `privacy-node.bank-<x>.svc`, plus a SPIFFE-style URI SAN naming the bank; **pending work** with Zane and Caleb) |
| CNI enforcing NetworkPolicy | k3d: k3s's built-in network policy controller. EKS: TODO | k3d: enforced; `check-netpol.sh` passes |
| Registry | ECR | TODO |
| Images | One pattern: python-slim, `uv sync --frozen --no-dev`, non-root, tagged with the git SHA; only the architectures needed | **pending work** |
| CI | GitHub Actions | TODO |
| Monitoring | No dedicated stack: k3s metrics-server (`kubectl top`), `kubectl get hpa` for KEDA lag and replicas, `kafka-consumer-groups.sh` for lag, CloudWatch for Lambda, JSON logs to stdout | TODO |
| Fault injection | `kubectl delete pod`, `kubectl scale --replicas=0`, a temporary deny-all NetworkPolicy | TODO |

## Per-service settings

Each owner fills in their own row.

| Service | Owner | Image | Ports | Readiness / liveness | Resources | Config / Secrets |
| --- | --- | --- | --- | --- | --- | --- |
| Replay Job | Macarius | | — | — | | |
| Ingestor (Lambda on EKS) | Macarius | | HTTP | HTTP `/healthz` | | |
| Scorer | Cindy | | — | | | |
| Privacy node | Zane | | gRPC | gRPC health | | |
| Case service / UI | Jia Rui | | HTTP | HTTP `/healthz` | | |
| Summary service (Lambda on EKS) | Jia Rui | | HTTP | HTTP `/healthz` | | |

Each owner sends Isaac their service's runtime contract by Wed 4 Nov: port, environment variables, readiness check, JSON logs to stdout, and image entrypoint. Readiness probes only, with no dependency-tied liveness probes. Deployment files (`deploy/`, Dockerfiles) go through Isaac.

## Namespaces and network paths

One namespace per bank: `bank-a`, `bank-b`, `bank-c`, each a release of the `deploy/charts/bank` Helm chart.

Current baseline (`bank-baseline` policy): deny all, then allow traffic within the bank's own namespace, DNS, the Kubernetes API (TCP 443/6443 to the chart's `apiServerCIDRs` only; `k3d-up.sh` pins the k3d node subnet to match, and EKS needs its own value), and ingress from the `strimzi`, `cnpg-system` and `keda` operators. k3s applies a new pod's egress policy a few seconds after the pod starts, so its first connections can bypass it. Strimzi's own generated policies are turned off so that they can't open Kafka to other namespaces. Cross-bank traffic is blocked.

**pending work** (with Caleb): keep the default-deny baseline, add one small policy per store (Kafka, PostgreSQL, Valkey) allowing only labelled clients, and one allow rule per real path, each with an open/blocked probe pair in `check-netpol.sh`:

| Path | Rule |
| --- | --- |
| Privacy node ↔ other banks' privacy nodes | Ingress from the other two bank namespaces **and** `app: privacy-node` pods, gRPC port only, with matching egress |
| Replay → ingestor (k3d) | Ingress from the `replay` namespace to `app: ingestor` on its HTTP port |
| Ingestor Lambda → Kafka (EKS) | Ingress from the Lambda's VPC subnets to the broker port |
| Case service → summary Lambda (EKS) | Egress to AWS on 443; locally the summary container sits in the namespace |
| Analyst → web UI | `kubectl port-forward` only, no Ingress; access control is cluster RBAC |

The API-server rule should be narrowed from the whole k3d node subnet. On EKS, enable the VPC CNI network-policy feature and run the probes as a Job.

## Interfaces

Agreed contract first ([conventions](../README.md#conventions)): the producer adds one sample message here, and both sides' tests load it.

| Interface | From → to | Pins down | Status | Agree by |
| --- | --- | --- | --- | --- |
| Runtime contract per service ([deployment.md](deployment.md#per-service-settings)) | Every owner → Isaac | Port, env vars, readiness check, JSON logs to stdout, image entrypoint | **draft** | Wed 4 Nov |
| Evaluation timestamps ([evaluation.md](../evaluation.md#metric-definitions)) | Every owner → Isaac | `payment_id` or `chain_digest` logged with the service's own time | **draft** | Fri 6 Nov |

## Open

- [ ] EKS setup, teardown and check commands; budget and shutdown routine (Isaac).
- [ ] How the replay Job gets its workload: baked into its image or mounted (Isaac, with Macarius).
