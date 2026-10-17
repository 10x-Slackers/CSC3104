# Deployment contract

Owner: Isaac · Partners: every owner (per-service settings), Caleb (NetworkPolicy, Secrets, mTLS) · Status: TODO

Scope: one-command k3d setup, the per-bank Helm chart, image builds, EKS, monitoring and the evaluation harness.

## Environments

| | Local | Cloud |
| --- | --- | --- |
| Cluster | k3d | Amazon EKS |
| Setup command | TODO | TODO |
| Cost controls | — | TODO: budget and shutdown routine |

## Platform components

| Component | Choice | Status |
| --- | --- | --- |
| Kafka | Strimzi, KRaft mode | TODO |
| PostgreSQL | CloudNativePG or plain StatefulSet | TODO |
| Autoscaling | KEDA | TODO |
| Certificates | cert-manager | TODO |
| CNI enforcing NetworkPolicy | TODO (both k3d and EKS) | TODO |
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

One namespace per bank. Allowed paths: TODO, defined with Caleb. Test enforcement in both environments.

## Open

- TODO
