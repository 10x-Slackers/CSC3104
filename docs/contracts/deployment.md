# Deployment contract

Owner: Isaac · Partners: every owner (per-service settings), Caleb (NetworkPolicy, Secrets, mTLS) · Code: `deploy/` · Status: **draft**, needs review by Caleb

> [!NOTE]
> **Pending work (decided 30 Oct 2026).** The sections below describe what exists today; these items change it:
> - **Topic:** a declared `KafkaTopic` per bank (6 partitions, key = account, `retention.ms -1`, auto-create off).
> - **HA flag:** one values flag runs a bank with replicas (Kafka 3 with `min.insync.replicas` 2, PostgreSQL 3, privacy node 2). One bank uses it on k3d, which is a one-node PoC, so failover there is pod-level only; every bank uses it on EKS.
> - **New components:** Valkey per bank; KEDA `ScaledObject` for the scorer; the ingestor and summary service as containers locally and Lambda function URLs (`AWS_IAM`) on EKS; replay as a Job in a `replay` namespace.
> - **Hardening:** Role 4 has prepared per-path/per-store NetworkPolicies and probes below for Isaac review; workload `automountServiceAccountToken: false`, requests, memory limits, readiness probes, PSS `warn: restricted`, k3s secrets encryption and one Dockerfile pattern still belong to deployment work.
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
| Certificates | cert-manager v1.21.2 | Lab CA `ClusterIssuer/bank-ca`; Secret `privacy-node-tls` per bank, DNS SANs `privacy-node.bank-<x>.svc[.cluster.local]`, URI SAN `spiffe://csc3104/bank/<A\|B\|C>/privacy-node`, server/client auth and fresh private key on renewal |
| CNI enforcing NetworkPolicy | k3d: k3s's built-in network policy controller. EKS: TODO | Revised policies and probes require Isaac's k3d run; previous baseline evidence does not validate this revision |
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

Role 4 review prepared the following paths for Isaac's deployment review. `bank-baseline` denies ingress and egress for every pod, with only kube-dns UDP/TCP 53 allowed for all bank pods. Each listed local path has both source egress and destination ingress. Source/destination pod selectors without a namespace selector stay inside one bank. Cross-bank peers require the bank namespace **and** `app: privacy-node` in the same selector entry.

| Path | Rule |
| --- | --- |
| Ingestor/scorer/privacy node → Kafka | Local `app: ingestor`, `scorer`, `privacy-node` → `strimzi.io/name: kafka-kafka`, TCP 9092. The node currently consumes Kafka in-process; remove that path when Zane finishes the Valkey handoff |
| Scorer/privacy node → Valkey | Local `app: scorer`/`privacy-node` → `app: valkey`, TCP 6379; Valkey deployment is still Isaac-owned |
| Privacy node/case → PostgreSQL | Local `app: privacy-node`/`case` → `cnpg.io/cluster: postgres`, TCP 5432 |
| Privacy node → case service | Local `app: privacy-node` → `app: case`, TCP 8000 |
| Case → existing mock intake | Local `app: case` → `app: intake`, TCP 8001; remove with Jia Rui's intake removal |
| Privacy node ↔ other banks' privacy nodes | `bank-a`/`bank-b`/`bank-c` namespace **and** `app: privacy-node`, TCP 50051, matching egress; certificate identity is still required |
| PostgreSQL control | Same-cluster PostgreSQL and the labelled `cnpg-system` operator only, TCP 5432/8000. Only PostgreSQL pods may reach the configured API-server CIDRs on 443/6443 |
| Kafka control | Same-bank brokers and labelled Strimzi operator, TCP 9090/9091/8443; labelled KEDA operator to Kafka 9092 |
| Replay → ingestor / ingestor Lambda → Kafka | **Isaac pending:** add only after the ingestor runtime HTTP port and EKS Lambda subnets are agreed; no namespace-wide allowance |
| Case → summary container/Lambda | **Isaac/Jia Rui pending:** summary deployment and runtime port, or HTTPS Function URL plus Pod Identity credential path; no broad HTTPS rule is pre-opened |
| Analyst → web UI | `kubectl port-forward` only, no Ingress; access control is cluster RBAC |

The app workloads do not yet exist in this chart: Isaac must apply the labels above and check the deployed operator labels (`name: strimzi-cluster-operator`, `app: keda-operator`, `app.kubernetes.io/name: cloudnative-pg`). No bank may reach another bank's Kafka, PostgreSQL, Valkey or case service. Kubernetes labels are trusted deployment configuration; RBAC must prevent an untrusted workload from assigning itself a privileged label.

Strimzi-generated policies remain disabled, since policies are additive. k3s applies egress policy shortly after pod creation; probes wait ten seconds. Run `bash deploy/k3d-up.sh` then `bash deploy/check-netpol.sh | tee netpol-evidence.txt`. The revised script proves each blocked destination OPEN with an authorized source first, covers cross-bank peer access, cross-bank Kafka/PostgreSQL denial, untrusted-namespace denial, local store/case paths and API access. Temporary unready TCP listeners test app policies before app deployments exist; they are kept out of real Service endpoints and deleted on exit. These are connectivity checks, not application/TLS checks. Capture the output for the submission; the current Windows k3d attempt reached operator readiness/bank release installation but the API and Docker engine failed during bank-store readiness, before probes could run. See [the attempt and cleanup record](../evidence/role4-k3d-attempt.txt).

Without a cluster, run `helm lint deploy/charts/bank` and `uv run pytest apps/privacy_node/tests/test_deployment_security.py -q` with Helm on `PATH` (or environment variable `HELM` set to its executable). Tests render the actual chart and assert 44 authorized/blocked paths, certificate identity/default-deny, and absence of public Function URL/static AWS key configuration. The model checks both ingress and egress and does not replace live CNI probes. Helm-dependent tests explicitly skip if the CLI is unavailable.

The API-server rule should be narrowed from the whole k3d node subnet. On EKS, enable VPC CNI network policy and re-run the probes. Do not copy the k3d API CIDRs into EKS.

## Peer certificate mounts and renewal

Mount `privacy-node-tls` as a read-only **directory**, not with `subPath`, and pass `--tls-cert /tls/tls.crt --tls-key /tls/tls.key --tls-ca /tls/ca.crt` to the node. Use DNS peer targets, e.g. `privacy-node.bank-b.svc:50051`, so gRPC verifies the certificate hostname. `TlsFiles` reads current client files for each peer call; gRPC's dynamic server certificate callback re-reads files before each new TLS connection. Existing connections retain their session; subsequent calls use new channels. The local fixture demo explicitly opts into insecure mode and must never be used as the Kubernetes transport configuration.

The receiver derives bank identity from the authenticated URI SAN and matches it to protobuf `sender`, before processing or cache lookup. A trusted CA certificate without the bank URI is insufficient. The shared lab CA is a trusted third party: anyone holding its private key can impersonate any bank. This is a coursework limitation, not independent bank PKI.

## Summary Function URL IAM review

There is no Lambda resource or Function URL client in the current repository; `TemplateClient` generates local reports. The future summary receives bank-local customer data. Isaac must configure `AuthType: AWS_IAM`, give only the case workload's Pod Identity role `lambda:InvokeFunctionUrl` and `lambda:InvokeFunction` on its summary function, and avoid public invocation grants. Jia Rui must sign HTTPS requests with SigV4 using the SDK's refreshable default credential provider chain (EKS Pod Identity), with no hard-coded/static access keys. Enabling service-account tokens for the workload identity integration, if required, must be scoped to that workload. Isaac must allow only the required summary HTTPS and Pod Identity agent credential path when implementing EKS; those paths cannot be validated before that deployment exists.

Verify after deployment: `aws lambda get-function-url-config --function-name "$SUMMARY_FUNCTION" --query AuthType --output text` must print `AWS_IAM`; an unsigned request must return 403; a signed request from the case service's Pod Identity role must succeed; a different workload role must fail. These are remaining Isaac/Jia Rui integration checks, not completed cloud evidence.

References: [gRPC credentials and reload API](https://grpc.github.io/grpc/python/grpc.html), [cert-manager Certificate SANs/usages](https://cert-manager.io/docs/usage/certificate/), [Kubernetes selector semantics](https://kubernetes.io/docs/concepts/services-networking/network-policies/), [CloudNativePG networking](https://cloudnative-pg.io/docs/1.30/networking/), [Strimzi internal ports](https://strimzi.io/docs/operators/in-development/full/deploying.html), [Lambda IAM/SigV4 invocation](https://docs.aws.amazon.com/lambda/latest/dg/urls-invocation.html).

## Interfaces

Agreed contract first ([conventions](../README.md#conventions)): the producer adds one sample message here, and both sides' tests load it.

| Interface | From → to | Pins down | Status | Agree by |
| --- | --- | --- | --- | --- |
| Runtime contract per service ([deployment.md](deployment.md#per-service-settings)) | Every owner → Isaac | Port, env vars, readiness check, JSON logs to stdout, image entrypoint | **draft** | Wed 4 Nov |
| Evaluation timestamps ([evaluation.md](../evaluation.md#metric-definitions)) | Every owner → Isaac | `payment_id` or `chain_digest` logged with the service's own time | **draft** | Fri 6 Nov |

## Open

- [ ] EKS setup, teardown and check commands; budget and shutdown routine (Isaac).
- [ ] How the replay Job gets its workload: baked into its image or mounted (Isaac, with Macarius).
