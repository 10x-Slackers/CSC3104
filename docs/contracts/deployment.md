# Deployment contract

Owner: Isaac · Partners: every owner (per-service settings), Caleb (NetworkPolicy, Secrets, mTLS) · Code: `deploy/` · Status: **draft**, needs review by Caleb

> [!NOTE]
> **Pending work (decided 30 Oct 2026).** The sections below describe what exists today; these items change it:
> - **HA flag:** one values flag runs a bank with replicas (Kafka 3 with `min.insync.replicas` 2, PostgreSQL 3, privacy node 2). One bank uses it on k3d, which is a one-node PoC, so failover there is pod-level only; every bank uses it on EKS.
> - **State through Valkey:** the chart already runs the target topology, but nothing uses Valkey yet. The scorer keeps state in memory and takes CLI flags until its `ValkeyState` and env-var config land (Cindy); the node starts empty and finds no chains until it reads Valkey (Zane).
> - **New components:** the summary service as a container locally; the ingestor and summary service as Lambda function URLs (`AWS_IAM`) on EKS; replay as a Job in a `replay` namespace.
> - **Hardening:** per-path and per-store NetworkPolicies; PSS `warn: restricted`; k3s secrets encryption.
> - **EKS:** scripted cluster, VPC CNI network policy, Pod Identity, ElastiCache, a Kafka listener for the ingestor Lambda.

Scope: one-command k3d setup, the per-bank Helm chart, image builds, EKS, monitoring and the evaluation harness.

## Environments

| | Local | Cloud |
| --- | --- | --- |
| Cluster | k3d | Amazon EKS |
| Setup command | `deploy/k3d-up.sh` | TODO |
| Replay | `deploy/replay.sh <workload> [--speed N \| --rate N]`: a Job in the `replay` namespace reading `apps/dataset/out/<workload>` | TODO |
| Teardown | `k3d cluster delete csc3104` | TODO |
| Check | `deploy/check-netpol.sh` | TODO |
| Tested on | 4 CPU, 8 GB VM, Docker 29: setup about 3 min, about 3.8 GB memory in use with no bank services yet | |
| Cost controls | — | TODO: budget and shutdown routine |

## Platform components

| Component | Choice | Status |
| --- | --- | --- |
| Kafka | Strimzi 1.2.0, KRaft mode | k3d: one single-node cluster per bank, bootstrap `kafka-kafka-bootstrap:9092` (plaintext). Declared `KafkaTopic` `bank-<X>-transactions` through the Topic Operator: 6 partitions keyed by account, `retention.ms -1`, auto-create off; the scorer is its only consumer. **pending work:** 3 brokers under the HA flag |
| PostgreSQL | CloudNativePG 1.30.1 | k3d: one instance per bank, `postgres-rw:5432`, credentials in Secret `postgres-app`. **pending work:** `instances: 3` under the HA flag; separate databases for the privacy node and the case service |
| Valkey | One instance per bank (ElastiCache on EKS) | k3d: `valkey/valkey:9.1.3-alpine`, Service `valkey:6379`, no persistence (derived state, rebuilt from Kafka). **pending work:** only the scorer and privacy node may connect |
| Autoscaling | KEDA 2.21.0 | k3d: `ScaledObject/scorer` on group `scorer-<X>`'s lag (threshold 100), 1–6 replicas, `offsetResetPolicy: earliest` |
| Serverless | AWS Lambda function URLs, `AuthType: AWS_IAM` | **pending work:** Ingestor and summary service; plain containers in the namespace on k3d |
| Certificates | cert-manager v1.21.2 | k3d: lab CA `ClusterIssuer/bank-ca`; Secret `privacy-node-tls` per bank: DNS SAN `privacy-node.bank-<x>.svc`, URI SAN `spiffe://csc3104/bank/<X>/privacy-node`, server and client auth; mounted at `/tls` without `subPath` |
| Keys | `privacy_node.demo.prepare_demo_keys` (lab only) | `k3d-up.sh` runs it in the privacy-node image on every setup, replacing `deploy/charts/bank/keys/` (gitignored). Each release packs only `keys/<bank>/` into Secret `bank-keys`, mounted at `/keys` without `subPath`, so the node's 10 s `active.json` re-read sees a `helm upgrade` |
| CNI enforcing NetworkPolicy | k3d: k3s's built-in network policy controller. EKS: TODO | k3d: enforced; `check-netpol.sh` passes |
| Registry | ECR | TODO |
| Images | One pattern, the root `Dockerfile`: `docker build --build-arg APP=<package> -t csc3104/<package>:$(git rev-parse --short HEAD) .`; only the architectures needed. Non-root (UID 65532), and the image holds no state: durable data goes to PostgreSQL. The entrypoint is `tini`, so SIGTERM reaches the app; the chart sets each container's `args` (`[python, -m, <package>.<module>]`), never `command`, which would replace `tini`. The ingestor Lambda ships as a zip instead | Dockerfile done; `k3d-up.sh` builds the ingestor, scorer, privacy-node and dataset (replay) images and imports them with `k3d image import`. ECR **pending work** |
| CI | GitHub Actions | TODO |
| Monitoring | No dedicated stack: k3s metrics-server (`kubectl top`), `kubectl get hpa` for KEDA lag and replicas, `kafka-consumer-groups.sh` for lag, CloudWatch for Lambda, JSON logs to stdout | TODO |
| Fault injection | `kubectl delete pod`, `kubectl scale --replicas=0`, a temporary deny-all NetworkPolicy | TODO |

## Per-service settings

Each owner fills in their own row.

| Service | Owner | Image | Ports | Readiness / liveness | Resources | Config / Secrets |
| --- | --- | --- | --- | --- | --- | --- |
| Replay Job | Macarius | The one pattern; entrypoint `python -m dataset.replay <workload> --url A=<url> --url B=<url> --url C=<url>` with `--speed` or `--rate` ([data.md](data.md#replay-replaypy)) | None: an HTTP client to each bank's ingestor port | None: a Job that exits 0 when done, 1 if an ingestor rejects a batch (`400`) | 31 MB resident replaying all of `altman-small` (measured); memory request 64Mi, limit 128Mi; about one CPU core at `--speed 0` | `--url` per bank; the workload directory, mounted read-only at `/workloads/<workload>` (agreed on [#29](https://github.com/10x-Slackers/CSC3104/issues/29); k3d: `k3d-up.sh` mounts `apps/dataset/out` into the node, and the Job uses a `hostPath`; EKS: the Terraform bucket `csc3104-workloads-<account-id>`, copied by an init container into an `emptyDir` of 1Gi, **pending work**); no Secrets on k3d; HTTPS and SigV4 credentials on EKS (**pending work**); one JSON log line per POST to stdout |
| Ingestor (Lambda on EKS) | Macarius | The one pattern; entrypoint `python -m ingestor.app`; Lambda handler `ingestor.app.lambda_handler`, timeout 30 s | HTTP `8080` (`PORT`): `POST /` with a batch | Readiness `GET /healthz`: up once it serves; a Kafka outage shows as `503` on `POST` instead | 29 MB resident when idle (measured); memory request 64Mi, limit 128Mi; CPU to be measured under load | `INGESTOR_BANK` (`A`, `B` or `C`), `KAFKA_BOOTSTRAP_SERVERS` (k3d: `kafka-kafka-bootstrap:9092`), `PORT`; no Secrets; one JSON log line per batch to stdout ([data.md](data.md#ingestor)) |
| Scorer | Cindy | The one pattern; entrypoint `python -m scorer.stream`. A Deployment scaled by KEDA (1–6 replicas, one consumer group) | None: a Kafka consumer, no Service routes to it | None (**agreed**, Isaac, 4 Nov): it serves no traffic, and KEDA scales on lag, not readiness. A fatal Kafka or Valkey error exits non-zero, so Kubernetes restarts it. SIGTERM finishes the current event, stores its offset and closes the consumer (**pending work**), inside the default 30 s grace | About 39 MB resident when idle (measured, macOS arm64, before the Valkey client). librdkafka prefetches up to 64 MiB per consumer by default, so the consumer sets `queued.max.messages.kbytes` to 16384 (16 MiB, **pending work**). Memory request 64Mi, limit 128Mi, to confirm by measuring resident memory with a backlog; CPU to be measured under load | **pending work** (today: CLI flags, [streaming.md](streaming.md#kafka-events)): `SCORER_BANK` (`A`, `B` or `C`), `KAFKA_BOOTSTRAP_SERVERS` (k3d: `kafka-kafka-bootstrap:9092`), `KAFKA_GROUP` (default `scorer-<bank>`; change it only to rebuild Valkey), `VALKEY_URL` (k3d: `valkey://valkey:6379`), `KEY_DIR`: this bank's X25519 key files and `<KEY_DIR>/<bank>/active.json`, mounted read-only in the [security.md](security.md) layout. Like the node, the scorer re-reads `active.json` every 10 s and writes `tok:` keys for every version in `active_versions`; a newly active version is backfilled from the `pay:` keys, so peers find tokens under either version during a rotation. Egress to Kafka 9092 and Valkey 6379 only. One JSON line to stdout per event that writes a link or hit: `ts`, `service: scorer`, `bank`, `payment_id`, `kafka_ts` (the ingestor's record timestamp), `links`, `hits`; errors as `level: error` lines |
| Privacy node | Zane | | gRPC | gRPC health | | |
| Case service / UI | Jia Rui | | HTTP | HTTP `/healthz` | | |
| Summary service (Lambda on EKS) | Jia Rui | | HTTP | HTTP `/healthz` | | |

On k3d the chart runs one Deployment per service with the shared non-root pod settings: ingestor (Service `ingestor:8080`) → Kafka → scorer (the only consumer, scaled by KEDA) → Valkey ← privacy node (Service `privacy-node:50051`, readiness on its TCP port, no Kafka access) ↔ the other banks' nodes over mTLS.

Each owner sends Isaac their service's runtime contract by Wed 4 Nov: port, environment variables, readiness check, JSON logs to stdout, and image entrypoint. Readiness probes only, with no dependency-tied liveness probes. Deployment files (`deploy/`, Dockerfiles) go through Isaac.

## Namespaces and network paths

One namespace per bank: `bank-a`, `bank-b`, `bank-c`, each a release of the `deploy/charts/bank` Helm chart.

Current baseline (`bank-baseline` policy): deny all, then allow traffic within the bank's own namespace, DNS, the Kubernetes API (TCP 443/6443 to the chart's `apiServerCIDRs` only; `k3d-up.sh` pins the k3d node subnet to match, and EKS needs its own value), and ingress from the `strimzi`, `cnpg-system` and `keda` operators. k3s applies a new pod's egress policy a few seconds after the pod starts, so its first connections can bypass it. Strimzi's own generated policies are turned off so that they can't open Kafka to other namespaces. Cross-bank traffic is blocked.

**pending work** (with Caleb): keep the default-deny baseline, add one small policy per store (Kafka, PostgreSQL, Valkey) allowing only labelled clients, and one allow rule per real path, each with an open/blocked probe pair in `check-netpol.sh`:

| Path | Rule |
| --- | --- |
| Privacy node ↔ other banks' privacy nodes | Ingress from the other two bank namespaces **and** `app: privacy-node` pods, gRPC port only, with matching egress. **Done** (`privacy-node-peers`), probed in `check-netpol.sh` |
| Replay → ingestor (k3d) | Ingress from the `replay` namespace to `app: ingestor` on its HTTP port. **Done** (`replay-to-ingestor`), probed in `check-netpol.sh` |
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
- [x] How the replay Job gets its workload: mounted (agreed on [#29](https://github.com/10x-Slackers/CSC3104/issues/29), 9 Nov).
