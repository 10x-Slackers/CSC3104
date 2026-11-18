# Deployment contract

Owner: Isaac · Partners: every owner (per-service settings), Caleb (NetworkPolicy, Secrets, mTLS) · Code: `deploy/` · Status: **draft**, needs review by Caleb

> [!NOTE]
> **Pending work (decided 30 Oct 2026).** The sections below describe what exists today; these items change it:
> - **HA flag:** one values flag runs a bank with replicas (Kafka 3 with `min.insync.replicas` 2, PostgreSQL 3, privacy node 2). One bank uses it on k3d, which is a one-node PoC, so failover there is pod-level only; every bank uses it on EKS.
> - **New components:** the summary service as a container locally; the ingestor and summary service as Lambda function URLs (`AWS_IAM`) on EKS; replay as a Job in a `replay` namespace.
> - **Hardening:** per-path and per-store NetworkPolicies; PSS `warn: restricted`; k3s secrets encryption.
> - **EKS:** the account is an AWS Academy Learner Lab, which can't create IAM roles, so everything runs as the pre-made `LabRole` and Pod Identity is dropped. Pods take the role from the node's metadata service instead. The bank namespaces' default-deny policy blocks it (probed in `check-netpol.sh`), though a new pod may get through in the seconds before its policy applies. `deploy/eks-up.sh` sets it all up with the same chart as k3d (Environments, below). Still to come: the HA flag, ElastiCache, the Lambdas and a Kafka listener for the ingestor Lambda.

Scope: one-command k3d setup, the per-bank Helm chart, image builds, EKS, monitoring and the evaluation harness.

## Environments

| | Local | Cloud |
| --- | --- | --- |
| Cluster | k3d | Amazon EKS |
| Setup command | `deploy/k3d-up.sh`: the cluster and images, then `deploy/install.sh` (operators, lab CA, keys, the three bank releases) | `deploy/eks-up.sh`, with the Learner Lab's credentials in `~/.aws/credentials`: Terraform, the workloads bucket, images pushed to ECR, then `deploy/install.sh` with the EKS API server addresses |
| Replay | `deploy/replay.sh <workload> [--speed N \| --rate N]`: a Job in the `replay` namespace reading `apps/dataset/out/<workload>` | The same command: an init container first copies `s3://csc3104-workloads-<account-id>/<workload>` into an `emptyDir`, as `LabRole`. Upload a workload once with `aws s3 sync apps/dataset/out/<workload> s3://csc3104-workloads-<account-id>/<workload>`. Plain HTTP to the ingestor Services until the ingestor Lambda lands |
| Teardown | `k3d cluster delete csc3104` | `deploy/eks-down.sh`: deletes the bank namespaces and waits for their EBS volumes to go, then `terraform destroy` (a plain destroy leaves the volumes behind) |
| Check | `deploy/check-netpol.sh` | The same command, plus a probe that bank pods can't reach the node's metadata service |
| Tested on | 4 CPU, 8 GB VM, Docker 29: setup about 3 min, about 3.8 GB memory in use with no bank services yet | Learner Lab, 3 × `t3a.large`, Kubernetes 1.37: setup about 20 min, `tiny` replayed all 200, 25/25 policy probes |
| Cost controls | — | The Learner Lab's fixed credit; it stops the nodes when a session ends, but the control plane keeps billing, so destroy after each run |

## Platform components

| Component | Choice | Status |
| --- | --- | --- |
| Kafka | Strimzi 1.2.0, KRaft mode | k3d: one single-node cluster per bank, bootstrap `kafka-kafka-bootstrap:9092` (plaintext). Declared `KafkaTopic` `bank-<X>-transactions` through the Topic Operator: 6 partitions keyed by account, `retention.ms -1`, auto-create off; the scorer is its only consumer. **pending work:** 3 brokers under the HA flag |
| PostgreSQL | CloudNativePG 1.30.1 | k3d: one instance per bank, `postgres-rw:5432`, credentials in Secret `postgres-app`. **pending work:** `instances: 3` under the HA flag; separate databases for the privacy node and the case service |
| Valkey | One instance per bank (ElastiCache on EKS) | k3d: `valkey/valkey:9.1.3-alpine`, Service `valkey:6379`, no persistence (derived state, rebuilt from Kafka). **pending work:** only the scorer and privacy node may connect |
| Autoscaling | KEDA 2.21.0 | k3d: `ScaledObject/scorer` on group `scorer-<X>`'s lag (threshold 100), 1–6 replicas, `offsetResetPolicy: earliest` |
| Serverless | AWS Lambda function URLs, `AuthType: AWS_IAM` | **pending work:** Ingestor and summary service; plain containers in the namespace on k3d |
| Certificates | cert-manager v1.21.2 | k3d: lab CA `ClusterIssuer/bank-ca`; Secret `privacy-node-tls` per bank: DNS SAN `privacy-node.bank-<x>.svc`, URI SAN `spiffe://csc3104/bank/<X>/privacy-node`, server and client auth; mounted at `/tls` without `subPath` |
| Keys | `privacy_node.demo.prepare_demo_keys` (lab only) | `install.sh` runs it in the privacy-node image when `deploy/charts/bank/keys/` (gitignored) is missing, and keeps it after that, so a rerun doesn't change keys under running pods; delete it for fresh keys. Each release packs only `keys/<bank>/` into Secret `bank-keys`, mounted at `/keys` without `subPath`, so the node's 10 s `active.json` re-read sees a `helm upgrade` |
| CNI enforcing NetworkPolicy | k3d: k3s's built-in network policy controller. EKS: the VPC CNI add-on with `enableNetworkPolicy` | k3d: enforced; `check-netpol.sh` passes. EKS: enforced; `check-netpol.sh` passes |
| Registry | ECR | One repository per image, `<account>.dkr.ecr.us-east-1.amazonaws.com/csc3104/<package>` (Terraform output `registry`); `eks-up.sh` pushes each image tagged with the commit |
| Images | One pattern, the root `Dockerfile`: `docker build --build-arg APP=<package> -t csc3104/<package>:$(git rev-parse --short HEAD) .`; only the architectures needed. Non-root (UID 65532), and the image holds no state: durable data goes to PostgreSQL. The entrypoint is `tini`, so SIGTERM reaches the app; the chart sets each container's `args` (`[python, -m, <package>.<module>]`), never `command`, which would replace `tini`. The ingestor Lambda ships as a zip instead | Dockerfile done; `k3d-up.sh` builds the ingestor, scorer, privacy-node and dataset (replay) images and imports them with `k3d image import`. `eks-up.sh` builds them for `linux/amd64` and pushes them to ECR |
| CI | GitHub Actions | TODO |
| Monitoring | No dedicated stack: k3s metrics-server (`kubectl top`), `kubectl get hpa` for KEDA lag and replicas, `kafka-consumer-groups.sh` for lag, CloudWatch for Lambda, JSON logs to stdout | TODO |
| Fault injection | `kubectl delete pod`, `kubectl scale --replicas=0`, a temporary deny-all NetworkPolicy | TODO |

## Per-service settings

Each owner fills in their own row.

| Service | Owner | Image | Ports | Readiness / liveness | Resources | Config / Secrets |
| --- | --- | --- | --- | --- | --- | --- |
| Replay Job | Macarius | The one pattern; entrypoint `python -m dataset.replay <workload> --url A=<url> --url B=<url> --url C=<url>` with `--speed` or `--rate` ([data.md](data.md#replay-replaypy)) | None: an HTTP client to each bank's ingestor port | None: a Job that exits 0 when done, 1 if an ingestor rejects a batch (`400`) | 59 MiB peak resident replaying all of `altman-small` at `--speed 0` into real ingestors (measured, Linux arm64); memory request 64Mi, limit 128Mi; about one CPU core at `--speed 0` | `--url` per bank; the workload directory, mounted read-only at `/workloads/<workload>` (k3d: a `hostPath` from the `k3d-up.sh` volume; EKS: an `emptyDir` filled from the workloads bucket by an init container); no Secrets: on EKS, `https://` function URLs are signed with SigV4 as `LabRole` from the node's metadata service ([data.md](data.md#replay-replaypy)), and `replay.sh` still sends plain HTTP to the ingestor Services until the ingestor Lambda lands; one JSON log line per POST to stdout |
| Ingestor (Lambda on EKS) | Macarius | The one pattern; entrypoint `python -m ingestor.app`; Lambda handler `ingestor.app.lambda_handler`, timeout 30 s | HTTP `8080` (`PORT`): `POST /` with a batch | Readiness `GET /healthz`: up once it serves; a Kafka outage shows as `503` on `POST` instead | 29 MB resident when idle (measured); memory request 64Mi, limit 128Mi. CPU: 0.04–0.07 of a core on average, peaks under 10%, at about 2,100 events a second each (all of `altman-small` at `--speed 0` into three ingestors and one Kafka broker in Docker, Linux arm64); it waits on Kafka's acks for each batch, so CPU doesn't limit it | `INGESTOR_BANK` (`A`, `B` or `C`), `KAFKA_BOOTSTRAP_SERVERS` (k3d: `kafka-kafka-bootstrap:9092`), `PORT`; no Secrets; one JSON log line per batch to stdout ([data.md](data.md#ingestor)) |
| Scorer | Cindy | The one pattern; entrypoint `python -m scorer.stream`. A Deployment scaled by KEDA (1–6 replicas, one consumer group) | None: a Kafka consumer, no Service routes to it | None (**agreed**, Isaac, 4 Nov): it serves no traffic, and KEDA scales on lag, not readiness. A fatal Kafka or Valkey error exits non-zero, so Kubernetes restarts it. SIGTERM finishes the current event, stores its offset and closes the consumer, committing it, inside the default 30 s grace | About 44 MB resident when idle (measured, macOS arm64, with the Valkey client). librdkafka prefetches up to 64 MiB per consumer by default, so the consumer sets `queued.max.messages.kbytes` to 16384 (16 MiB). Measured on Linux x86-64 (Isaac): 58 MB resident after draining a 42,000-event backlog, 67 MB peak; that backlog was about 10 MB, under the prefetch cap. Memory limit 128Mi holds; request 64Mi under review on the chart side; CPU to be measured under load | Environment, each overridable by a flag ([streaming.md](streaming.md#kafka-events)): `SCORER_BANK` (`A`, `B` or `C`), `KAFKA_BOOTSTRAP_SERVERS` (k3d: `kafka-kafka-bootstrap:9092`), `KAFKA_GROUP` (default `scorer-<bank>`; change it only to rebuild Valkey), `VALKEY_URL` (default `valkey://localhost:6379`; k3d: `valkey://valkey:6379`), `KEY_DIR`: this bank's X25519 key files and `<KEY_DIR>/<bank>/active.json`, mounted read-only in the [security.md](security.md) layout. Like the node, the scorer re-reads `active.json` every 10 s and writes `tok:` keys for every version in `active_versions`; a newly active version is backfilled from the `pay:` keys, so peers find tokens under either version during a rotation, and a pod restarted mid-rotation redoes the backfill ([streaming.md](streaming.md#token-index)). Without `KEY_DIR` (or the key file flags) no tokens are written. Egress to Kafka 9092 and Valkey 6379 only. One JSON line to stdout per event that writes a link or hit: `ts`, `service: scorer`, `bank`, `payment_id`, `kafka_ts` (the ingestor's record timestamp), `links`, `hits`; errors as `level: error` lines |
| Privacy node | Isaac (from Zane, 18 Nov) | The one pattern; entrypoint `python -m privacy_node.node --bank <X> --valkey-url valkey://valkey:6379 --key-directory /keys --tls-cert /tls/tls.crt --tls-key /tls/tls.key --tls-ca /tls/ca.crt --peer <Y> <host:port> --peer <Z> <host:port> --search --case-url http://case:8000`. One replica until the store moves to PostgreSQL (**pending work:** 2) | gRPC `50051` (`--port`) from the other banks' nodes | TCP on 50051 today; **pending work:** gRPC health, not serving until state is ready | To be measured | Keys at `/keys` and certificates at `/tls` (no `subPath`, so renewals and rotations apply without a restart). Egress to Valkey 6379, the other banks' nodes on 50051 and this bank's case service on 8000. Without `--case-url` nothing is posted to the case service. Logs: Python logging to stderr (dead letters as warnings) |
| Case service / UI | Jia Rui | The one pattern; entrypoint `python -m case.app`. One replica (the store's transfer join takes a table lock, so more replicas only queue) | HTTP `8000` (`PORT`): `POST /results` from this bank's privacy node, the analyst UI and `/api/cases` | Readiness `GET /healthz`: up once it serves; it doesn't check PostgreSQL or the summary service | To be measured | `CASE_BANK` (`A`, `B` or `C`); `CASE_DATABASE_URL`, `postgresql://<user>:<password>@postgres-rw:5432/cases` from the CloudNativePG app Secret (a separate `cases` database, **pending work**); `CASE_KEY_DIRECTORY` (`/keys`, the `bank-keys` Secret: public signing keys only are read); `CASE_SUMMARY_URL` (k3d: `http://summary:8080/`; EKS: the function URL); `CASE_SUMMARY_TIMEOUT` (default `2` s); `CASE_SUMMARY_REGION` (EKS only: signs requests with SigV4 as `LabRole`, using credentials from the node's metadata service; unset on k3d). Ingress from the privacy node only, plus `kubectl port-forward`; egress to PostgreSQL 5432 and the summary service. One JSON line to stdout per message: `ts`, `service: case`, `bank`, `event`, `key`, `kind`, `case_id`, `tier`, `raised_at` ([case.md](case.md#analyst-ui)) |
| Summary service (Lambda on EKS) | Jia Rui | The one pattern; entrypoint `python -m summary.app`; Lambda handler `summary.app.lambda_handler`, function URL `AuthType: AWS_IAM`, timeout 5 s. No dependencies beyond the standard library | HTTP `8080` (`PORT`): `POST /` with one case's JSON | Readiness `GET /healthz` | To be measured | `PORT` only; no Secrets, no state, no egress. Ingress from the case service only. One JSON line to stdout (CloudWatch on Lambda) per request: `ts`, `service: summary`, `status`, `ms`, `cold`, `case_id` |

On k3d the chart runs one Deployment per service with the shared non-root pod settings: ingestor (Service `ingestor:8080`) → Kafka → scorer (the only consumer, scaled by KEDA) → Valkey ← privacy node (Service `privacy-node:50051`, readiness on its TCP port, no Kafka access) ↔ the other banks' nodes over mTLS.

Each owner sends Isaac their service's runtime contract by Wed 4 Nov: port, environment variables, readiness check, JSON logs to stdout, and image entrypoint. Readiness probes only, with no dependency-tied liveness probes. Deployment files (`deploy/`, Dockerfiles) go through Isaac.

## Namespaces and network paths

One namespace per bank: `bank-a`, `bank-b`, `bank-c`, each a release of the `deploy/charts/bank` Helm chart.

Current baseline (`bank-baseline` policy): deny all, then allow traffic within the bank's own namespace, DNS, the Kubernetes API (TCP 443/6443 to the chart's `apiServerCIDRs` only; `k3d-up.sh` pins the k3d node subnet to match; `eks-up.sh` passes the control plane's addresses and the `kubernetes` Service address, since pods share the VPC; `check-netpol.sh` probes that another 443 service stays blocked), and ingress from the `strimzi`, `cnpg-system` and `keda` operators. k3s applies a new pod's egress policy a few seconds after the pod starts, so its first connections can bypass it. Strimzi's own generated policies are turned off so that they can't open Kafka to other namespaces. Cross-bank traffic is blocked.

**pending work** (with Caleb): keep the default-deny baseline, add one small policy per store (Kafka, PostgreSQL, Valkey) allowing only labelled clients, and one allow rule per real path, each with an open/blocked probe pair in `check-netpol.sh`:

| Path | Rule |
| --- | --- |
| Privacy node ↔ other banks' privacy nodes | Ingress from the other two bank namespaces **and** `app: privacy-node` pods, gRPC port only, with matching egress. **Done** (`privacy-node-peers`), probed in `check-netpol.sh` |
| Replay → ingestor (k3d) | Ingress from the `replay` namespace to `app: ingestor` on its HTTP port. **Done** (`replay-to-ingestor`), probed in `check-netpol.sh` |
| Ingestor Lambda → Kafka (EKS) | Ingress from the Lambda's VPC subnets to the broker port |
| Case service → summary Lambda (EKS) | Egress to AWS on 443, and to the node's metadata service (`169.254.169.254:80`) from `app: case-service` only, so it can sign the call as `LabRole`. This hands that pod the whole role: an accepted limitation of the lab account. Locally the summary container sits in the namespace |
| Analyst → web UI | `kubectl port-forward` only, no Ingress; access control is cluster RBAC |

The API-server rule should be narrowed from the whole k3d node subnet. On EKS the VPC CNI enforces the same policies and `check-netpol.sh` runs the same probes.

## Interfaces

Agreed contract first ([conventions](../README.md#conventions)): the producer adds one sample message here, and both sides' tests load it.

| Interface | From → to | Pins down | Status | Agree by |
| --- | --- | --- | --- | --- |
| Runtime contract per service ([deployment.md](deployment.md#per-service-settings)) | Every owner → Isaac | Port, env vars, readiness check, JSON logs to stdout, image entrypoint | **draft** | Wed 4 Nov |
| Evaluation timestamps ([evaluation.md](../evaluation.md#metric-definitions)) | Every owner → Isaac | `payment_id` or `chain_digest` logged with the service's own time | **draft** | Fri 6 Nov |

## Open

- [x] EKS setup and check commands (Isaac): `eks-up.sh`, `replay.sh`, `check-netpol.sh`, `eks-down.sh`.
- [x] How the replay Job gets its workload: mounted (agreed on [#29](https://github.com/10x-Slackers/CSC3104/issues/29), 9 Nov).
