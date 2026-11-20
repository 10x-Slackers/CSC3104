# Deployment

How the system is built, deployed and operated: locally on k3d, and on AWS with the same charts. Every command is a `make` target ([Makefile](../Makefile)); `make` alone lists them.

## Prerequisites

| For | Tools |
| --- | --- |
| Code checks | `uv`, `make`, `git` |
| Local cluster | `docker`, `k3d`, `kubectl`, `helm`, `helmfile` |
| AWS | also `aws`, `terraform`, `zip`, and the lab's credentials in `~/.aws/credentials` |

## One image

Every service runs the same image, built from the root [Dockerfile](../Dockerfile). The chart picks the module with the container's `args`, for example `python -m mulechain.scorer`. The image runs as a non-root user and holds no state, and `tini` is its entrypoint so that a stop signal reaches the service.

## Commands

| Command | What it does |
| --- | --- |
| `make check` | Lint, format check and unit tests |
| `make proto` | Regenerate the gRPC code from `protocol.proto` |
| `make k3d` | Create the local cluster, build and load the image, deploy all three banks |
| `make image` / `make load` | Build the image / copy it into the k3d cluster |
| `make deploy` | Install or update everything in the current cluster and wait until it is ready |
| `make replay WORKLOAD=tiny` | Replay a workload from `workloads/` into the banks; `ARGS` passes replay flags ([data.md](data.md#replay)) |
| `make cases` | Print each bank's cases as sorted `bank tier payments` lines |
| `make ui BANK=b` | Open a bank's analyst page on <http://localhost:8000> |
| `make cluster-check` | Check the running cluster (below) |
| `make key-add` / `make key-drop` | The two steps of a key rotation |
| `make rebuild BANK=a` | Empty a bank's Valkey and rebuild it from Kafka |
| `make k3d-down` | Delete the local cluster |
| `make lambda`, `make eks`, `make eks-down` | The AWS path (below) |

Two variables are chosen for you:

- `ENV` is `eks` when `kubectl`'s current context is an EKS cluster and `k3d` otherwise, so the same commands work on both.
- `TAG`, the image tag, is a hash of everything that goes into the image (`Dockerfile`, `pyproject.toml`, `uv.lock`, `src/`). Changed code always gets a new tag, so `make load deploy` rolls the pods.

`CLUSTER` names the k3d cluster and defaults to `csc3104`. If a cluster of that name already exists, pass another (`make k3d CLUSTER=<name>`), and the same one to `make load` and `make k3d-down`.

## What `make k3d` installs

1. A one-node k3d cluster from [k3d.yaml](../deploy/k3d.yaml), with Secrets encrypted at rest and `workloads/` mounted for the replay Job.
2. Everything in [helmfile.yaml.gotmpl](../deploy/helmfile.yaml.gotmpl), in dependency order:

| Release | Chart | Purpose |
| --- | --- | --- |
| `strimzi` | Strimzi operator 1.2.0 | Runs each bank's Kafka (KRaft mode) |
| `cnpg` | CloudNativePG chart 0.29.1 | Runs each bank's PostgreSQL |
| `keda` | KEDA 2.21.0 | Scales the scorers on Kafka consumer lag |
| `cert-manager` | cert-manager v1.21.2 | Issues and renews the privacy nodes' certificates |
| `platform` | [charts/platform](../deploy/charts/platform) | The bank namespaces with their Pod Security labels, the `replay` namespace, the lab CA |
| `bank-a`, `bank-b`, `bank-c` | [charts/bank](../deploy/charts/bank) | One release of the same chart per bank, each in its own namespace |

`make deploy` then waits for every Kafka cluster, PostgreSQL cluster and certificate to be ready and for every bank's Deployments to roll out. If one bank fails, the command fails.

## The bank chart

Values ([values.yaml](../deploy/charts/bank/values.yaml)):

| Value | Meaning |
| --- | --- |
| `bank` | `A`, `B` or `C` |
| `ha` | Run with replicas: Kafka 3 brokers (a write needs 2 in sync), PostgreSQL 3 instances with automatic failover, privacy node 2. On k3d only bank A has it, because a laptop has room for one; on AWS every bank does. Set it on a fresh bank, not a running one |
| `image.repository`, `image.tag` | The one image |
| `aws` | AWS only, from Terraform: this bank's ElastiCache and Lambda endpoints |

What each release runs:

| Service | Module or image | Replicas | Port | Settings | Memory (request / limit) |
| --- | --- | --- | --- | --- | --- |
| Ingestor | `mulechain.ingestor` | 1 | 8080 HTTP | `BANK`, `KAFKA_BOOTSTRAP_SERVERS` | 64Mi / 128Mi |
| Kafka | Strimzi `Kafka` | 1 (3 with `ha`) | 9092 | Topic `bank-<X>-transactions`: 6 partitions, kept forever, no auto-created topics | JVM heap 256m to 512m |
| Scorer | `mulechain.scorer` | 1 to 6, by KEDA: one more per 100 events of lag | none | `BANK`, `KAFKA_BOOTSTRAP_SERVERS`, `VALKEY_URL`, `KEY_DIR` | 64Mi / 128Mi |
| Valkey | `valkey/valkey` | 1 | 6379 | No persistence: it is rebuilt from Kafka | 256Mi / 1Gi |
| Privacy node | `mulechain.node.node` | 1 (2 with `ha`) | 50051 gRPC, mTLS | `BANK`, `PEERS`, `VALKEY_URL`, `DATABASE_URL`, `CASE_URL`, `KEY_DIR`, `TLS_DIR`, `INGESTION_GRACE_SECONDS` (60) | 64Mi / 256Mi |
| PostgreSQL | CloudNativePG `Cluster` | 1 (3 with `ha`) | 5432 at `postgres-rw` | One database for the node's store and the case store; credentials in Secret `postgres-app` | operator default |
| Case service | `mulechain.case.app` | 1 | 8000 HTTP | `BANK`, `DATABASE_URL`, `SUMMARY_URL` | 64Mi / 256Mi |
| Summary service | `mulechain.summary` | 1 | 8080 HTTP | none | 64Mi / 128Mi |

Keys (`bank-keys`, mounted at `/keys`) and the node's certificate (`privacy-node-tls`, at `/tls`) are mounted as whole directories, so a rotation or renewal shows up without a restart. Network paths and pod hardening are in [security.md](security.md).

## Replay and cases

```sh
make replay WORKLOAD=tiny                      # 60 simulated seconds per second (the default)
make replay WORKLOAD=slice-6h ARGS="--speed 0" # as fast as the ingestors accept
```

`make replay` runs a Job in the `replay` namespace ([charts/replay](../deploy/charts/replay)) with the image the scorers run. It replaces any earlier replay Job, follows its log, and fails if the Job fails. `make cases` prints the same lines as `uv run python -m mulechain.evaluate workloads/<name> --cases` does offline, so a cluster run is compared with the offline one by `diff` ([evaluation.md](evaluation.md)):

```sh
diff <(make cases) <(uv run python -m mulechain.evaluate workloads/tiny --cases 2>/dev/null) && echo identical
```

Workloads are described in [data.md](data.md).

## Cluster checks

`make replay WORKLOAD=tiny && make cluster-check` runs [test_cluster.py](../deploy/test_cluster.py) against the running cluster, from inside the pods:

- **End to end:** every bank has exactly the cases the offline evaluator finds for `tiny`.
- **NetworkPolicy:** each path a service needs is open, and everything else is closed.
- **mTLS:** which callers a privacy node accepts, what a caller may ask, and that a renewed certificate is served without a restart. This renews bank A's certificate.

## Operations

**Rebuild Valkey from Kafka.** `make rebuild BANK=a` pauses the bank's scorers, empties its Valkey and starts the scorers again, which read the topic from the start. Run it while the bank is idle. A Valkey pod that is lost needs no command: on their next event the scorers notice, restart and rebuild it ([detection.md](detection.md#rebuilding-valkey-from-the-log)).

**Rotate a bank's keys.** `make key-add BANK=b KEY=2`, wait five minutes, `make key-drop BANK=b KEY=1` ([security.md](security.md#key-rotation) says why). `KEY` is the version number; `key-add` refuses a version that exists. Don't restart the bank's privacy node in the two minutes after either step.

**Simulate a bank outage.** `kubectl scale deploy/privacy-node -n bank-c --replicas=0` takes bank C off the network. Searches that need it stay pending and their cases show "Waiting for bank X" until it is scaled back ([protocol.md](protocol.md#failure-handling)). Other faults are injected the same way, with `kubectl delete pod`.

## AWS

The environment is an AWS Academy Learner Lab account, which sets these constraints:

- Only the pre-made `LabRole` exists, and no IAM roles can be created. There is no Pod Identity, so pods that call AWS take `LabRole` from the node's metadata service.
- The lab's credit is fixed and the EKS control plane bills while it exists, so destroy the environment after each run.

| Command | What it does |
| --- | --- |
| `make lambda` | Builds `ingestor.zip` and `summary.zip` into `deploy/terraform/build`: the package plus, for the ingestor, the Kafka client for Lambda's Python 3.12 |
| `make eks` | Builds the zips, applies Terraform, writes `deploy/eks-values.yaml`, creates the workloads bucket if missing, points `kubectl` at the cluster, builds and pushes the image to ECR, then runs `make deploy ENV=eks` |
| `make eks-down` | Deletes the bank namespaces and waits for their volumes to go, then destroys everything Terraform made |

Terraform ([deploy/terraform](../deploy/terraform)) creates, in the default VPC in `us-east-1`:

- an EKS cluster `csc3104` with three `t3a.large` nodes, NetworkPolicy enforcement in the VPC CNI, and the EBS driver for the Kafka and PostgreSQL volumes;
- one ECR repository, `csc3104/mulechain`;
- per bank, an ElastiCache Valkey with a replica in a second zone, in two subnets of its own;
- per bank, an ingestor Lambda inside the VPC and a summary Lambda outside it, each behind a function URL that requires IAM-signed requests.

Its `helm_values` output becomes `deploy/eks-values.yaml`, which helmfile's `eks` environment and the replay chart read: the image repository, the workloads bucket, the ingestor URLs, and each bank's `aws` values.

What differs from k3d:

| | k3d | AWS |
| --- | --- | --- |
| Scorer state | A Valkey pod | The bank's ElastiCache. Inside the bank namespaces only that bank's scorer and privacy node may reach its subnets; what other namespaces can reach is in [security.md](security.md#trust-limits) |
| Ingestor | A pod | A Lambda. It reaches the bank's Kafka through a node-port listener (9094), and its security group opens only that bank's ports |
| Summary service | A pod | A Lambda. The case service signs its calls with `LabRole` ([serverless.py](../src/mulechain/serverless.py)) |
| Replay | Reads `workloads/` from the host, posts to the ingestor Services | An init container copies the workload from the bucket; posts are signed and go to the function URLs |

Upload a workload once with `aws s3 sync workloads/<name> s3://csc3104-workloads-<account-id>/<name>`; the bucket survives `make eks-down`. The ingestor Lambdas are given the nodes' addresses when Terraform applies, so run `make eks` again if the node group replaces its nodes.

## Monitoring

There is no monitoring stack. Look at:

- `kubectl top pod -n bank-a` for CPU and memory;
- `kubectl get hpa -A` for each bank's consumer lag and scorer replicas;
- `kubectl logs`: every service writes one JSON line per event to stdout, with `ts`, `service` and the payment, search or case it concerns;
- CloudWatch for the Lambdas on AWS.

## Things to expect

- A privacy-node or case pod that starts before its bank's PostgreSQL waits for it. A scorer restarts until its Valkey answers (once or twice in a new install, sometimes once when KEDA adds a pod), then waits for the peers' keys and logs `waiting_for_keys_of`.
- A deleted PostgreSQL pod serves its open connections for up to 180 seconds and refuses new ones before it stops (CloudNativePG's [smart shutdown](https://cloudnative-pg.io/documentation/current/instance_manager/)). A service that restarts in that time waits until PostgreSQL is back.
