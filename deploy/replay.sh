#!/usr/bin/env bash
# Replays a workload into all three banks' ingestors as a Job in the replay namespace. Contract: docs/contracts/data.md (Replay).
# Usage: deploy/replay.sh <workload> [replay flags, e.g. --speed 0]. Run after k3d-up.sh or eks-up.sh.
# k3d mounts apps/dataset/out/<workload> and sends to the ingestor Services; EKS copies s3://<workloads bucket>/<workload>
# first, as LabRole from the node, and sends to the ingestor Lambdas' function URLs, signed as LabRole (needs jq and
# the Terraform state eks-up.sh left in deploy/terraform).
set -euo pipefail
workload=$1; shift
image=$(kubectl get deploy scorer -n bank-a -o jsonpath='{.spec.template.spec.containers[0].image}')  # what setup built
args=$([ $# -eq 0 ] || printf ', "%s"' "$@")
hardening='{allowPrivilegeEscalation: false, readOnlyRootFilesystem: true, capabilities: {drop: [ALL]}}'
volume='hostPath: {path: /workloads, type: Directory}'
init='[]'
urls=(A=http://ingestor.bank-a:8080 B=http://ingestor.bank-b:8080 C=http://ingestor.bank-c:8080)
if [[ $image == *.dkr.ecr.* ]]; then  # EKS: <account>.dkr.ecr.us-east-1.amazonaws.com/csc3104/scorer:<tag>
  bucket=csc3104-workloads-${image%%.*}
  volume='emptyDir: {}'
  init="[{name: fetch, image: amazon/aws-cli:2.40.1, args: [s3, sync, --only-show-errors, s3://$bucket/$workload, /workloads/$workload],
         volumeMounts: [{name: workloads, mountPath: /workloads}], securityContext: $hardening}]"
  urls=($(terraform -chdir="$(dirname "$0")/terraform" output -json ingestor_urls | jq -r 'to_entries[] | "\(.key)=\(.value)"'))
fi
url_args=$(printf ', --url, "%s"' "${urls[@]}")

kubectl replace --force --cascade=foreground -n replay -f - <<YAML  # the old replay stops first
apiVersion: batch/v1
kind: Job
metadata:
  name: replay
spec:
  backoffLimit: 0  # replay retries 5xx itself; a 400 can't succeed on a rerun
  template:
    spec:
      restartPolicy: Never
      automountServiceAccountToken: false
      securityContext: {runAsNonRoot: true, runAsUser: 65532, runAsGroup: 65532, seccompProfile: {type: RuntimeDefault}}
      initContainers: $init
      containers:
        - name: replay
          image: ${image/scorer:/dataset:}
          args: [python, -m, dataset.replay, /workloads/$workload$url_args$args]
          volumeMounts: [{name: workloads, mountPath: /workloads, readOnly: true}]
          securityContext: $hardening
          resources: {requests: {memory: 64Mi}, limits: {memory: 128Mi}}
      volumes:
        - name: workloads
          $volume
YAML
kubectl logs -n replay -f job/replay --pod-running-timeout=10m  # on EKS, after the S3 copy
kubectl wait -n replay --for=condition=complete job/replay --timeout=10s  # logs -f exits 0 even if replay failed
