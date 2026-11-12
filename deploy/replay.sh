#!/usr/bin/env bash
# Replays a workload into all three banks' ingestors as a Job in the replay namespace. Contract: docs/contracts/data.md (Replay).
# Usage: deploy/replay.sh <workload> [replay flags, e.g. --speed 0]. Run after k3d-up.sh or eks-up.sh.
# k3d mounts apps/dataset/out/<workload>; EKS copies s3://<workloads bucket>/<workload> first, as LabRole from the node.
set -euo pipefail
workload=$1; shift
image=$(kubectl get deploy ingestor -n bank-a -o jsonpath='{.spec.template.spec.containers[0].image}')  # what setup built
args=$([ $# -eq 0 ] || printf ', "%s"' "$@")
hardening='{allowPrivilegeEscalation: false, readOnlyRootFilesystem: true, capabilities: {drop: [ALL]}}'
volume='hostPath: {path: /workloads, type: Directory}'
init='[]'
if [[ $(kubectl config current-context) == arn:aws:eks:* ]]; then
  bucket=csc3104-workloads-$(aws sts get-caller-identity --query Account --output text)
  volume='emptyDir: {}'
  init="[{name: fetch, image: amazon/aws-cli:2.40.1, args: [s3, sync, --only-show-errors, s3://$bucket/$workload, /workloads/$workload],
         volumeMounts: [{name: workloads, mountPath: /workloads}], securityContext: $hardening}]"
fi

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
          image: ${image%/ingestor:*}/dataset:${image##*:}
          args: [python, -m, dataset.replay, /workloads/$workload,
                 --url, "A=http://ingestor.bank-a:8080", --url, "B=http://ingestor.bank-b:8080", --url, "C=http://ingestor.bank-c:8080"$args]
          volumeMounts: [{name: workloads, mountPath: /workloads, readOnly: true}]
          securityContext: $hardening
          resources: {requests: {memory: 64Mi}, limits: {memory: 128Mi}}
      volumes:
        - name: workloads
          $volume
YAML
kubectl logs -n replay -f job/replay --pod-running-timeout=2m
kubectl wait -n replay --for=condition=complete job/replay --timeout=10s  # logs -f exits 0 even if replay failed
