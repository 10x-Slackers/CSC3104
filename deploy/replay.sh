#!/usr/bin/env bash
# Replays a workload from apps/dataset/out/ into all three banks' ingestors as a Job in the replay namespace.
# Usage: deploy/replay.sh <workload> [replay flags, e.g. --speed 0]. Run after k3d-up.sh. Contract: docs/contracts/data.md (Replay).
set -euo pipefail
workload=$1; shift
tag=$(git -C "$(dirname "$0")" rev-parse --short HEAD 2>/dev/null || echo dev)
args=$(printf ', "%s"' "$@")

kubectl delete job replay -n replay --ignore-not-found
kubectl apply -n replay -f - <<YAML
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
      containers:
        - name: replay
          image: csc3104/dataset:$tag
          args: [python, -m, dataset.replay, /workloads/$workload,
                 --url, "A=http://ingestor.bank-a:8080", --url, "B=http://ingestor.bank-b:8080", --url, "C=http://ingestor.bank-c:8080"$args]
          volumeMounts: [{name: workloads, mountPath: /workloads, readOnly: true}]
          securityContext: {allowPrivilegeEscalation: false, readOnlyRootFilesystem: true, capabilities: {drop: [ALL]}}
          resources: {requests: {memory: 64Mi}, limits: {memory: 128Mi}}
      volumes:
        - name: workloads
          hostPath: {path: /workloads, type: Directory}
YAML
kubectl wait -n replay --for=condition=Ready pod -l job-name=replay --timeout=2m
kubectl logs -n replay -f job/replay
