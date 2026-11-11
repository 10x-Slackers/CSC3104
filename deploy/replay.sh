#!/usr/bin/env bash
# Replays a workload from apps/dataset/out/ into all three banks' ingestors as a Job in the replay namespace.
# Usage: deploy/replay.sh <workload> [replay flags, e.g. --speed 0]. Run after k3d-up.sh. Contract: docs/contracts/data.md (Replay).
set -euo pipefail
workload=$1; shift
image=$(kubectl get deploy ingestor -n bank-a -o jsonpath='{.spec.template.spec.containers[0].image}')  # what k3d-up.sh imported
args=$([ $# -eq 0 ] || printf ', "%s"' "$@")

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
      containers:
        - name: replay
          image: csc3104/dataset:${image##*:}
          args: [python, -m, dataset.replay, /workloads/$workload,
                 --url, "A=http://ingestor.bank-a:8080", --url, "B=http://ingestor.bank-b:8080", --url, "C=http://ingestor.bank-c:8080"$args]
          volumeMounts: [{name: workloads, mountPath: /workloads, readOnly: true}]
          securityContext: {allowPrivilegeEscalation: false, readOnlyRootFilesystem: true, capabilities: {drop: [ALL]}}
          resources: {requests: {memory: 64Mi}, limits: {memory: 128Mi}}
      volumes:
        - name: workloads
          hostPath: {path: /workloads, type: Directory}
YAML
kubectl logs -n replay -f job/replay --pod-running-timeout=2m
kubectl wait -n replay --for=condition=complete job/replay --timeout=10s  # logs -f exits 0 even if replay failed
