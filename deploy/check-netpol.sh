#!/usr/bin/env bash
# Security review probes. Run after k3d-up.sh; requires permission to create test pods.
# Each blocked destination is shown OPEN first. App probes use temporary TCP listeners
# (not gRPC/storage tests), so they also work before Isaac adds the app Deployments.
# A deliberately-unready gate keeps test pods OUT of real Service endpoints.
set -uo pipefail

failed=0
run_id=$RANDOM
listeners=()
not_ready='{"spec":{"automountServiceAccountToken":false,"readinessGates":[{"conditionType":"csc3104.test/never-ready"}]}}'
cleanup() {
  for item in "${listeners[@]}"; do
    kubectl delete pod -n "${item%/*}" "${item#*/}" --ignore-not-found --wait=false >/dev/null
  done
}
trap cleanup EXIT

listener() { # namespace app port; sets host to a temporary listener's Pod IP
  local namespace=$1 app=$2 port=$3 name="netpol-$2-$run_id"
  listeners+=("$namespace/$name")
  kubectl run "$name" -n "$namespace" --quiet --restart=Never --image=busybox:1.37 \
    --labels="app=$app" --overrides="$not_ready" --command -- \
    sh -c "while true; do nc -l -p $port -e cat; done" >/dev/null
  kubectl wait pod/"$name" -n "$namespace" --for=jsonpath='{.status.phase}'=Running --timeout=90s >/dev/null
  host=$(kubectl get pod "$name" -n "$namespace" -o jsonpath='{.status.podIP}')
  [ -n "$host" ] || { echo "FAIL listener $namespace/$app"; exit 1; }
}

probe() { # namespace source-labels host:port expect: open|blocked
  local name="probe-$run_id-$RANDOM" got result
  got=$(kubectl run "$name" -n "$1" --rm -i --quiet --restart=Never --image=busybox:1.37 \
    --labels="$2" --overrides="$not_ready" --command -- \
    sh -c "sleep 10; nc -z -w 3 ${3%:*} ${3#*:} && echo open || echo blocked" 2>/dev/null)
  [ "$got" = "$4" ] && result=PASS || { result=FAIL; failed=1; }
  echo "$result  $1 ($2) -> $3: ${got:-probe failed} (expected $4)"
}

listener bank-b privacy-node 50051; peer=$host:50051
probe bank-a app=privacy-node "$peer" open
probe bank-a app=scorer "$peer" blocked
probe default app=privacy-node "$peer" blocked

probe bank-a app=privacy-node postgres-rw.bank-a:5432 open
probe bank-a app=case postgres-rw.bank-a:5432 open
probe bank-a app=scorer postgres-rw.bank-a:5432 blocked
probe bank-b app=privacy-node postgres-rw.bank-a:5432 blocked
probe default app=case postgres-rw.bank-a:5432 blocked

probe bank-a app=ingestor kafka-kafka-bootstrap.bank-a:9092 open
probe bank-a app=scorer kafka-kafka-bootstrap.bank-a:9092 open
probe bank-a app=privacy-node kafka-kafka-bootstrap.bank-a:9092 open
probe bank-a app=case kafka-kafka-bootstrap.bank-a:9092 blocked
probe bank-b app=scorer kafka-kafka-bootstrap.bank-a:9092 blocked
probe default app=ingestor kafka-kafka-bootstrap.bank-a:9092 blocked

listener bank-a valkey 6379; valkey=$host:6379
probe bank-a app=scorer "$valkey" open
probe bank-a app=privacy-node "$valkey" open
probe bank-a app=case "$valkey" blocked
probe bank-a app=ingestor "$valkey" blocked
probe bank-b app=privacy-node "$valkey" blocked
probe default app=privacy-node "$valkey" blocked

listener bank-a case 8000; case_service=$host:8000
probe bank-a app=privacy-node "$case_service" open
probe bank-a app=scorer "$case_service" blocked
probe bank-b app=privacy-node "$case_service" blocked
probe default app=privacy-node "$case_service" blocked

listener bank-a intake 8001; intake=$host:8001
probe bank-a app=case "$intake" open
probe bank-a app=privacy-node "$intake" blocked
probe bank-b app=case "$intake" blocked

probe bank-a cnpg.io/cluster=postgres kubernetes.default:443 open
probe bank-a app=privacy-node kubernetes.default:443 blocked
exit "$failed"
