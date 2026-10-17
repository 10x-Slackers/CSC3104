#!/usr/bin/env bash
# One-command local setup: k3d cluster, platform operators, lab CA, and one bank release per namespace.
# Needs docker, k3d, kubectl and helm. Teardown: k3d cluster delete csc3104
set -euo pipefail
cd "$(dirname "$0")"

k3d cluster list csc3104 >/dev/null 2>&1 || k3d cluster create csc3104 --wait

helm upgrade --install --wait strimzi oci://quay.io/strimzi-helm/strimzi-kafka-operator --version 1.2.0 \
  -n strimzi --create-namespace --set watchAnyNamespace=true --set generateNetworkPolicy=false
helm upgrade --install --wait cnpg cloudnative-pg --repo https://cloudnative-pg.github.io/charts --version 0.29.1 \
  -n cnpg-system --create-namespace
helm upgrade --install --wait keda keda --repo https://kedacore.github.io/charts --version 2.21.0 \
  -n keda --create-namespace
helm upgrade --install --wait cert-manager oci://quay.io/jetstack/charts/cert-manager --version v1.21.2 \
  -n cert-manager --create-namespace --set crds.enabled=true
kubectl apply -f platform/ca.yaml

for b in a b c; do helm upgrade --install "bank-$b" charts/bank -n "bank-$b" --create-namespace; done
kubectl wait -A --all --for=condition=Ready --timeout=15m kafka,clusters.postgresql.cnpg.io
