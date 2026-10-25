#!/usr/bin/env bash
# Role 3: build the privacy node image, load lab keys into each bank, and enable the node.
# Run after deploy/k3d-up.sh. Keys go to the git-ignored .secrets/ folder.
set -euo pipefail
cd "$(dirname "$0")/.."

docker build -f apps/privacy_node/Dockerfile -t privacy-node:dev .
k3d image import privacy-node:dev -c csc3104
[ -d .secrets/pairs ] || uv run python -m apps.privacy_node.keys .secrets

for b in A B C; do
  ns="bank-$(echo "$b" | tr A-Z a-z)"  # macOS bash 3.2 has no ${b,,}
  pairs=()
  for p in .secrets/pairs/*.key; do [[ "$(basename "$p")" == *"$b"* ]] && pairs+=(--from-file="$p"); done
  kubectl -n "$ns" create secret generic privacy-node-keys "${pairs[@]}" \
    --from-file=signing.pem=".secrets/signing/$b.pem" --dry-run=client -o yaml | kubectl apply -f -
  kubectl -n "$ns" create configmap bank-public-keys --from-file=.secrets/public \
    --dry-run=client -o yaml | kubectl apply -f -
  helm upgrade --install "$ns" deploy/charts/bank -n "$ns" --set privacyNode.enabled=true
done
kubectl wait -n bank-a --for=condition=Available deployment/privacy-node --timeout=3m
echo "Trace output: kubectl -n bank-b logs deploy/privacy-node"
