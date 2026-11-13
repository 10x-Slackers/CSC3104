#!/usr/bin/env bash
# Installs onto the current cluster: platform operators, lab CA, fresh lab keys, the replay namespace and one bank release per namespace.
# Usage: install.sh <image repository> <tag> [helm flags for every bank]. Called by k3d-up.sh and eks-up.sh once the images are in place.
# Safe to rerun, so a failed eks-up.sh can be run again to resume.
set -euo pipefail
cd "$(dirname "$0")"
repo=$1 tag=$2; shift 2

helm upgrade --install --wait strimzi oci://quay.io/strimzi-helm/strimzi-kafka-operator --version 1.2.0 \
  -n strimzi --create-namespace --set watchAnyNamespace=true --set generateNetworkPolicy=false
helm upgrade --install --wait cnpg cloudnative-pg --repo https://cloudnative-pg.github.io/charts --version 0.29.1 \
  -n cnpg-system --create-namespace
helm upgrade --install --wait keda keda --repo https://kedacore.github.io/charts --version 2.21.0 \
  -n keda --create-namespace
helm upgrade --install --wait cert-manager oci://quay.io/jetstack/charts/cert-manager --version v1.21.2 \
  -n cert-manager --create-namespace --set crds.enabled=true
kubectl apply -f platform/ca.yaml

# Fresh lab keys for all three banks; each release packs only its own bank's tree.
rm -rf charts/bank/keys
docker run --rm --user "$(id -u):$(id -g)" -v "$PWD/charts/bank:/chart" "$repo/privacy-node:$tag" \
  python -c "from privacy_node.demo import prepare_demo_keys; prepare_demo_keys('/chart/keys')"

kubectl create namespace replay --dry-run=client -o yaml | kubectl apply -f -
for b in a b c; do
  helm upgrade --install "bank-$b" charts/bank -n "bank-$b" --create-namespace \
    --set bank=${b^^} --set image.repository="$repo" --set image.tag="$tag" "$@"
done
kubectl wait -A --all --for=condition=Ready --timeout=15m kafka,clusters.postgresql.cnpg.io,certificates.cert-manager.io
kubectl wait -A -l 'app in (ingestor,scorer,privacy-node)' --for=condition=Ready --timeout=5m pod
