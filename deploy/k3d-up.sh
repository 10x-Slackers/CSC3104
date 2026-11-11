#!/usr/bin/env bash
# One-command local setup: k3d cluster, platform operators, lab CA, service images, lab keys, and one bank release per namespace.
# Needs docker, k3d, kubectl and helm. Expects no existing cluster; teardown: k3d cluster delete csc3104
set -euo pipefail
cd "$(dirname "$0")"

# The fixed subnet matches apiServerCIDRs in charts/bank/values.yaml. Workloads are mounted for replay.sh.
mkdir -p ../apps/dataset/out  # else Docker creates the mount source as root
k3d cluster create csc3104 --subnet 172.28.0.0/16 --wait \
  --volume "$PWD/../apps/dataset/out:/workloads@server:0"
kubectl config use-context k3d-csc3104

helm install --wait strimzi oci://quay.io/strimzi-helm/strimzi-kafka-operator --version 1.2.0 \
  -n strimzi --create-namespace --set watchAnyNamespace=true --set generateNetworkPolicy=false
helm install --wait cnpg cloudnative-pg --repo https://cloudnative-pg.github.io/charts --version 0.29.1 \
  -n cnpg-system --create-namespace
helm install --wait keda keda --repo https://kedacore.github.io/charts --version 2.21.0 \
  -n keda --create-namespace
helm install --wait cert-manager oci://quay.io/jetstack/charts/cert-manager --version v1.21.2 \
  -n cert-manager --create-namespace --set crds.enabled=true
kubectl apply -f platform/ca.yaml

# Service images from the root Dockerfile, imported into the cluster (no registry on k3d).
tag=$(git rev-parse --short HEAD 2>/dev/null || echo dev)
for app in ingestor scorer privacy-node dataset; do docker build -q --build-arg APP=$app -t csc3104/$app:$tag .. >/dev/null; done
k3d image import -c csc3104 csc3104/{ingestor,scorer,privacy-node,dataset}:$tag

# Fresh lab keys for all three banks; each release packs only its own bank's tree.
rm -rf charts/bank/keys
docker run --rm --user "$(id -u):$(id -g)" -v "$PWD/charts/bank:/chart" csc3104/privacy-node:$tag \
  python -c "from privacy_node.demo import prepare_demo_keys; prepare_demo_keys('/chart/keys')"

kubectl create namespace replay
for b in a b c; do
  helm install "bank-$b" charts/bank -n "bank-$b" --create-namespace --set bank=${b^^} --set image.tag=$tag
done
kubectl wait -A --all --for=condition=Ready --timeout=15m kafka,clusters.postgresql.cnpg.io,certificates.cert-manager.io
kubectl wait -A -l 'app in (ingestor,scorer,privacy-node)' --for=condition=Ready --timeout=5m pod
