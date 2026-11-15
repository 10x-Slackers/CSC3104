#!/usr/bin/env bash
# One-command local setup: k3d cluster and service images, then install.sh (operators, lab CA, keys, one bank release per namespace).
# Needs docker, k3d, kubectl and helm. Expects no existing cluster; teardown: k3d cluster delete csc3104
set -euo pipefail
cd "$(dirname "$0")"

# The fixed subnet matches apiServerCIDRs in charts/bank/values.yaml. Workloads are mounted for replay.sh.
mkdir -p ../apps/dataset/out  # else Docker creates the mount source as root
k3d cluster create csc3104 --subnet 172.28.0.0/16 --wait \
  --volume "$PWD/../apps/dataset/out:/workloads@server:0"
kubectl config use-context k3d-csc3104

# Service images from the root Dockerfile, imported into the cluster (no registry on k3d).
tag=$(git rev-parse --short HEAD 2>/dev/null || echo dev)
for app in ingestor scorer privacy-node dataset; do docker build -q --build-arg APP=$app -t csc3104/$app:$tag .. >/dev/null; done
k3d image import -c csc3104 csc3104/{ingestor,scorer,privacy-node,dataset}:$tag

./install.sh csc3104 "$tag"
