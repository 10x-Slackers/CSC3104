#!/usr/bin/env bash
# Installs onto the current cluster: platform operators, lab CA, lab keys, the replay namespace and one bank release per namespace.
# Usage: [HA=<banks, e.g. abc>] install.sh <image repository> <tag> [helm flags for every bank]. Called by k3d-up.sh and
# eks-up.sh once the images are in place; HA lists the banks run with replicas (the chart's ha flag).
# Safe to rerun, so a failed eks-up.sh can be run again to resume.
set -euo pipefail
cd "$(dirname "$0")"
repo=$1 tag=$2; shift 2

helm upgrade --install --wait strimzi oci://quay.io/strimzi-helm/strimzi-kafka-operator --version 1.2.0 \
  -n strimzi --create-namespace --set watchAnyNamespace=true --set generateNetworkPolicy=false \
  --set-json 'extraEnvs=[{"name": "STRIMZI_POD_SECURITY_PROVIDER_CLASS", "value": "restricted"}]'  # Kafka pods pass PSS
helm upgrade --install --wait cnpg cloudnative-pg --repo https://cloudnative-pg.github.io/charts --version 0.29.1 \
  -n cnpg-system --create-namespace
helm upgrade --install --wait keda keda --repo https://kedacore.github.io/charts --version 2.21.0 \
  -n keda --create-namespace
helm upgrade --install --wait cert-manager oci://quay.io/jetstack/charts/cert-manager --version v1.21.2 \
  -n cert-manager --create-namespace --set crds.enabled=true
kubectl apply -f platform/ca.yaml

# Lab keys for all three banks, made once and kept so a rerun doesn't swap them under running pods.
# Each release packs only its own bank's tree.
[ -d charts/bank/keys ] || docker run --rm --user "$(id -u):$(id -g)" -v "$PWD/charts/bank:/chart" "$repo/privacy-node:$tag" \
  python -c "from privacy_node.demo import prepare_demo_keys; prepare_demo_keys('/chart/keys')"

# Bank pods may reach the API server only: the kubernetes Service and the addresses behind it (k3s's server node,
# EKS's control-plane ENIs), not the node subnet or VPC around them.
api=$(kubectl get endpointslice -l kubernetes.io/service-name=kubernetes \
  -o jsonpath='{range .items[*].endpoints[*]}{.addresses[0]}/32,{end}')$(kubectl get svc kubernetes -o jsonpath='{.spec.clusterIP}')/32

kubectl create namespace replay --dry-run=client -o yaml | kubectl apply -f -
for b in a b c; do  # PSS rejects a bank pod that breaks the restricted profile; warn makes helm say so
  kubectl create namespace "bank-$b" --dry-run=client -o yaml | kubectl apply -f -
  kubectl label namespace "bank-$b" --overwrite pod-security.kubernetes.io/{enforce,warn}=restricted
  helm upgrade --install "bank-$b" charts/bank -n "bank-$b" \
    --set bank=${b^^} --set ha=$([[ ${HA:-} == *$b* ]] && echo true || echo false) \
    --set image.repository="$repo" --set image.tag="$tag" --set "apiServerCIDRs={$api}" "$@"
done
kubectl wait -A --all --for=condition=Ready --timeout=15m kafka,clusters.postgresql.cnpg.io,certificates.cert-manager.io
kubectl wait -A -l 'app in (ingestor,scorer,privacy-node,case,summary)' --for=condition=Ready --timeout=5m pod
