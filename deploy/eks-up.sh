#!/usr/bin/env bash
# One-command EKS setup: Terraform (cluster, ECR, ElastiCache, the ingestor and summary Lambdas), the workloads bucket,
# service images pushed to ECR, then install.sh with every bank in HA.
# Needs aws, terraform, docker, kubectl and helm, with the Learner Lab's credentials in ~/.aws/credentials. Teardown: eks-down.sh
set -euo pipefail
cd "$(dirname "$0")"

./lambda-zips.sh
terraform -chdir=terraform init -input=false >/dev/null
terraform -chdir=terraform apply -auto-approve -input=false
registry=$(terraform -chdir=terraform output -raw registry)
bucket=$(terraform -chdir=terraform output -raw workloads_bucket)
aws s3api head-bucket --bucket "$bucket" 2>/dev/null || aws s3 mb "s3://$bucket"  # outside Terraform, survives teardown
aws eks update-kubeconfig --region us-east-1 --name csc3104

# Service images from the root Dockerfile, for the nodes' x86-64.
tag=$(git rev-parse --short HEAD 2>/dev/null || echo dev)
aws ecr get-login-password --region us-east-1 | docker login -u AWS --password-stdin "${registry%%/*}"
for app in scorer privacy-node case dataset; do  # the ingestor and summary service run as Lambdas
  docker build -q --platform linux/amd64 --build-arg APP=$app -t "$registry/$app:$tag" .. >/dev/null
  docker push -q "$registry/$app:$tag"
done

# Bank pods reach the API server through the kubernetes Service and the control plane's ENIs; pods share the VPC, so not all of it.
api=$(kubectl get endpointslice -l kubernetes.io/service-name=kubernetes -o jsonpath='{range .items[*].endpoints[*]}{.addresses[0]}/32,{end}')
HA=abc ./install.sh "$registry" "$tag" --set "apiServerCIDRs={${api}10.100.0.1/32}" \
  --set-json "aws=$(terraform -chdir=terraform output -raw bank_values)"
