#!/usr/bin/env bash
# One-command EKS setup: Terraform (cluster, ECR), the workloads bucket, service images pushed to ECR, then install.sh.
# Needs aws, terraform, docker, kubectl and helm, with the Learner Lab's credentials in ~/.aws/credentials. Teardown: eks-down.sh
set -euo pipefail
cd "$(dirname "$0")"

terraform -chdir=terraform init -input=false >/dev/null
terraform -chdir=terraform apply -auto-approve -input=false
registry=$(terraform -chdir=terraform output -raw registry)
bucket=$(terraform -chdir=terraform output -raw workloads_bucket)
aws s3api head-bucket --bucket "$bucket" 2>/dev/null || aws s3 mb "s3://$bucket"  # outside Terraform, survives teardown
aws eks update-kubeconfig --region us-east-1 --name csc3104

# Service images from the root Dockerfile, for the nodes' x86-64.
tag=$(git rev-parse --short HEAD 2>/dev/null || echo dev)
aws ecr get-login-password --region us-east-1 | docker login -u AWS --password-stdin "${registry%%/*}"
for app in ingestor scorer privacy-node dataset; do
  docker build -q --platform linux/amd64 --build-arg APP=$app -t "$registry/$app:$tag" .. >/dev/null
  docker push -q "$registry/$app:$tag"
done

# Bank pods reach the API server through the kubernetes Service (10.100.0.1) and the control plane's ENIs in the default VPC.
./install.sh "$registry" "$tag" --set-json 'apiServerCIDRs=["172.31.0.0/16","10.100.0.1/32"]'
