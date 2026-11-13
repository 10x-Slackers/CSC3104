#!/usr/bin/env bash
# EKS teardown. EBS volumes are deleted only while the cluster runs, so the bank namespaces go first, and their topics before
# them (each bank's own topic operator clears the topic finalizer, and deleting the namespace would stop it first).
# Needs aws, kubectl and terraform, with the Learner Lab's credentials in ~/.aws/credentials.
set -euo pipefail
cd "$(dirname "$0")"

aws eks update-kubeconfig --region us-east-1 --name csc3104
kubectl delete kafkatopics -A --all
kubectl delete namespace bank-a bank-b bank-c --ignore-not-found
kubectl wait --for=delete pv --all --timeout=5m
terraform -chdir=terraform destroy -auto-approve
