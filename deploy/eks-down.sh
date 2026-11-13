#!/usr/bin/env bash
# EKS teardown. EBS volumes are deleted only while the cluster runs, so the bank namespaces go first, and their topics before
# them (each bank's own topic operator clears the topic finalizer, and deleting the namespace would stop it first).
# Needs aws, kubectl and terraform, with the Learner Lab's credentials in ~/.aws/credentials.
set -euo pipefail
cd "$(dirname "$0")"

if aws eks update-kubeconfig --region us-east-1 --name csc3104; then  # no cluster if setup failed early: just destroy
  kubectl delete kafkatopics -A --all || true  # no such type if setup stopped before Strimzi
  kubectl delete namespace bank-a bank-b bank-c --ignore-not-found
  kubectl wait --for=delete pv --all --timeout=5m
fi
terraform -chdir=terraform destroy -auto-approve
