resource "aws_ecr_repository" "image" {
  for_each     = toset(["ingestor", "scorer", "privacy-node", "case", "summary", "dataset"])
  name         = "csc3104/${each.key}"
  force_delete = true
}

# The replay workloads bucket is not here: the lab denies s3:GetBucketObjectLockConfiguration, which Terraform's
# bucket resource always reads. eks-up.sh creates it if missing; it survives destroy, so workloads are uploaded once.
output "workloads_bucket" { value = "csc3104-workloads-${data.aws_caller_identity.me.account_id}" }
output "registry" { value = "${data.aws_caller_identity.me.account_id}.dkr.ecr.us-east-1.amazonaws.com/csc3104" }
