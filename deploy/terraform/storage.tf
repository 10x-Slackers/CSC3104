# One image for every service that runs in the cluster; the ingestor and summary Lambdas ship as zips.
resource "aws_ecr_repository" "image" {
  name         = "csc3104/mulechain"
  force_delete = true
}

# The replay workloads bucket is not here: the lab denies s3:GetBucketObjectLockConfiguration, which Terraform's
# bucket resource always reads. `make eks` creates it if missing; it survives destroy, so workloads are uploaded once.
output "workloads_bucket" { value = local.workloads_bucket }
output "image" { value = aws_ecr_repository.image.repository_url }
