# AWS side of the EKS environment, sized for an AWS Academy Learner Lab account: the pre-made LabRole everywhere
# (the lab can't create IAM roles), the default VPC, us-east-1, and at most 9 instances of size large or smaller.
# Usage: paste the lab's credentials into ~/.aws/credentials, then terraform -chdir=deploy/terraform init && ... apply.
# Teardown: terraform -chdir=deploy/terraform destroy. State stays local (gitignored).
terraform {
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 6.0" }
  }
}

provider "aws" {
  region = "us-east-1"
}

data "aws_caller_identity" "me" {}
data "aws_iam_role" "lab" { name = "LabRole" }
