# AWS side of the EKS environment, sized for an AWS Academy Learner Lab account: the pre-made LabRole everywhere
# (the lab can't create IAM roles), the default VPC, us-east-1, and at most 9 instances of size large or smaller.
# `make eks` applies it and `make eks-down` destroys it (docs/deployment.md). State stays local.
terraform {
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 6.0" }
  }
}

provider "aws" { region = "us-east-1" }

data "aws_caller_identity" "me" {}
data "aws_iam_role" "lab" { name = "LabRole" }
