data "aws_subnets" "default" {
  filter {
    name   = "default-for-az"
    values = ["true"]
  }
  filter {
    name   = "availability-zone-id" # EKS can't use use1-az3; zone names differ per account
    values = ["use1-az1", "use1-az2", "use1-az6"]
  }
}

resource "aws_eks_cluster" "csc3104" {
  name     = "csc3104"
  role_arn = data.aws_iam_role.lab.arn
  vpc_config { subnet_ids = data.aws_subnets.default.ids }
}

# Hop limit 2 lets pods take LabRole from the node's metadata service: the lab has no Pod Identity, and the
# EBS driver and the replay Job need AWS credentials. Bank pods can't reach it (default-deny NetworkPolicy).
resource "aws_launch_template" "nodes" {
  name = "csc3104-nodes"
  metadata_options {
    http_tokens                 = "required"
    http_put_response_hop_limit = 2
  }
}

# Three t3a.large (the lab allows nothing bigger) across three zones hold all three banks with replicas.
resource "aws_eks_node_group" "nodes" {
  cluster_name    = aws_eks_cluster.csc3104.name
  node_group_name = "nodes"
  node_role_arn   = data.aws_iam_role.lab.arn
  subnet_ids      = data.aws_subnets.default.ids
  instance_types  = ["t3a.large"]
  launch_template {
    id      = aws_launch_template.nodes.id
    version = aws_launch_template.nodes.latest_version
  }
  scaling_config {
    desired_size = 3
    min_size     = 3
    max_size     = 3
  }
}

resource "aws_eks_addon" "vpc_cni" {
  cluster_name                = aws_eks_cluster.csc3104.name
  addon_name                  = "vpc-cni"
  resolve_conflicts_on_create = "OVERWRITE" # replaces the copy every new cluster starts with
  configuration_values        = jsonencode({ enableNetworkPolicy = "true" })
}

# Provisions the Kafka and PostgreSQL volumes.
resource "aws_eks_addon" "ebs_csi" {
  cluster_name = aws_eks_cluster.csc3104.name
  addon_name   = "aws-ebs-csi-driver"
  depends_on   = [aws_eks_node_group.nodes]
}

output "kubeconfig_command" { value = "aws eks update-kubeconfig --region us-east-1 --name ${aws_eks_cluster.csc3104.name}" }
