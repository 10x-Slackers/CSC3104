# Each bank's serverless edge: the ingestor and the summary service as Lambda function URLs with AWS_IAM, run as
# LabRole (docs/contracts/deployment.md). eks-up.sh builds the zips into build/ before applying;
# try() lets eks-down.sh destroy without them.
locals {
  banks = { A = 0, B = 1, C = 2 }
  # Each bank's Kafka VPC listener: bootstrap on this node port, broker n on port + 1 + n (charts/bank/templates/kafka.yaml).
  kafka_port = { for b, i in local.banks : b => 32090 + 10 * i }
}

data "aws_vpc" "default" { default = true }

# Node IPs for the ingestor's Kafka bootstrap: every node forwards the bank's bootstrap node port. Read at apply time,
# so rerun eks-up.sh after the node group replaces its nodes.
data "aws_instances" "nodes" {
  instance_tags = { "eks:nodegroup-name" = aws_eks_node_group.nodes.node_group_name }
  depends_on    = [aws_eks_node_group.nodes] # read after the nodes exist, not at plan time
}

# Only the ingestor joins the VPC, with one security group per bank that reaches only that bank's Kafka node ports.
resource "aws_security_group" "ingestor" {
  for_each = local.banks
  name     = "csc3104-ingestor-${lower(each.key)}"
  vpc_id   = data.aws_vpc.default.id
  egress {
    from_port   = local.kafka_port[each.key]
    to_port     = local.kafka_port[each.key] + 3
    protocol    = "tcp"
    cidr_blocks = [data.aws_vpc.default.cidr_block]
  }
}

resource "aws_vpc_security_group_ingress_rule" "kafka_from_ingestor" {
  for_each                     = local.banks
  security_group_id            = aws_eks_cluster.csc3104.vpc_config[0].cluster_security_group_id # the nodes'
  referenced_security_group_id = aws_security_group.ingestor[each.key].id
  from_port                    = local.kafka_port[each.key]
  to_port                      = local.kafka_port[each.key] + 3
  ip_protocol                  = "tcp"
}

resource "aws_lambda_function" "ingestor" {
  for_each         = local.banks
  function_name    = "csc3104-ingestor-${lower(each.key)}"
  role             = data.aws_iam_role.lab.arn
  runtime          = "python3.12"
  handler          = "ingestor.app.lambda_handler"
  filename         = "build/ingestor.zip"
  source_code_hash = try(filebase64sha256("build/ingestor.zip"), null)
  timeout          = 30 # message.timeout.ms is 25 s
  memory_size      = 256
  vpc_config {
    subnet_ids         = data.aws_subnets.default.ids
    security_group_ids = [aws_security_group.ingestor[each.key].id]
  }
  environment {
    variables = {
      INGESTOR_BANK           = each.key
      KAFKA_BOOTSTRAP_SERVERS = join(",", [for ip in sort(data.aws_instances.nodes.private_ips) : "${ip}:${local.kafka_port[each.key]}"])
    }
  }
}

# Stateless and allowed to fail, so outside the VPC: the case service calls it over the internet with a 2 s timeout.
resource "aws_lambda_function" "summary" {
  for_each         = local.banks
  function_name    = "csc3104-summary-${lower(each.key)}"
  role             = data.aws_iam_role.lab.arn
  runtime          = "python3.12"
  handler          = "summary.app.lambda_handler"
  filename         = "build/summary.zip"
  source_code_hash = try(filebase64sha256("build/summary.zip"), null)
  timeout          = 5
}

resource "aws_lambda_function_url" "ingestor" {
  for_each           = local.banks
  function_name      = aws_lambda_function.ingestor[each.key].function_name
  authorization_type = "AWS_IAM"
}

resource "aws_lambda_function_url" "summary" {
  for_each           = local.banks
  function_name      = aws_lambda_function.summary[each.key].function_name
  authorization_type = "AWS_IAM"
}

# Per-bank chart values for install.sh (--set-json aws=...): the chart drops the in-cluster ingestor, summary and Valkey.
output "bank_values" {
  value = jsonencode({ for b, _ in local.banks : b => {
    summaryUrl    = aws_lambda_function_url.summary[b].function_url
    valkeyUrl     = "valkey://${aws_elasticache_replication_group.valkey[b].primary_endpoint_address}:6379"
    vpcCidr       = data.aws_vpc.default.cidr_block
    valkeyCidrs   = [for z in [0, 1] : aws_subnet.valkey["${b}${z}"].cidr_block]
    kafkaNodePort = local.kafka_port[b]
  } })
}

output "ingestor_urls" { value = { for b, _ in local.banks : b => aws_lambda_function_url.ingestor[b].function_url } }
