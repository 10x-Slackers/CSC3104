# Each bank's Valkey on ElastiCache, Multi-AZ: a primary and a replica in another zone with automatic failover.
# Derived state rebuilt from Kafka, so no snapshots, auth or TLS (docs/contracts/deployment.md).
resource "aws_elasticache_subnet_group" "valkey" {
  name       = "csc3104-valkey"
  subnet_ids = data.aws_subnets.default.ids
}

resource "aws_elasticache_replication_group" "valkey" {
  for_each                   = local.banks
  replication_group_id       = "csc3104-valkey-${lower(each.key)}"
  description                = "Bank ${each.key} scorer state"
  engine                     = "valkey"
  node_type                  = "cache.t3.medium"
  num_cache_clusters         = 2
  automatic_failover_enabled = true
  multi_az_enabled           = true
  transit_encryption_enabled = false
  subnet_group_name          = aws_elasticache_subnet_group.valkey.name
  security_group_ids         = [aws_eks_cluster.csc3104.vpc_config[0].cluster_security_group_id] # pods use the nodes' group
}
