# Each bank's Valkey on ElastiCache, Multi-AZ: a primary and a replica in another zone with automatic failover.
# Derived state rebuilt from Kafka, so no snapshots, auth or TLS (docs/contracts/deployment.md).
# Each bank gets two small subnets of its own (one per zone, above the default subnets' 172.31.0.0-95.255), so the
# chart's valkey-clients policy can let a bank's pods reach only its own ElastiCache.
resource "aws_subnet" "valkey" {
  for_each             = { for p in setproduct(keys(local.banks), [0, 1]) : join("", p) => p } # A0, A1, B0, ...
  vpc_id               = data.aws_vpc.default.id
  availability_zone_id = ["use1-az1", "use1-az2"][each.value[1]]
  cidr_block           = cidrsubnet(data.aws_vpc.default.cidr_block, 12, 2048 + 2 * local.banks[each.value[0]] + each.value[1])
}

resource "aws_elasticache_subnet_group" "valkey" {
  for_each   = local.banks
  name       = "csc3104-valkey-${lower(each.key)}"
  subnet_ids = [for z in [0, 1] : aws_subnet.valkey["${each.key}${z}"].id]
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
  subnet_group_name          = aws_elasticache_subnet_group.valkey[each.key].name
  security_group_ids         = [aws_eks_cluster.csc3104.vpc_config[0].cluster_security_group_id] # pods use the nodes' group
}
