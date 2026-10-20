# Gate on EKS: the cluster, its nodes, the AWS state Gate needs, and the IAM
# each pod gets through EKS Pod Identity. A separate root from the ECS one
# (terraform/main.tf), with its own state key and environment name, so the two
# can run side by side and never share a table. ADR-008 says why there are two.
#
# Prepared, not applied: scripts/deploy-eks.sh applies it and installs the
# chart. `make tf-validate` checks it without AWS.

data "aws_caller_identity" "current" {}

locals {
  name = "${var.project_name}-${var.environment}"
  # The service accounts the chart creates (deploy/helm/gate/values.yaml).
  namespace = "gate"
}

module "networking" {
  source = "../modules/networking"

  project_name    = var.project_name
  environment     = var.environment
  vpc_cidr        = var.vpc_cidr
  azs             = var.availability_zones
  private_subnets = var.private_subnet_cidrs
  public_subnets  = var.public_subnet_cidrs
}

# The AWS Load Balancer Controller finds subnets by these tags. The shared
# networking module does not set them, because the ECS root names its subnets
# directly; tagging here leaves that module and its plan untouched.
resource "aws_ec2_tag" "public_elb" {
  count       = length(var.public_subnet_cidrs)
  resource_id = module.networking.public_subnet_ids[count.index]
  key         = "kubernetes.io/role/elb"
  value       = "1"
}

resource "aws_ec2_tag" "private_internal_elb" {
  count       = length(var.private_subnet_cidrs)
  resource_id = module.networking.private_subnet_ids[count.index]
  key         = "kubernetes.io/role/internal-elb"
  value       = "1"
}

module "dynamodb" {
  source = "../modules/dynamodb"

  project_name             = var.project_name
  environment              = var.environment
  rate_limit_table_config  = { billing_mode = "PAY_PER_REQUEST" }
  idempotency_table_config = { billing_mode = "PAY_PER_REQUEST" }
  token_quota_table_config = { billing_mode = "PAY_PER_REQUEST" }
}

module "kinesis" {
  source = "../modules/kinesis"

  project_name    = var.project_name
  environment     = var.environment
  shard_count     = var.kinesis_shard_count
  retention_hours = var.kinesis_retention_hours
}

module "secrets" {
  source = "../modules/secrets"

  project_name = var.project_name
  environment  = var.environment
}

# The usage aggregator's sums and per-shard checkpoints (ADR-007). One table,
# so a commit's sums and its checkpoint fit one transaction.
resource "aws_dynamodb_table" "usage" {
  name         = "${local.name}-usage"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"
  range_key    = "sk"

  attribute {
    name = "pk"
    type = "S"
  }

  attribute {
    name = "sk"
    type = "S"
  }

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  point_in_time_recovery {
    enabled = true
  }

  server_side_encryption {
    enabled = true
  }
}
