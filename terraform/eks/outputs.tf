output "cluster_name" {
  value = aws_eks_cluster.main.name
}

output "region" {
  value = var.aws_region
}

output "vpc_id" {
  description = "The AWS Load Balancer Controller's --set vpcId"
  value       = module.networking.vpc_id
}

output "api_keys_secret_name" {
  value = module.secrets.api_keys_secret_name
}

output "usage_table_name" {
  value = aws_dynamodb_table.usage.name
}

# Everything the chart needs from this root, so deploy-eks.sh never copies a
# table name by hand: `terraform output -raw helm_values > values-eks.yaml`.
output "helm_values" {
  value = yamlencode({
    gate = {
      image = {
        repository = var.gate_image == "" ? "" : split(":", var.gate_image)[0]
        tag        = var.gate_image == "" ? "" : split(":", var.gate_image)[1]
      }
      env = {
        AWS_REGION          = var.aws_region
        RATE_LIMIT_TABLE    = module.dynamodb.rate_limit_table_name
        IDEMPOTENCY_TABLE   = module.dynamodb.idempotency_table_name
        TOKEN_QUOTA_TABLE   = module.dynamodb.token_quota_table_name
        KINESIS_STREAM      = module.kinesis.stream_name
        METRICS_NAMESPACE   = "RateLimiter/${var.environment}"
        METRICS_ENVIRONMENT = var.environment
        SECRETS_PREFIX      = var.project_name
        SECRETS_ENVIRONMENT = var.environment
      }
    }
    aggregator = {
      image = {
        repository = var.aggregator_image == "" ? "" : split(":", var.aggregator_image)[0]
        tag        = var.aggregator_image == "" ? "" : split(":", var.aggregator_image)[1]
      }
      env = {
        AWS_REGION     = var.aws_region
        KINESIS_STREAM = module.kinesis.stream_name
        USAGE_TABLE    = aws_dynamodb_table.usage.name
        USAGE_TTL_DAYS = tostring(var.usage_retention_days)
      }
    }
    ingress = {
      enabled        = true
      certificateArn = var.certificate_arn
      inboundCidrs   = var.certificate_arn == "" ? var.alb_ingress_cidrs : ["0.0.0.0/0"]
    }
  })
}
