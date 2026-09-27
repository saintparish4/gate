output "load_balancer_dns" {
  description = "DNS name of the load balancer"
  value       = module.ecs.load_balancer_dns
}

output "api_endpoint" {
  description = "API endpoint URL. With a certificate, use your domain, which the certificate covers, rather than the ALB name."
  value       = "${var.certificate_arn != "" ? "https" : "http"}://${module.ecs.load_balancer_dns}"
}

output "dynamodb_rate_limit_table" {
  description = "DynamoDB rate limit table name"
  value       = module.dynamodb.rate_limit_table_name
}

output "dynamodb_idempotency_table" {
  description = "DynamoDB idempotency table name"
  value       = module.dynamodb.idempotency_table_name
}

output "kinesis_stream_name" {
  description = "Kinesis stream name"
  value       = module.kinesis.stream_name
}

output "ecs_cluster_name" {
  description = "ECS cluster name"
  value       = module.ecs.cluster_name
}

output "vpc_id" {
  description = "VPC ID"
  value       = module.networking.vpc_id
}

output "cloudwatch_dashboard_url" {
  description = "CloudWatch dashboard URL"
  value       = module.monitoring.dashboard_url
}

output "api_keys_secret_name" {
  description = "Secrets Manager secret name for API keys"
  value       = module.secrets.api_keys_secret_name
}
