terraform {
  required_version = ">= 1.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Remote state configuration - uncomment and configure for production
  # backend "s3" {
  #   bucket         = "your-terraform-state-bucket"
  #   key            = "rate-limiter/terraform.tfstate"
  #   region         = "us-east-1"
  #   dynamodb_table = "terraform-locks"
  #   encrypt        = true
  # }
}

locals {
  # Only enable tracing when there is somewhere to send it.
  tracing_enabled = var.otel_exporter_otlp_endpoint != ""
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}

# Data sources
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# -----------------------------------------------------------------------------
# Production-like infrastructure
# Provisions: DynamoDB (rate limit + idempotency), Kinesis stream,
#             ECS Fargate service, ALB in front of Fargate
# -----------------------------------------------------------------------------

# Networking
module "networking" {
  source = "./modules/networking"

  project_name     = var.project_name
  environment      = var.environment
  vpc_cidr         = var.vpc_cidr
  azs              = var.availability_zones
  private_subnets  = var.private_subnet_cidrs
  public_subnets   = var.public_subnet_cidrs
}

# DynamoDB Tables
module "dynamodb" {
  source = "./modules/dynamodb"

  project_name = var.project_name
  environment  = var.environment

  rate_limit_table_config = {
    billing_mode   = var.dynamodb_billing_mode
    read_capacity  = var.dynamodb_read_capacity
    write_capacity = var.dynamodb_write_capacity
  }

  idempotency_table_config = {
    billing_mode   = var.dynamodb_billing_mode
    read_capacity  = var.dynamodb_read_capacity
    write_capacity = var.dynamodb_write_capacity
  }

  token_quota_table_config = {
    billing_mode   = var.dynamodb_billing_mode
    read_capacity  = var.dynamodb_read_capacity
    write_capacity = var.dynamodb_write_capacity
  }
}

# Kinesis Streams
module "kinesis" {
  source = "./modules/kinesis"

  project_name    = var.project_name
  environment     = var.environment
  shard_count     = var.kinesis_shard_count
  retention_hours = var.kinesis_retention_hours

  enable_firehose         = var.enable_kinesis_firehose
  s3_bucket_name          = var.kinesis_s3_bucket
  enable_audit_compliance = var.enable_audit_compliance
}

# Secrets Manager
module "secrets" {
  source = "./modules/secrets"

  project_name = var.project_name
  environment  = var.environment
}

# ECS Cluster and Service
module "ecs" {
  source = "./modules/ecs"

  project_name = var.project_name
  environment  = var.environment

  vpc_id             = module.networking.vpc_id
  private_subnet_ids = module.networking.private_subnet_ids
  public_subnet_ids  = module.networking.public_subnet_ids

  container_image = var.container_image
  container_port  = var.container_port
  desired_count   = var.ecs_desired_count
  cpu             = var.ecs_cpu
  memory          = var.ecs_memory

  # Environment variables for the container
  environment_variables = {
    # Pinned so the bind address and port cannot silently disagree with the
    # task definition, health check, target group and listener -- all of which
    # derive from var.container_port. The application defaults (0.0.0.0:8080)
    # happen to match today, so a mismatch would only surface as failing health
    # checks with no stated cause.
    SERVER_HOST = "0.0.0.0"
    SERVER_PORT = tostring(var.container_port)

    AWS_REGION        = var.aws_region
    RATE_LIMIT_TABLE  = module.dynamodb.rate_limit_table_name
    IDEMPOTENCY_TABLE = module.dynamodb.idempotency_table_name
    TOKEN_QUOTA_TABLE = module.dynamodb.token_quota_table_name
    KINESIS_STREAM    = module.kinesis.stream_name
    KINESIS_ENABLED   = "true"
    METRICS_ENABLED   = "true"
    METRICS_NAMESPACE = "RateLimiter/${var.environment}"
    # I set this explicitly: application.conf defaults it off, and without it
    # /v1/quota/check answers 404 even though its table and IAM are provisioned.
    TOKEN_QUOTA_ENABLED = "true"
    # These three compose the Secrets Manager lookup key and must agree with the
    # secret's name in modules/secrets/main.tf. SECRETS_ENABLED/SECRETS_MANAGER
    # naming matters: the app reads SECRETS_MANAGER_ENABLED, and the previous
    # SECRETS_ENABLED was silently ignored.
    SECRETS_MANAGER_ENABLED = var.enable_secrets_manager ? "true" : "false"
    SECRETS_PREFIX          = var.project_name
    SECRETS_ENVIRONMENT     = var.environment
    API_KEYS_SECRET_NAME    = "api-keys"

    # Pinned rather than inherited. The application default is reject-all, so
    # leaving these unset meant one tripped breaker returned 429 to every
    # tenant for reset_timeout, and there was no way to retune without a
    # rebuild. See variables.tf for the trade-off.
    DEGRADATION_MODE              = var.degradation_mode
    CIRCUIT_BREAKER_MAX_FAILURES  = tostring(var.circuit_breaker_max_failures)
    CIRCUIT_BREAKER_RESET_TIMEOUT = var.circuit_breaker_reset_timeout

    # Tracing follows the endpoint. The application default is enabled = true,
    # so leaving these unset pointed the exporter at its own default of
    # localhost:4317, where no collector runs on Fargate: 73 connection
    # failures in a single load run, each opening a doomed socket on a task
    # that was already starving for CPU. OTEL_SDK_DISABLED is set as well
    # because it is honoured by the SDK itself, not just our config.
    # Per-key ceiling on the auth middleware's anti-brute-force counter. The
    # application default is 1000/min and Terraform set nothing, so the deployed
    # task inherited it while docker-compose raises it to 10,000,000 for load
    # tests. Any correctness run therefore died at ~1000 requests into the
    # minute with HTTP 401 -- on AWS only, and looking exactly like an auth
    # failure rather than a throttle.
    AUTH_RATE_LIMIT_PER_MINUTE = tostring(var.auth_rate_limit_per_minute)

    TRACING_ENABLED             = local.tracing_enabled ? "true" : "false"
    OTEL_SDK_DISABLED           = local.tracing_enabled ? "false" : "true"
    OTEL_EXPORTER_OTLP_ENDPOINT = var.otel_exporter_otlp_endpoint
    OTEL_SERVICE_NAME           = var.project_name
  }

  # IAM permissions
  dynamodb_table_arns = [
    module.dynamodb.rate_limit_table_arn,
    module.dynamodb.idempotency_table_arn,
    module.dynamodb.token_quota_table_arn
  ]
  kinesis_stream_arn     = module.kinesis.stream_arn
  secrets_manager_arn    = module.secrets.api_keys_secret_arn

  enable_autoscaling     = var.enable_autoscaling
  min_capacity           = var.ecs_min_capacity
  max_capacity           = var.ecs_max_capacity
  scale_up_threshold     = 70
  scale_down_threshold   = 30
}

# Monitoring and Alarms
module "monitoring" {
  source = "./modules/monitoring"

  project_name = var.project_name
  environment  = var.environment

  ecs_cluster_name = module.ecs.cluster_name
  ecs_service_name = module.ecs.service_name
  alb_arn_suffix   = module.ecs.alb_arn_suffix

  alarm_sns_topic_arn = var.alarm_sns_topic_arn
}
