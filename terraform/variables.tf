variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name used for resource naming"
  type        = string
  default     = "rate-limiter"
}

variable "environment" {
  description = "Environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

# Networking
variable "vpc_cidr" {
  description = "CIDR block for VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "availability_zones" {
  description = "List of availability zones"
  type        = list(string)
  default     = ["us-east-1a", "us-east-1b"]
}

variable "private_subnet_cidrs" {
  description = "CIDR blocks for private subnets"
  type        = list(string)
  default     = ["10.0.1.0/24", "10.0.2.0/24"]
}

variable "public_subnet_cidrs" {
  description = "CIDR blocks for public subnets"
  type        = list(string)
  default     = ["10.0.101.0/24", "10.0.102.0/24"]
}

# DynamoDB
variable "dynamodb_billing_mode" {
  description = "DynamoDB billing mode (PAY_PER_REQUEST or PROVISIONED)"
  type        = string
  default     = "PAY_PER_REQUEST"
}

variable "dynamodb_read_capacity" {
  description = "DynamoDB read capacity units (only for PROVISIONED mode)"
  type        = number
  default     = 5
}

variable "dynamodb_write_capacity" {
  description = "DynamoDB write capacity units (only for PROVISIONED mode)"
  type        = number
  default     = 5
}

# Kinesis
variable "kinesis_shard_count" {
  description = "Number of Kinesis shards"
  type        = number
  default     = 1
}

variable "kinesis_retention_hours" {
  description = "Kinesis data retention in hours"
  type        = number
  default     = 24
}

variable "enable_kinesis_firehose" {
  description = "Enable Kinesis Firehose for S3 delivery"
  type        = bool
  default     = false
}

variable "kinesis_s3_bucket" {
  description = "S3 bucket for Kinesis Firehose delivery"
  type        = string
  default     = ""
}

# Audit trail (PCI DSS 4.0.1); used by kinesis module for S3/Glue/Firehose
variable "enable_audit_compliance" {
  description = "Enable PCI DSS 4.0.1 audit trail with 7-year S3 retention"
  type        = bool
  default     = false
}

# ECS
variable "container_image" {
  description = "Docker container image"
  type        = string
}

variable "ecs_desired_count" {
  description = "Desired number of ECS tasks"
  type        = number
  default     = 2
}

variable "ecs_cpu" {
  description = "CPU units for ECS task"
  type        = number
  default     = 256
}

variable "ecs_memory" {
  description = "Memory (MB) for ECS task"
  type        = number
  default     = 512
}

variable "enable_autoscaling" {
  description = "Enable ECS service autoscaling"
  type        = bool
  default     = true
}

variable "ecs_min_capacity" {
  description = "Minimum number of ECS tasks"
  type        = number
  default     = 2
}

variable "ecs_max_capacity" {
  description = "Maximum number of ECS tasks"
  type        = number
  default     = 10
}

# Monitoring
variable "alarm_sns_topic_arn" {
  description = "SNS topic ARN for CloudWatch alarms"
  type        = string
  default     = ""
}

variable "enable_secrets_manager" {
  description = <<-DESC
    Load API keys from Secrets Manager instead of the built-in keys.

    Off by default on purpose: modules/secrets seeds the api-keys secret with a
    single placeholder entry whose "active" flag is false, so turning this on
    before writing real keys into the secret leaves the service with zero usable
    API keys and every request answers 401. Populate the secret first, then set
    this to true.
  DESC
  type        = bool
  default     = false
}

# ---------------------------------------------------------------------------
# Resilience posture
#
# These were previously unset, so the container inherited application.conf's
# defaults by accident. Both decide what happens to *all* traffic when the
# shared DynamoDB circuit breaker opens, so they are pinned explicitly here.
# ---------------------------------------------------------------------------

variable "degradation_mode" {
  description = "Behaviour when the circuit breaker opens or the bulkhead sheds a request. reject-all fails closed (429 for every caller, no runaway downstream spend); allow-all fails open (admits everything while the breaker is open); use-cached has no cache wired yet and currently behaves as allow-all."
  type        = string
  default     = "reject-all"

  validation {
    condition     = contains(["reject-all", "allow-all", "use-cached"], var.degradation_mode)
    error_message = "degradation_mode must be one of: reject-all, allow-all, use-cached."
  }
}

variable "circuit_breaker_max_failures" {
  description = "Consecutive unanswered DynamoDB calls before the shared circuit breaker opens. The count resets on the next success. Low values are hazardous: the breaker is process-wide, so tripping it applies degradation_mode to every tenant."
  type        = number
  default     = 20

  validation {
    condition     = var.circuit_breaker_max_failures >= 5
    error_message = "circuit_breaker_max_failures below 5 will trip on routine latency blips."
  }
}

variable "circuit_breaker_reset_timeout" {
  description = "How long the circuit breaker stays open before it admits a probe request. HOCON duration string."
  type        = string
  default     = "30 seconds"
}
