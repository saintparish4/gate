# =============================================================================
# Demo Environment Configuration
# Quick-deploy configuration for interviews and demos
# =============================================================================

environment = "demo"

# AWS Configuration
aws_region = "us-east-1"

# ECS Configuration.
# Raised from 256/512 after the first real AWS validation: a quarter-vCPU JVM
# could serve the enforcement demo but saturated under the correctness
# scenario's concurrency, logging cats-effect starvation warnings and dropping
# requests at the connection layer before they reached the app. 25 RPS on AWS
# against 77 locally. 1024/2048 is the smallest valid Fargate pairing that
# leaves headroom to actually measure the invariants.
ecs_desired_count = 1
ecs_cpu           = 1024
ecs_memory        = 2048
enable_autoscaling = false
ecs_min_capacity  = 1
ecs_max_capacity  = 1

# DynamoDB Configuration (on-demand for demo)
dynamodb_billing_mode = "PAY_PER_REQUEST"

# Kinesis Configuration (minimal)
kinesis_shard_count     = 1
kinesis_retention_hours = 24
enable_kinesis_firehose = false

# Networking.
# Two AZs is the minimum, not a preference: an Application Load Balancer
# requires subnets in at least two Availability Zones, and the subnet
# resources index availability_zones[count.index] over the two subnet CIDRs,
# so a single-AZ list fails at plan time with an index-out-of-range error.
availability_zones   = ["us-east-1a", "us-east-1b"]
public_subnet_cidrs  = ["10.0.101.0/24", "10.0.102.0/24"]
private_subnet_cidrs = ["10.0.1.0/24", "10.0.2.0/24"]

# Container image - set via -var flag in deploy script
# container_image = "123456789.dkr.ecr.us-east-1.amazonaws.com/rate-limiter:latest"
