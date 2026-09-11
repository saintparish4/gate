# Secrets Manager Module
# Creates: API keys secrets, IAM policies, and optional rotation

locals {
  name_prefix = "${var.project_name}-${var.environment}"
  common_tags = merge(var.tags, {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  })
}

# -----------------------------------------------------------------------------
# API Keys Secret
# -----------------------------------------------------------------------------

# The app looks this secret up as "<secret-prefix>/<environment>/<name>" (see
# SecretsConfig.fullSecretName), so the slashes here are load-bearing: with
# "${project}-${environment}/api-keys" the app would search for
# "<project>/<environment>/api-keys" and never find it. main.tf feeds the same
# three values in as SECRETS_PREFIX / SECRETS_ENVIRONMENT / API_KEYS_SECRET_NAME.
resource "aws_secretsmanager_secret" "api_keys" {
  name        = "${var.project_name}/${var.environment}/api-keys"
  description = "API keys for rate limiter authentication"
  
  recovery_window_in_days = var.environment == "prod" ? 30 : 0
  
  tags = merge(local.common_tags, {
    Name = "${local.name_prefix}-api-keys"
  })
}

resource "aws_secretsmanager_secret_version" "api_keys_initial" {
  secret_id = aws_secretsmanager_secret.api_keys.id
  
  # Initial placeholder - should be updated via CI/CD or manually
  secret_string = jsonencode([
    {
      apiKey      = "REPLACE_WITH_ACTUAL_KEY_1"
      apiKeyId    = "key_001"
      clientName  = "Default Client"
      tier        = "basic"
      permissions = ["ratelimit_check", "ratelimit_status", "idempotency_check"]
      active      = false
    }
  ])
  
  lifecycle {
    ignore_changes = [secret_string]
  }
}

resource "aws_secretsmanager_secret" "app_secrets" {
  name        = "${local.name_prefix}/app-secrets"
  description = "Application secrets and configuration"
  
  recovery_window_in_days = var.environment == "prod" ? 30 : 0
  
  tags = merge(local.common_tags, {
    Name = "${local.name_prefix}-app-secrets"
  })
}

resource "aws_secretsmanager_secret_version" "app_secrets_initial" {
  secret_id = aws_secretsmanager_secret.app_secrets.id
  
  secret_string = jsonencode({
    # Placeholder for future secrets
    placeholder = "Configure actual secrets via CI/CD"
  })
  
  lifecycle {
    ignore_changes = [secret_string]
  }
}

# -----------------------------------------------------------------------------
# IAM Policy for Secret Access
# -----------------------------------------------------------------------------

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

resource "aws_iam_policy" "secrets_read" {
  name        = "${local.name_prefix}-secrets-read"
  description = "Allow reading secrets for ${var.project_name}"
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue",
          "secretsmanager:DescribeSecret"
        ]
        Resource = [
          aws_secretsmanager_secret.api_keys.arn,
          aws_secretsmanager_secret.app_secrets.arn
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:ListSecrets"
        ]
        Resource = "*"
        Condition = {
          StringEquals = {
            "secretsmanager:ResourceTag/Project" = var.project_name
          }
        }
      }
    ]
  })
  
  tags = local.common_tags
}

# Policy for CI/CD to update secrets
resource "aws_iam_policy" "secrets_write" {
  name        = "${local.name_prefix}-secrets-write"
  description = "Allow updating secrets for ${var.project_name}"
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue",
          "secretsmanager:DescribeSecret",
          "secretsmanager:PutSecretValue",
          "secretsmanager:UpdateSecret"
        ]
        Resource = [
          aws_secretsmanager_secret.api_keys.arn,
          aws_secretsmanager_secret.app_secrets.arn
        ]
      }
    ]
  })
  
  tags = local.common_tags
}

# Secret rotation (optional - requires Lambda function)
# resource "aws_secretsmanager_secret_rotation" "api_keys" {
#   count = var.enable_rotation ? 1 : 0
#   
#   secret_id           = aws_secretsmanager_secret.api_keys.id
#   rotation_lambda_arn = aws_lambda_function.secret_rotation[0].arn
#   
#   rotation_rules {
#     automatically_after_days = var.rotation_days
#   }
# }

output "api_keys_secret_arn" {
  description = "ARN of the API keys secret"
  value       = aws_secretsmanager_secret.api_keys.arn
}

output "api_keys_secret_name" {
  description = "Name of the API keys secret"
  value       = aws_secretsmanager_secret.api_keys.name
}

output "app_secrets_arn" {
  description = "ARN of the application secrets"
  value       = aws_secretsmanager_secret.app_secrets.arn
}

output "app_secrets_name" {
  description = "Name of the application secrets"
  value       = aws_secretsmanager_secret.app_secrets.name
}

output "secrets_read_policy_arn" {
  description = "ARN of the IAM policy for reading secrets"
  value       = aws_iam_policy.secrets_read.arn
}

output "secrets_write_policy_arn" {
  description = "ARN of the IAM policy for writing secrets"
  value       = aws_iam_policy.secrets_write.arn
}
