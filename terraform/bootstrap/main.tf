# Bootstrap: what outlives every environment. It is applied by
# scripts/bootstrap.sh, never by deploy or teardown, so tearing a demo down
# leaves the image registry and its images in place.
#
# The ECR repository used to be created by publish-image.sh with imperative CLI
# calls, outside any Terraform state. The state bucket itself is created by the
# script, because Terraform cannot store its state in a bucket it has not made
# yet.

terraform {
  required_version = ">= 1.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Partial configuration: bootstrap.sh passes backend.hcl (bucket, region,
  # lockfile) and key = "gate/bootstrap/terraform.tfstate".
  backend "s3" {}
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project   = "gate"
      ManagedBy = "terraform"
      Stack     = "bootstrap"
    }
  }
}

variable "aws_region" {
  description = "Region for the image registry"
  type        = string
  default     = "us-east-1"
}

variable "repository_name" {
  description = "ECR repository for the Gate image"
  type        = string
  default     = "gate"
}

resource "aws_ecr_repository" "app" {
  name = var.repository_name

  # A tag always names the same image. publish-image.sh tags with the commit,
  # so a mutable tag would let one commit's tag point at different builds.
  image_tag_mutability = "IMMUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  # AES256 is also what a repository created by the CLI gets, so importing one
  # does not force a replacement.
  encryption_configuration {
    encryption_type = "AES256"
  }

  # force_delete stays false: destroying this root must not silently take every
  # image with it.
}

resource "aws_ecr_lifecycle_policy" "app" {
  repository = aws_ecr_repository.app.name

  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Expire untagged images after 1 day"
        selection = {
          tagStatus   = "untagged"
          countType   = "sinceImagePushed"
          countUnit   = "days"
          countNumber = 1
        }
        action = { type = "expire" }
      },
      {
        rulePriority = 2
        description  = "Keep only the 10 most recent images"
        selection = {
          tagStatus   = "any"
          countType   = "imageCountMoreThan"
          countNumber = 10
        }
        action = { type = "expire" }
      }
    ]
  })
}

output "repository_url" {
  description = "Push images here; publish-image.sh reads it"
  value       = aws_ecr_repository.app.repository_url
}

# ---------------------------------------------------------------------------
# A role GitHub Actions can assume, for the scheduled correctness run.
#
# CI checks the invariants against LocalStack with 10 s store timeouts.
# Production uses 2 s, and the one failure seen on AWS was a 2 s timeout, which
# CI cannot produce. .github/workflows/aws-correctness.yml deploys the demo,
# runs the invariants and tears it down, and needs credentials to do it.
#
# Off by default: nothing here exists until the account owner opts in with
#   TF_VAR_github_actions_role=true ./scripts/bootstrap.sh
# The role is assumed through OIDC, so there is no long-lived key to store, and
# only by workflows running on one ref of one repository. It gets no
# permissions here either: github_actions_policy_arns names what to attach.
# Deploying the demo creates IAM roles, so whatever is attached is close to
# administrator in effect; that is the owner's call to make, not a default.
# ---------------------------------------------------------------------------

variable "github_actions_role" {
  description = "Create the role the scheduled AWS correctness workflow assumes"
  type        = bool
  default     = false
}

variable "github_repository" {
  description = "owner/name of the repository whose workflows may assume the role"
  type        = string
  default     = "saintparish4/gate"
}

variable "github_actions_ref" {
  description = "The only git ref whose workflow runs may assume the role"
  type        = string
  default     = "refs/heads/master"
}

variable "github_oidc_provider_arn" {
  description = "An existing GitHub OIDC provider in this account. Empty creates one; an account can hold only one per URL."
  type        = string
  default     = ""
}

variable "github_actions_policy_arns" {
  description = "Policies to attach to the role. Empty leaves it able to do nothing."
  type        = list(string)
  default     = []
}

resource "aws_iam_openid_connect_provider" "github" {
  count = var.github_actions_role && var.github_oidc_provider_arn == "" ? 1 : 0

  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]
}

locals {
  github_oidc_provider_arn = var.github_oidc_provider_arn != "" ? var.github_oidc_provider_arn : one(aws_iam_openid_connect_provider.github[*].arn)
}

resource "aws_iam_role" "github_actions" {
  count = var.github_actions_role ? 1 : 0

  name                 = "gate-github-actions-correctness"
  description          = "Assumed by the scheduled correctness workflow of ${var.github_repository}"
  max_session_duration = 3600

  # Both conditions matter. Without the sub condition any repository on GitHub
  # could assume the role; without the ref, any branch or pull request of this
  # one could.
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRoleWithWebIdentity"
      Principal = { Federated = local.github_oidc_provider_arn }
      Condition = {
        StringEquals = {
          "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
          "token.actions.githubusercontent.com:sub" = "repo:${var.github_repository}:ref:${var.github_actions_ref}"
        }
      }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "github_actions" {
  for_each = var.github_actions_role ? toset(var.github_actions_policy_arns) : toset([])

  role       = aws_iam_role.github_actions[0].name
  policy_arn = each.value
}

output "github_actions_role_arn" {
  description = "Set this as the repository variable AWS_CORRECTNESS_ROLE_ARN to switch the scheduled run on"
  value       = one(aws_iam_role.github_actions[*].arn)
}
