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
