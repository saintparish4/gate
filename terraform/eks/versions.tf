terraform {
  # 1.10 for S3 lockfile locking (scripts/lib-terraform.sh).
  required_version = ">= 1.10"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.70"
    }
  }

  # Partial configuration, like the ECS root: scripts/deploy-eks.sh passes
  # backend.hcl and key = "gate/eks/terraform.tfstate".
  backend "s3" {}
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
