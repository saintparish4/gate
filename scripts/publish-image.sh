#!/usr/bin/env bash
# publish-image.sh - Build the Gate image and push it to ECR, then print the URI.
#
# terraform/ has no aws_ecr_repository resource and container_image is a
# required variable with no default, so the image has to exist in ECR before
# deploy-demo.sh runs. This script closes that gap.
#
#   export ECR_IMAGE=$(./scripts/publish-image.sh)
#   ./scripts/deploy-demo.sh
set -euo pipefail

REPO="${ECR_REPO_NAME:-gate}"
REGION="${AWS_REGION:-us-east-1}"
TAG="${IMAGE_TAG:-$(git rev-parse --short HEAD)}"

# Everything informational goes to stderr so the last stdout line is the URI.
log() { echo "$@" >&2; }

ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
REGISTRY="${ACCOUNT}.dkr.ecr.${REGION}.amazonaws.com"
URI="${REGISTRY}/${REPO}:${TAG}"

log "Account $ACCOUNT / region $REGION / image $URI"

if ! aws ecr describe-repositories --repository-names "$REPO" --region "$REGION" >/dev/null 2>&1; then
  log "Creating ECR repository '$REPO'..."
  aws ecr create-repository \
    --repository-name "$REPO" \
    --region "$REGION" \
    --image-scanning-configuration scanOnPush=true >/dev/null
fi

# The repository lives outside Terraform state, so `terraform destroy` and
# teardown-demo.sh both leave it behind and it gains an image per deploy. I
# reapply this every run rather than only on create, so repositories made
# before the policy existed pick it up too.
log "Applying ECR lifecycle policy (expire untagged >1d, keep last 10)..."
aws ecr put-lifecycle-policy \
  --repository-name "$REPO" \
  --region "$REGION" \
  --lifecycle-policy-text '{
    "rules": [
      {
        "rulePriority": 1,
        "description": "Expire untagged images after 1 day",
        "selection": {
          "tagStatus": "untagged",
          "countType": "sinceImagePushed",
          "countUnit": "days",
          "countNumber": 1
        },
        "action": { "type": "expire" }
      },
      {
        "rulePriority": 2,
        "description": "Keep only the 10 most recent images",
        "selection": {
          "tagStatus": "any",
          "countType": "imageCountMoreThan",
          "countNumber": 10
        },
        "action": { "type": "expire" }
      }
    ]
  }' >/dev/null

log "Logging Docker in to ECR..."
aws ecr get-login-password --region "$REGION" \
  | docker login --username AWS --password-stdin "$REGISTRY" >/dev/null 2>&1

# Fargate runs linux/amd64. Building on any other host architecture without
# --platform produces an image the task will fail to start.
log "Building $URI for linux/amd64..."
docker build --platform linux/amd64 -t "$URI" . >&2

log "Pushing..."
docker push "$URI" >&2

log "Pushed."
echo "$URI"
