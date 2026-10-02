#!/usr/bin/env bash
# publish-image.sh - Build the Gate image and push it to ECR, then print the URI.
#
# The repository and its lifecycle policy belong to terraform/bootstrap, so run
# ./scripts/bootstrap.sh once first. This script used to create both with the
# CLI, outside any Terraform state.
#
#   export ECR_IMAGE=$(./scripts/publish-image.sh)
#   ./scripts/deploy-demo.sh
set -euo pipefail

REPO="${ECR_REPO_NAME:-gate}"
REGION="${AWS_REGION:-us-east-1}"

# Tags are immutable, so a tag always names one image. A build from a tree
# with uncommitted changes must not take the commit's tag, or that tag would
# name code the commit never contained.
TAG="${IMAGE_TAG:-$(git rev-parse --short HEAD)}"
if [ -z "${IMAGE_TAG:-}" ] && [ -n "$(git status --porcelain)" ]; then
  TAG="$TAG-dirty-$(date +%Y%m%d%H%M%S)"
fi

# Everything informational goes to stderr so the last stdout line is the URI.
log() { echo "$@" >&2; }

ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
REGISTRY="${ACCOUNT}.dkr.ecr.${REGION}.amazonaws.com"
URI="${REGISTRY}/${REPO}:${TAG}"

log "Account $ACCOUNT / region $REGION / image $URI"

if ! aws ecr describe-repositories --repository-names "$REPO" --region "$REGION" >/dev/null 2>&1; then
  log "Error: ECR repository '$REPO' does not exist. Create it with:"
  log "   ./scripts/bootstrap.sh"
  exit 1
fi

if aws ecr describe-images --repository-name "$REPO" --region "$REGION" \
     --image-ids imageTag="$TAG" >/dev/null 2>&1; then
  log "$URI is already in ECR; tags are immutable, so it is reused as is."
  echo "$URI"
  exit 0
fi

log "Logging Docker in to ECR..."
aws ecr get-login-password --region "$REGION" \
  | docker login --username AWS --password-stdin "$REGISTRY" >/dev/null 2>&1

# Fargate runs linux/amd64. Building on any other host architecture without
# --platform produces an image the task will fail to start.
log "Building $URI for linux/amd64..."
# The tag is the commit (or commit-dirty-timestamp), and /health reports it.
docker build --platform linux/amd64 --build-arg GIT_COMMIT="$TAG" -t "$URI" . >&2

log "Pushing..."
docker push "$URI" >&2

log "Pushed."
echo "$URI"
