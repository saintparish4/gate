#!/bin/bash
# push-image.sh - Build the app image and push it to ECR, then print the URI
# that deploy-demo.sh expects in $ECR_IMAGE.
#
# This is the step that was missing between "there is a Dockerfile" and
# "export ECR_IMAGE=...". deploy-demo.sh refuses to run without that variable
# and nothing in the repository produced it.
#
# Usage:
#   ./scripts/push-image.sh                 # builds :latest, region us-east-1
#   AWS_REGION=eu-west-1 TAG=v1 ./scripts/push-image.sh
#
# Then:
#   export ECR_IMAGE=$(cat .ecr-image)
#   ./scripts/deploy-demo.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

REPO_NAME="${REPO_NAME:-gate}"
AWS_REGION="${AWS_REGION:-us-east-1}"
TAG="${TAG:-latest}"

command -v aws >/dev/null || { echo "aws CLI not found"; exit 1; }
command -v docker >/dev/null || { echo "docker not found"; exit 1; }

ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)" || {
  echo "AWS credentials are not configured. Run: aws configure"
  exit 1
}
REGISTRY="${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"
IMAGE="${REGISTRY}/${REPO_NAME}:${TAG}"

echo "Account:  ${ACCOUNT_ID}"
echo "Region:   ${AWS_REGION}"
echo "Image:    ${IMAGE}"
echo ""

# Idempotent: create the repository only if it is not already there.
if ! aws ecr describe-repositories --repository-names "$REPO_NAME" --region "$AWS_REGION" >/dev/null 2>&1; then
  echo "Creating ECR repository ${REPO_NAME}..."
  aws ecr create-repository \
    --repository-name "$REPO_NAME" \
    --region "$AWS_REGION" \
    --image-scanning-configuration scanOnPush=true >/dev/null
fi

echo "Logging Docker in to ECR..."
aws ecr get-login-password --region "$AWS_REGION" \
  | docker login --username AWS --password-stdin "$REGISTRY"

# ECS Fargate runs linux/amd64. Building on an arm64 laptop without this flag
# produces an image that pushes fine and then crash-loops with an exec format
# error that surfaces only in CloudWatch logs.
echo "Building (linux/amd64)..."
docker build --platform linux/amd64 -t "$IMAGE" .

echo "Pushing..."
docker push "$IMAGE"

echo "$IMAGE" > .ecr-image
echo ""
echo "Pushed. Next:"
echo "  export ECR_IMAGE=\$(cat .ecr-image)"
echo "  ./scripts/deploy-demo.sh"
