#!/usr/bin/env bash
# bootstrap.sh - create what every environment stands on, once per account:
# the S3 bucket that holds Terraform state, and the ECR repository (through the
# terraform/bootstrap root). Safe to re-run.
#
#   ./scripts/bootstrap.sh
#   export ECR_IMAGE=$(./scripts/publish-image.sh)
#   ./scripts/deploy-demo.sh
#
# State used to be a local file, lost with the machine and shared by every
# environment, and the ECR repository was made by publish-image.sh outside any
# state (rows 12-13). Terraform cannot keep its state in a bucket it has not
# created yet, so the bucket is made here with the CLI.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
# shellcheck source=lib-terraform.sh
source "$SCRIPT_DIR/lib-terraform.sh"

require_terraform
use_cli_credentials

REGION="${AWS_REGION:-us-east-1}"
REPO="${ECR_REPO_NAME:-gate}"
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
BUCKET="${TF_STATE_BUCKET:-gate-tfstate-$ACCOUNT}"

echo "Account $ACCOUNT / region $REGION / state bucket $BUCKET"

if aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null; then
  echo "State bucket exists."
else
  echo "Creating state bucket..."
  if [ "$REGION" = "us-east-1" ]; then
    aws s3api create-bucket --bucket "$BUCKET" --region "$REGION" >/dev/null
  else
    aws s3api create-bucket --bucket "$BUCKET" --region "$REGION" \
      --create-bucket-configuration LocationConstraint="$REGION" >/dev/null
  fi
fi

# Reapplied every run, so a bucket made by hand or before this script gets the
# same guarantees: private, versioned (a bad apply can be rolled back),
# encrypted, and reachable only over TLS.
echo "Securing the state bucket..."
aws s3api put-public-access-block --bucket "$BUCKET" \
  --public-access-block-configuration \
  BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
aws s3api put-bucket-versioning --bucket "$BUCKET" \
  --versioning-configuration Status=Enabled
aws s3api put-bucket-encryption --bucket "$BUCKET" \
  --server-side-encryption-configuration \
  '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"},"BucketKeyEnabled":true}]}'
aws s3api put-bucket-policy --bucket "$BUCKET" --policy "$(printf '{
  "Version": "2012-10-17",
  "Statement": [{
    "Sid": "DenyInsecureTransport",
    "Effect": "Deny",
    "Principal": "*",
    "Action": "s3:*",
    "Resource": ["arn:aws:s3:::%s", "arn:aws:s3:::%s/*"],
    "Condition": {"Bool": {"aws:SecureTransport": "false"}}
  }]
}' "$BUCKET" "$BUCKET")"

cat > "$BACKEND_HCL" <<HCL
# Written by scripts/bootstrap.sh; gitignored. Each root adds its own key.
bucket       = "$BUCKET"
region       = "$REGION"
encrypt      = true
use_lockfile = true
HCL
echo "Wrote $BACKEND_HCL"

cd "$PROJECT_ROOT/terraform/bootstrap"
tf_init "gate/bootstrap/terraform.tfstate"

VARS=(-var="aws_region=$REGION" -var="repository_name=$REPO")

# A repository made by the old publish-image.sh is adopted, not recreated, so
# its images survive.
if aws ecr describe-repositories --repository-names "$REPO" --region "$REGION" >/dev/null 2>&1 \
   && ! terraform state list 2>/dev/null | grep -qx 'aws_ecr_repository.app'; then
  echo "Importing existing ECR repository '$REPO'..."
  terraform import -input=false "${VARS[@]}" aws_ecr_repository.app "$REPO"
  if aws ecr get-lifecycle-policy --repository-name "$REPO" --region "$REGION" >/dev/null 2>&1; then
    terraform import -input=false "${VARS[@]}" aws_ecr_lifecycle_policy.app "$REPO"
  fi
fi

terraform apply -input=false -auto-approve "${VARS[@]}"

echo ""
echo "Registry: $(terraform output -raw repository_url)"
echo "Next:  export ECR_IMAGE=\$(./scripts/publish-image.sh) && ./scripts/deploy-demo.sh"
