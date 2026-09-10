#!/bin/bash
# teardown-demo.sh - Destroy the demo stack.
#
# This script previously ran `cd terraform/environments/demo`. That directory
# does not exist -- `environments/` holds .tfvars FILES, not per-environment
# root modules, and deploy-demo.sh runs from `terraform/` with
# `-var-file=environments/demo.tfvars`. The `cd` failed, `|| exit` fired, and
# the script exited BEFORE `terraform destroy` ran, while printing nothing to
# say so. Anyone trusting it kept paying for a stack they believed was gone.
#
# It also has to pass the same var-file the deploy used: `container_image` has
# no default, so a bare `terraform destroy` cannot even build a plan.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT/terraform"

if [ ! -d .terraform ]; then
  echo "No Terraform state initialised here. Nothing to destroy."
  echo "(If you deployed from a different machine, run 'terraform init' first.)"
  exit 0
fi

# The image URI is irrelevant to a destroy, but the variable is required, so
# give it something rather than blocking on a prompt.
terraform destroy -auto-approve \
  -var-file=environments/demo.tfvars \
  -var="container_image=${ECR_IMAGE:-unused-for-destroy}"

echo ""
echo "Demo environment destroyed."
echo ""
echo "Confirm nothing is left running -- a destroy that partially fails still"
echo "bills for whatever survived:"
echo "  aws ecs list-clusters"
echo "  aws elbv2 describe-load-balancers --query 'LoadBalancers[].LoadBalancerName'"
echo "  aws dynamodb list-tables"
echo "  aws kinesis list-streams"
