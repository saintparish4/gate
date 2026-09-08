#!/usr/bin/env bash
# teardown-demo.sh - Destroy the demo environment and prove it is gone.
#
# The previous version did `cd terraform/environments/demo`, which is a file
# path that does not exist (environments/ holds .tfvars files, not directories),
# so it exited before destroying anything while still reading like it had
# worked. Nothing here is allowed to fail silently: the demo bills by the hour.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TF_DIR="$(cd "$SCRIPT_DIR/.." && pwd)/terraform"

cd "$TF_DIR"

if [ ! -f terraform.tfstate ] && [ ! -d .terraform ]; then
  echo "No Terraform state in $TF_DIR - nothing to destroy."
  echo "If you deployed from a different machine or backend, run destroy there."
  exit 0
fi

terraform init -input=false >/dev/null

# container_image has no default, and destroy still evaluates variables, so it
# has to be supplied even though the value is irrelevant to teardown.
terraform destroy -auto-approve \
  -var-file=environments/demo.tfvars \
  -var="container_image=${ECR_IMAGE:-unused-during-destroy}" \
  -var="enable_autoscaling=false" \
  -var="enable_kinesis_firehose=false"

REMAINING=$(terraform state list 2>/dev/null | wc -l)
if [ "$REMAINING" -ne 0 ]; then
  echo
  echo "TEARDOWN INCOMPLETE - $REMAINING resource(s) still in state:"
  terraform state list
  echo "These are still billing. Re-run this script or destroy them manually."
  exit 1
fi

echo
echo "Demo environment destroyed; Terraform state is empty."
echo "Verify nothing was left behind (NAT gateways and ALBs bill by the hour):"
echo "  aws ec2 describe-nat-gateways --filter Name=state,Values=available --query 'NatGateways[].NatGatewayId'"
echo "  aws elbv2 describe-load-balancers --query 'LoadBalancers[].LoadBalancerName'"
echo "  aws ecs list-clusters --query 'clusterArns'"
