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

# .terraform/ exists after any init or validate, so I look for state itself: the
# local state file, or the backend record a remote backend leaves behind.
if [ ! -f terraform.tfstate ] && [ ! -f .terraform/terraform.tfstate ]; then
  echo "No Terraform state in $TF_DIR - nothing to destroy."
  echo "If you deployed from a different machine or backend, run destroy there."
  exit 0
fi

terraform init -input=false >/dev/null

# container_image has no default, and destroy still evaluates variables, so it
# has to be supplied even though the value is irrelevant to teardown.
DESTROY_STATUS=0
terraform destroy -auto-approve \
  -var-file=environments/demo.tfvars \
  -var="container_image=${ECR_IMAGE:-unused-during-destroy}" \
  -var="enable_autoscaling=false" \
  -var="enable_kinesis_firehose=false" || DESTROY_STATUS=$?

# I read state back even when destroy failed: a partial destroy is the one that
# keeps billing, so it still has to end with the list of what survived.
if ! REMAINING=$(terraform state list); then
  echo
  echo "TEARDOWN UNVERIFIED - could not read Terraform state in $TF_DIR."
  echo "Assume the demo is still billing until 'terraform state list' is empty."
  exit 1
fi

if [ -n "$REMAINING" ]; then
  echo
  echo "TEARDOWN INCOMPLETE - $(printf '%s\n' "$REMAINING" | wc -l | tr -d ' ') resource(s) still in state:"
  printf '%s\n' "$REMAINING"
  echo "These are still billing. Re-run this script or destroy them manually."
  exit 1
fi

echo
if [ "$DESTROY_STATUS" -ne 0 ]; then
  echo "terraform destroy exited $DESTROY_STATUS, but Terraform state is empty."
else
  echo "Demo environment destroyed; Terraform state is empty."
fi
echo "Verify nothing was left behind (NAT gateways and ALBs bill by the hour):"
echo "  aws ec2 describe-nat-gateways --filter Name=state,Values=available --query 'NatGateways[].NatGatewayId'"
echo "  aws elbv2 describe-load-balancers --query 'LoadBalancers[].LoadBalancerName'"
echo "  aws ecs list-clusters --query 'clusterArns'"
exit "$DESTROY_STATUS"
