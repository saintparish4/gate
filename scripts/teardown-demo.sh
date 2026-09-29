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
# shellcheck source=lib-terraform.sh
source "$SCRIPT_DIR/lib-terraform.sh"

require_terraform
use_cli_credentials
require_backend

cd "$TF_DIR"

# State is in S3 under the demo's key, so any machine with the backend config
# sees the same demo; there is no local file to look for any more.
tf_init "gate/demo/terraform.tfstate" >/dev/null

if [ -z "$(terraform state list)" ]; then
  echo "The demo's Terraform state is empty - nothing to destroy."
  exit 0
fi

# container_image has no default, and destroy still evaluates variables, so it
# has to be supplied even though the value is irrelevant to teardown.
DESTROY_STATUS=0
# allow_public_plaintext only keeps the listener's plaintext guard quiet while
# planning a destroy; nothing is being opened.
terraform destroy -auto-approve \
  -var-file=environments/demo.tfvars \
  -var="container_image=${ECR_IMAGE:-unused-during-destroy}" \
  -var="enable_autoscaling=false" \
  -var="enable_kinesis_firehose=false" \
  -var="allow_public_plaintext=true" || DESTROY_STATUS=$?

# I read state back even when destroy failed: a partial destroy is the one that
# keeps billing, so it still has to end with the list of what survived.
if ! REMAINING=$(terraform state list); then
  echo
  echo "TEARDOWN UNVERIFIED - could not read the demo's Terraform state from S3."
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

# The keys died with their secret, and the next deploy writes new ones.
rm -f "$TF_DIR/../.demo-keys.env"

echo
if [ "$DESTROY_STATUS" -ne 0 ]; then
  echo "terraform destroy exited $DESTROY_STATUS, but Terraform state is empty."
else
  echo "Demo environment destroyed; Terraform state is empty."
fi
echo "The ECR repository and state bucket belong to terraform/bootstrap and stay."
echo "Verify nothing was left behind (NAT gateways and ALBs bill by the hour):"
echo "  aws ec2 describe-nat-gateways --filter Name=state,Values=available --query 'NatGateways[].NatGatewayId'"
echo "  aws elbv2 describe-load-balancers --query 'LoadBalancers[].LoadBalancerName'"
echo "  aws ecs list-clusters --query 'clusterArns'"
exit "$DESTROY_STATUS"
