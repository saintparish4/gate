#!/usr/bin/env bash
# teardown-eks.sh - remove everything deploy-eks.sh created, ALB first.
#
# The ALB and its target groups belong to the AWS Load Balancer Controller, not
# to Terraform. Destroying the cluster first would orphan them, and their
# security groups would then block the VPC's deletion. So the chart goes first,
# and this waits for the controller to delete the ALB.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
# shellcheck source=lib-terraform.sh
source "$SCRIPT_DIR/lib-terraform.sh"

require_terraform
use_cli_credentials
require_backend

cd "$PROJECT_ROOT/terraform/eks"
tf_init "gate/eks/terraform.tfstate"

if CLUSTER=$(terraform output -raw cluster_name 2>/dev/null) && [ -n "$CLUSTER" ]; then
  REGION=$(terraform output -raw region)
  if aws eks update-kubeconfig --name "$CLUSTER" --region "$REGION" >/dev/null 2>&1; then
    helm uninstall gate -n gate --wait 2>/dev/null || true
    echo "Waiting for the controller to delete the ALB..."
    for _ in $(seq 1 30); do
      kubectl -n gate get ingress gate >/dev/null 2>&1 || break
      sleep 10
    done
    helm uninstall kube-prometheus-stack -n monitoring --wait 2>/dev/null || true
    helm uninstall aws-load-balancer-controller -n kube-system --wait 2>/dev/null || true
  fi
fi

# The api_access_cidrs validation needs a value even to destroy.
terraform destroy -auto-approve -var='api_access_cidrs=["127.0.0.1/32"]'
echo "Torn down. The ECR images stay (terraform/bootstrap owns them)."
