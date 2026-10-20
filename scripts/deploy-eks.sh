#!/usr/bin/env bash
# deploy-eks.sh - Gate on EKS: Terraform (terraform/eks), then the AWS Load
# Balancer Controller, kube-prometheus-stack, and the gate chart.
#
# Prepared and validated without AWS (make tf-validate, make helm-check, and
# a kind rehearsal); its first real run is the owner's. What it costs while up:
# the EKS control plane ($0.10/h), three m6i.large nodes, two NAT gateways, an
# ALB, and on-demand DynamoDB and one Kinesis shard: roughly $0.55/h, about
# $13/day. scripts/teardown-eks.sh removes all of it.
#
#   ./scripts/bootstrap.sh                # once per account: state bucket, ECR
#   export GATE_IMAGE=$(./scripts/publish-image.sh)
#   export AGGREGATOR_IMAGE=$(ECR_REPO_NAME=gate-usage-aggregator DOCKERFILE=aggregator.Dockerfile ./scripts/publish-image.sh)
#   ./scripts/deploy-eks.sh
#
# Optional: CERTIFICATE_ARN (HTTPS on the ALB; without it the ALB serves HTTP
# to this machine's address only), ALB_INGRESS_CIDR, API_ACCESS_CIDR.
#
# Needs: aws (signed in), terraform >= 1.10, kubectl, helm 3, openssl, curl.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
# shellcheck source=lib-terraform.sh
source "$SCRIPT_DIR/lib-terraform.sh"

NAMESPACE=gate
RELEASE=gate
# Pinned. The controller's IAM policy in terraform/eks/policies is this version's.
LB_CONTROLLER_CHART_VERSION=1.10.1
KUBE_PROMETHEUS_STACK_VERSION=65.8.1
KEYS_FILE="$PROJECT_ROOT/.eks-keys.env"

for tool in aws terraform kubectl helm openssl curl; do
  command -v "$tool" >/dev/null 2>&1 || { echo "Error: $tool not found in PATH." >&2; exit 1; }
done
: "${GATE_IMAGE:?GATE_IMAGE is required: export GATE_IMAGE=\$(./scripts/publish-image.sh)}"
: "${AGGREGATOR_IMAGE:?AGGREGATOR_IMAGE is required (see the header of this script)}"
aws sts get-caller-identity >/dev/null 2>&1 || { echo "Error: no AWS session. Run: aws login" >&2; exit 1; }

require_terraform
use_cli_credentials
require_backend

MY_IP=""
my_cidr() {
  [ -n "$MY_IP" ] || MY_IP=$(curl -sf --max-time 10 https://checkip.amazonaws.com | tr -d '[:space:]')
  [ -n "$MY_IP" ] || { echo "Error: could not detect your public IP; set API_ACCESS_CIDR and ALB_INGRESS_CIDR." >&2; exit 1; }
  echo "$MY_IP/32"
}
API_CIDR="${API_ACCESS_CIDR:-$(my_cidr)}"

cd "$PROJECT_ROOT/terraform/eks"
tf_init "gate/eks/terraform.tfstate"

TF_VARS=(
  -var="api_access_cidrs=[\"$API_CIDR\"]"
  -var="gate_image=$GATE_IMAGE"
  -var="aggregator_image=$AGGREGATOR_IMAGE"
)
if [ -n "${CERTIFICATE_ARN:-}" ]; then
  TF_VARS+=(-var="certificate_arn=$CERTIFICATE_ARN")
else
  ALB_CIDR="${ALB_INGRESS_CIDR:-$(my_cidr)}"
  TF_VARS+=(-var="alb_ingress_cidrs=[\"$ALB_CIDR\"]")
  echo "No certificate: the ALB will serve HTTP to $ALB_CIDR only."
fi

# The secret first, with real keys, before any pod can read it: the service
# refuses to start on the placeholder (the same order as deploy-demo.sh).
terraform apply -auto-approve "${TF_VARS[@]}" -target=module.secrets
SECRET_ID=$(terraform output -raw api_keys_secret_name)

key_json() { # apiKey apiKeyId clientName tier permissions
  printf '{"apiKey":"%s","apiKeyId":"%s","clientName":"%s","tier":"%s","permissions":[%s],"active":true}' \
    "$1" "$2" "$3" "$4" "$5"
}
if aws secretsmanager get-secret-value --secret-id "$SECRET_ID" \
     --query SecretString --output text | grep -Eq '"active" *: *true'; then
  echo "$SECRET_ID already holds active keys; keeping them."
  [ -f "$KEYS_FILE" ] || { echo "Error: $KEYS_FILE is missing, and the metrics Secret needs the admin key from it." >&2; exit 1; }
else
  API_KEY="gate_$(openssl rand -hex 24)"
  FREE_API_KEY="gate_$(openssl rand -hex 24)"
  ADMIN_API_KEY="gate_$(openssl rand -hex 24)"
  STANDARD='"ratelimit_check","ratelimit_status","idempotency_check","idempotency_complete","quota_check","quota_reconcile"'
  ADMIN="$STANDARD,\"admin_metrics\""
  KEYS_JSON="[$(key_json "$API_KEY" key_eks_api "EKS client" premium "$STANDARD"),$(key_json "$FREE_API_KEY" key_eks_free "EKS free client" free "$STANDARD"),$(key_json "$ADMIN_API_KEY" key_eks_admin "EKS admin" enterprise "$ADMIN")]"
  aws secretsmanager put-secret-value --secret-id "$SECRET_ID" --secret-string "$KEYS_JSON" >/dev/null
  (
    umask 077
    cat > "$KEYS_FILE" <<KEYS
# EKS API keys, written by scripts/deploy-eks.sh. Load them with:
#   source .eks-keys.env
export API_KEY=$API_KEY
export FREE_API_KEY=$FREE_API_KEY
export ADMIN_API_KEY=$ADMIN_API_KEY
KEYS
  )
  echo "Saved the keys to $KEYS_FILE (gitignored, mode 600)."
fi
# shellcheck disable=SC1090
source "$KEYS_FILE"

echo "Applying terraform/eks (the cluster takes 10-15 minutes)..."
terraform apply -auto-approve "${TF_VARS[@]}"

CLUSTER=$(terraform output -raw cluster_name)
REGION=$(terraform output -raw region)
VPC_ID=$(terraform output -raw vpc_id)
VALUES_FILE=$(mktemp)
trap 'rm -f "$VALUES_FILE"' EXIT
terraform output -raw helm_values > "$VALUES_FILE"

aws eks update-kubeconfig --name "$CLUSTER" --region "$REGION" >/dev/null
kubectl get nodes

echo "Installing the AWS Load Balancer Controller $LB_CONTROLLER_CHART_VERSION..."
helm repo add eks https://aws.github.io/eks-charts >/dev/null
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts >/dev/null
helm repo update >/dev/null
# Its credentials come from the Pod Identity association terraform/eks made
# for kube-system/aws-load-balancer-controller.
helm upgrade --install aws-load-balancer-controller eks/aws-load-balancer-controller \
  --version "$LB_CONTROLLER_CHART_VERSION" -n kube-system \
  --set clusterName="$CLUSTER" --set region="$REGION" --set vpcId="$VPC_ID" \
  --set serviceAccount.create=true --set serviceAccount.name=aws-load-balancer-controller \
  --wait

echo "Installing kube-prometheus-stack $KUBE_PROMETHEUS_STACK_VERSION (Prometheus, Grafana, Alertmanager)..."
# The selectors are opened so Prometheus picks up the gate chart's
# ServiceMonitors and PrometheusRule without a release label on them.
helm upgrade --install kube-prometheus-stack prometheus-community/kube-prometheus-stack \
  --version "$KUBE_PROMETHEUS_STACK_VERSION" -n monitoring --create-namespace \
  --set prometheus.prometheusSpec.serviceMonitorSelectorNilUsesHelmValues=false \
  --set prometheus.prometheusSpec.ruleSelectorNilUsesHelmValues=false \
  --set prometheus.prometheusSpec.retention=30d \
  --set grafana.sidecar.dashboards.enabled=true \
  --set grafana.sidecar.dashboards.searchNamespace=ALL \
  --wait

kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
# /metrics needs an AdminMetrics key; Prometheus reads it from this Secret.
kubectl -n "$NAMESPACE" create secret generic gate-metrics-key \
  --from-literal=api-key="$ADMIN_API_KEY" --dry-run=client -o yaml | kubectl apply -f - >/dev/null

echo "Installing the gate chart..."
helm upgrade --install "$RELEASE" "$PROJECT_ROOT/deploy/helm/gate" -n "$NAMESPACE" \
  -f "$VALUES_FILE" \
  --set metrics.serviceMonitor.enabled=true \
  --set metrics.prometheusRule.enabled=true \
  --set metrics.grafanaDashboards.enabled=true \
  --wait --timeout 10m

echo "Waiting for the ALB..."
HOST=""
for _ in $(seq 1 60); do
  HOST=$(kubectl -n "$NAMESPACE" get ingress "$RELEASE" -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null || true)
  [ -n "$HOST" ] && break
  sleep 10
done
[ -n "$HOST" ] || { echo "Error: the ALB has no hostname after 10 minutes; check: kubectl -n $NAMESPACE describe ingress $RELEASE" >&2; exit 1; }
SCHEME=http; [ -n "${CERTIFICATE_ARN:-}" ] && SCHEME=https
URL="$SCHEME://$HOST"
for _ in $(seq 1 60); do
  curl -sf --max-time 5 "$URL/ready" >/dev/null && break
  sleep 10
done
curl -sf "$URL/health" && echo

cat <<DONE

Gate is up at $URL ($(kubectl -n "$NAMESPACE" get deploy "$RELEASE" -o jsonpath='{.status.readyReplicas}') replicas).

  source .eks-keys.env && make APP_URL=$URL correctness     # invariants A-D across the replicas
  kubectl -n monitoring port-forward svc/kube-prometheus-stack-grafana 3000:80
     # Grafana: dashboards "Gate — SLOs" and "Gate — Rate Limiting & Quotas"
     # admin password: kubectl -n monitoring get secret kube-prometheus-stack-grafana -o jsonpath='{.data.admin-password}' | base64 -d

Tear it all down with: ./scripts/teardown-eks.sh
DONE
