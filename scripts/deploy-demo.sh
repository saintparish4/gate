#!/bin/bash
# deploy-demo.sh - Spin up full stack in ~5 minutes

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
# shellcheck source=lib-terraform.sh
source "$SCRIPT_DIR/lib-terraform.sh"

echo "Deploying rate limiter demo environment..."

# Check for required environment variable
if [ -z "${ECR_IMAGE:-}" ]; then
  echo "Error: ECR_IMAGE environment variable is required"
  echo "   Build and push it first (./scripts/bootstrap.sh creates the repository):"
  echo "     export ECR_IMAGE=\$(./scripts/publish-image.sh)"
  exit 1
fi

# Check for AWS credentials
if ! aws sts get-caller-identity > /dev/null 2>&1; then
  echo "Error: AWS credentials not configured"
  echo "   Run: aws configure"
  exit 1
fi

require_terraform
require_backend

# Deploy infrastructure. State lives in S3 under the demo's own key.
cd "$PROJECT_ROOT/terraform"

echo "Initializing Terraform..."
tf_init "gate/demo/terraform.tfstate"

TF_VARS=(
  -var-file=environments/demo.tfvars
  -var="ecs_desired_count=1"
  -var="container_image=${ECR_IMAGE}"
  -var="enable_autoscaling=false"
  -var="enable_kinesis_firehose=false"
)

# API keys cross the ALB in every request. With CERTIFICATE_ARN the demo serves
# HTTPS; without it, the ALB serves HTTP to this machine's public IP only (or
# ALB_INGRESS_CIDR), because Terraform refuses plaintext to the whole internet.
if [ -n "${CERTIFICATE_ARN:-}" ]; then
  TF_VARS+=(-var="certificate_arn=${CERTIFICATE_ARN}")
  echo "Serving HTTPS with ${CERTIFICATE_ARN}"
else
  CIDR="${ALB_INGRESS_CIDR:-}"
  if [ -z "$CIDR" ]; then
    MY_IP=$(curl -sf --max-time 10 https://checkip.amazonaws.com | tr -d '[:space:]')
    [ -n "$MY_IP" ] || { echo "Error: could not detect your public IP; set ALB_INGRESS_CIDR."; exit 1; }
    CIDR="$MY_IP/32"
  fi
  TF_VARS+=(-var="alb_ingress_cidrs=[\"$CIDR\"]")
  echo "No certificate: the ALB serves HTTP to $CIDR only."
fi

# The app refuses the built-in keys outside docker compose, and refuses to start
# until the keys secret holds an active key. So the secret comes first and gets
# keys before the service exists to read them.
echo "Creating the API keys secret..."
terraform apply -auto-approve "${TF_VARS[@]}" -target=module.secrets

SECRET_ID=$(terraform output -raw api_keys_secret_name)
KEYS_FILE="$PROJECT_ROOT/.demo-keys.env"

# One key entry. The keys are hex, so nothing in them needs JSON escaping.
key_json() { # apiKey apiKeyId clientName tier permissions
  printf '{"apiKey":"%s","apiKeyId":"%s","clientName":"%s","tier":"%s","permissions":[%s],"active":true}' \
    "$1" "$2" "$3" "$4" "$5"
}

if aws secretsmanager get-secret-value --secret-id "$SECRET_ID" \
     --query SecretString --output text | grep -Eq '"active" *: *true'; then
  echo "$SECRET_ID already holds active keys; keeping them."
  [ -f "$KEYS_FILE" ] || echo "   $KEYS_FILE is missing; read the keys with: aws secretsmanager get-secret-value --secret-id $SECRET_ID"
else
  echo "Writing fresh demo keys into $SECRET_ID..."
  API_KEY="gate_$(openssl rand -hex 24)"
  FREE_API_KEY="gate_$(openssl rand -hex 24)"
  ADMIN_API_KEY="gate_$(openssl rand -hex 24)"
  STANDARD='"ratelimit_check","ratelimit_status","idempotency_check","idempotency_complete","quota_check","quota_reconcile"'
  ADMIN="$STANDARD,\"admin_metrics\",\"admin_config\""
  KEYS_JSON="[$(key_json "$API_KEY" key_demo_api "Demo client" premium "$STANDARD"),$(key_json "$FREE_API_KEY" key_demo_free "Demo free client" free "$STANDARD"),$(key_json "$ADMIN_API_KEY" key_demo_admin "Demo admin" enterprise "$ADMIN")]"
  aws secretsmanager put-secret-value --secret-id "$SECRET_ID" \
    --secret-string "$KEYS_JSON" > /dev/null
  (
    umask 077
    cat > "$KEYS_FILE" <<KEYS
# Demo API keys, written by scripts/deploy-demo.sh. Load them with:
#   source .demo-keys.env
export API_KEY=$API_KEY
export FREE_API_KEY=$FREE_API_KEY
export ADMIN_API_KEY=$ADMIN_API_KEY
KEYS
  )
  echo "Saved the keys to $KEYS_FILE (gitignored, mode 600)."
fi

echo "Applying Terraform configuration..."
terraform apply -auto-approve "${TF_VARS[@]}"

# Wait for healthy
API=$(terraform output -raw api_endpoint)
echo "Waiting for service to be healthy..."

MAX_WAIT=600  # 10 minutes
INTERVAL=10   # Check every 10 seconds
ELAPSED=0

while [ $ELAPSED -lt $MAX_WAIT ]; do
  # /ready, not /health: /health returns 200 as soon as the process is up, even
  # with every backend unreachable, so waiting on it would report success on a
  # deployment that cannot serve a single request.
  # "ok", not just 200: /ready answers 200 "degraded" while Kinesis is
  # unreachable, which serves traffic but is not a finished deploy.
  # -k: with a certificate, it names your domain rather than the ALB's DNS
  # name. Only the unauthenticated probes skip the name check.
  if curl -sfk --max-time 5 "$API/ready" 2>/dev/null | grep -q '"status":"ok"'; then
    echo "Service is ready (dependencies reachable)!"
    break
  fi

  # Distinguish "not up yet" from "up but dependencies unreachable". /health
  # answering while /ready does not means the process is fine and DynamoDB or
  # Kinesis is not -- usually IAM or the VPC endpoints, not a slow boot.
  if curl -sfk --max-time 5 "$API/health" > /dev/null 2>&1; then
    echo "Process is up but dependencies are not ready yet... (${ELAPSED}s/${MAX_WAIT}s)"
  else
    echo "Service not yet available, waiting... (${ELAPSED}s/${MAX_WAIT}s)"
  fi

  sleep $INTERVAL
  ELAPSED=$((ELAPSED + INTERVAL))
done

if [ $ELAPSED -ge $MAX_WAIT ]; then
  echo "Timeout: Service did not become ready within ${MAX_WAIT} seconds"
  echo "   Check ECS service status and CloudWatch logs. If /health answers but"
  echo "   /ready does not, the component list says which backend is failing:"
  echo "     curl -s $API/ready"
  exit 1
fi

echo ""
echo "Demo ready at: $API"
echo ""
echo "Cost: roughly \$0.25/hour in us-east-1 - 2 NAT gateways (\$0.09), five"
echo "interface VPC endpoints across 2 AZs (\$0.10), ALB (\$0.023), Fargate"
echo "256/512 (\$0.012), one Kinesis shard (\$0.015). About \$6/day if left up."
echo ""
echo "TEAR IT DOWN WHEN DONE:  ./scripts/teardown-demo.sh"
echo ""
echo "Quick test (the built-in keys are refused here; use the demo's own):"
echo "   source .demo-keys.env"
echo "   curl $API/ready"
echo "   curl -X POST $API/v1/ratelimit/check \\"
echo "     -H 'Content-Type: application/json' \\"
echo "     -H \"Authorization: Bearer \$API_KEY\" \\"
echo "     -d '{\"key\":\"demo\",\"cost\":1}'"
echo ""
echo "Correctness invariants against the demo:"
echo "   source .demo-keys.env && make APP_URL=$API correctness"
