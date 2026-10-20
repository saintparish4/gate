#!/usr/bin/env bash
# kind-rehearsal.sh - install the Helm chart on a local kind cluster, against
# LocalStack, with real (random) keys in Secrets Manager, and run the
# correctness invariants across its replicas.
#
# It checks what `helm template` cannot: that the pods start under the chart's
# security context (non-root, read-only root filesystem), that the probes pass,
# that the service reads its keys from Secrets Manager, and that two replicas
# behind one Service keep the invariants. It does not check anything
# AWS-specific: IAM, Pod Identity, the ALB and its TLS rule are EKS's.
#
#   bash scripts/kind-rehearsal.sh          # create, install, run, keep the cluster
#   KEEP=0 bash scripts/kind-rehearsal.sh   # and delete the cluster afterwards
#
# Needs: docker, kind, kubectl, helm, sbt, openssl, python3.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CLUSTER="${CLUSTER:-gate-rehearsal}"
NS=gate
URL="http://localhost:18080"
COMMIT=$(git -C "$ROOT" describe --always --dirty 2>/dev/null || echo unknown)

for tool in docker kind kubectl helm sbt openssl python3; do
  command -v "$tool" >/dev/null 2>&1 || { echo "Error: $tool not found in PATH." >&2; exit 1; }
done

echo "Building images ($COMMIT)..."
docker build -q --build-arg GIT_COMMIT="$COMMIT" -t gate-rehearsal-server:local "$ROOT" >/dev/null
docker build -q -f "$ROOT/aggregator.Dockerfile" -t gate-rehearsal-aggregator:local "$ROOT" >/dev/null
docker build -q -f "$ROOT/localstack.Dockerfile" -t gate-rehearsal-localstack:local "$ROOT" >/dev/null

if ! kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  kind create cluster --name "$CLUSTER" --config "$ROOT/deploy/kind/cluster.yaml" --wait 120s
fi
kubectl config use-context "kind-$CLUSTER" >/dev/null
for image in gate-rehearsal-server:local gate-rehearsal-aggregator:local gate-rehearsal-localstack:local; do
  kind load docker-image --name "$CLUSTER" "$image"
done

kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl apply -f "$ROOT/deploy/kind/localstack.yaml"
kubectl -n "$NS" rollout status deploy/localstack --timeout=300s

# Random keys, written where the service will look: <prefix>/<environment>/api-keys.
API_KEY="gate_$(openssl rand -hex 24)"
FREE_API_KEY="gate_$(openssl rand -hex 24)"
ADMIN_API_KEY="gate_$(openssl rand -hex 24)"
STANDARD='"ratelimit_check","ratelimit_status","idempotency_check","idempotency_complete","quota_check","quota_reconcile"'
key_json() {
  printf '{"apiKey":"%s","apiKeyId":"%s","clientName":"%s","tier":"%s","permissions":[%s],"active":true}' "$1" "$2" "$3" "$4" "$5"
}
KEYS_JSON="[$(key_json "$API_KEY" key_kind_api "Kind client" premium "$STANDARD"),$(key_json "$FREE_API_KEY" key_kind_free "Kind free client" free "$STANDARD"),$(key_json "$ADMIN_API_KEY" key_kind_admin "Kind admin" enterprise "$STANDARD,\"admin_metrics\"")]"
kubectl -n "$NS" exec deploy/localstack -- sh -c \
  "awslocal secretsmanager create-secret --name gate/kind/api-keys --secret-string '$KEYS_JSON' >/dev/null 2>&1 || awslocal secretsmanager put-secret-value --secret-id gate/kind/api-keys --secret-string '$KEYS_JSON' >/dev/null"

helm upgrade --install gate "$ROOT/deploy/helm/gate" -n "$NS" \
  -f "$ROOT/deploy/helm/gate/ci/kind-values.yaml" --wait --timeout 10m
kubectl -n "$NS" get pods -o wide

curl -sf "$URL/health" && echo
API_KEY="$API_KEY" FREE_API_KEY="$FREE_API_KEY" make -C "$ROOT" APP_URL="$URL" correctness
status=$?

echo "Requests per replica:"
for pod in $(kubectl -n "$NS" get pods -l app.kubernetes.io/component=server -o name); do
  n=$(kubectl -n "$NS" exec "$pod" -- curl -s -H "Authorization: Bearer $ADMIN_API_KEY" localhost:8080/metrics \
    | python3 -c 'import sys; print(int(sum(float(l.split()[-1]) for l in sys.stdin if l.startswith("gate_http_requests_total{route=\"/v1"))))')
  echo "  $pod: $n"
done

if [ "${KEEP:-1}" = "0" ]; then kind delete cluster --name "$CLUSTER"; fi
exit $status
