#!/usr/bin/env bash
# check-env-drift.sh - compare the environment variables the application reads
# against what each environment actually sets.
#
# Three deploys in a row shipped a variable that docker-compose set and Terraform
# did not, and every one was invisible locally and in CI precisely because
# compose already set it:
#
#   DEGRADATION_MODE            a tripped breaker rejected every tenant for 30s
#   OTEL_EXPORTER_OTLP_ENDPOINT 73 doomed connections per load run, on a starved task
#   AUTH_RATE_LIMIT_PER_MINUTE  every correctness run 401'd on AWS at ~1000 req/min
#
# Each cost a deploy cycle to find. This finds them before one.
#
# Exits 1 if anything compose sets is absent from Terraform, so it can gate CI.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

APP_CONF="$ROOT/src/main/resources/application.conf"
COMPOSE="$ROOT/docker-compose.yml"
TF_MAIN="$ROOT/terraform/main.tf"
COMPOSE_SERVICE="${COMPOSE_SERVICE:-rate-limiter}"

for f in "$APP_CONF" "$COMPOSE" "$TF_MAIN"; do
  [ -f "$f" ] || { echo "Error: missing $f" >&2; exit 1; }
done

# Variables that are local-only on purpose. Each is a deliberate difference, not
# drift, so listing them here keeps the real signal readable.
LOCAL_ONLY=(
  USE_LOCALSTACK              # LocalStack switch; never set on AWS
  AWS_ENDPOINT                # LocalStack gateway
  DYNAMODB_ENDPOINT           # LocalStack gateway
  KINESIS_ENDPOINT            # LocalStack gateway
  AWS_ACCESS_KEY_ID           # dummy creds locally; task role on AWS
  AWS_SECRET_ACCESS_KEY       # dummy creds locally; task role on AWS
  TIMEOUT_RATE_LIMIT_CHECK    # widened locally: LocalStack is 10-50x slower
  TIMEOUT_IDEMPOTENCY_CHECK   # widened locally, same reason
)

# Variables Terraform sets that application.conf never reads because another
# library consumes them straight from the environment. Named here so the CHECK
# section only ever lists something unexplained; a check that fires on every
# run is a check nobody reads.
KNOWN_EXTERNAL=(
  OTEL_SDK_DISABLED           # opentelemetry-sdk-extension-autoconfigure; Main uses OtelJava.autoConfigured
)

is_known_external() {
  local v="$1"
  for k in "${KNOWN_EXTERNAL[@]}"; do [ "$v" = "$k" ] && return 0; done
  return 1
}

# ── extraction ───────────────────────────────────────────────────────────────

# Every ${?VAR} override in application.conf: the variables the app honours.
app_vars() {
  grep -oE '\$\{\?[A-Z_][A-Z0-9_]*\}' "$APP_CONF" \
    | sed -E 's/^\$\{\?//; s/\}$//' | sort -u
}

# The environment block of one compose service.
compose_vars() {
  awk -v svc="$COMPOSE_SERVICE" '
    /^  [a-zA-Z0-9_-]+:/ { inservice = ($0 ~ "^  " svc ":"); env = 0 }
    inservice && /^    environment:/ { env = 1; next }
    env && /^    [a-zA-Z]/ { env = 0 }
    env && /^      - [A-Z_]/ {
      line = $0
      sub(/^      - /, "", line)
      split(line, parts, "=")
      print parts[1]
    }
  ' "$COMPOSE" | sort -u
}

# The environment_variables map passed to the ECS module.
tf_vars() {
  awk '
    /environment_variables[ \t]*=[ \t]*\{/ { inblock = 1; next }
    inblock && /^  \}/ { inblock = 0 }
    inblock && /^[ \t]+[A-Z_][A-Z0-9_]*[ \t]*=/ {
      match($0, /[A-Z_][A-Z0-9_]*/)
      print substr($0, RSTART, RLENGTH)
    }
  ' "$TF_MAIN" | sort -u
}

is_local_only() {
  local v="$1"
  for l in "${LOCAL_ONLY[@]}"; do [ "$v" = "$l" ] && return 0; done
  return 1
}

# ── comparison ───────────────────────────────────────────────────────────────

APP=$(app_vars)
CMP=$(compose_vars)
TF=$(tf_vars)

printf 'Environment variables\n'
printf '  application.conf reads : %s\n' "$(echo "$APP" | grep -c .)"
printf '  docker-compose sets    : %s (service: %s)\n' "$(echo "$CMP" | grep -c .)" "$COMPOSE_SERVICE"
printf '  terraform sets         : %s\n\n' "$(echo "$TF" | grep -c .)"

status=0

# 1. The bug class: compose sets it, Terraform does not.
drift=""
while read -r v; do
  [ -n "$v" ] || continue
  if ! echo "$TF" | grep -qx "$v" && ! is_local_only "$v"; then
    drift+="  $v"$'\n'
  fi
done <<< "$CMP"

if [ -n "$drift" ]; then
  printf 'DRIFT: set by docker-compose, absent from Terraform\n'
  printf 'These behave one way locally and fall back to application defaults on AWS.\n'
  printf '%s\n' "$drift"
  status=1
else
  printf 'OK: every compose variable is either set in Terraform or deliberately local-only.\n\n'
fi

# 2. Supported by the app but set nowhere: a silent default. Often fine, but
#    DEGRADATION_MODE sat here while defaulting to reject-all.
unset_vars=""
while read -r v; do
  [ -n "$v" ] || continue
  if ! echo "$TF" | grep -qx "$v" && ! echo "$CMP" | grep -qx "$v"; then
    unset_vars+="  $v"$'\n'
  fi
done <<< "$APP"

if [ -n "$unset_vars" ]; then
  printf 'INFO: read by the app, set by neither -- running on application defaults\n'
  printf '%s\n' "$unset_vars"
fi

# 3. Set in Terraform but never read: dead config, or a typo in the name.
orphans=""
while read -r v; do
  [ -n "$v" ] || continue
  if ! echo "$APP" | grep -qx "$v" && ! is_known_external "$v"; then
    orphans+="  $v"$'\n'
  fi
done <<< "$TF"

if [ -n "$orphans" ]; then
  printf 'CHECK: set by Terraform, not read by application.conf\n'
  printf 'Either consumed elsewhere (AWS SDK, OTel SDK) or a misspelled name.\n'
  printf '%s\n' "$orphans"
fi

exit $status
