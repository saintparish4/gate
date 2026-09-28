#!/usr/bin/env bash
# check-monitoring-metrics.sh - every app metric the Terraform dashboard and
# alarms reference must be one the service emits, queried with the Environment
# dimension the service adds to every datum.
#
# The dashboard and alarms had drifted from the code without anything noticing:
#
#   DegradedOperation    charted, while the app emits RateLimitDegraded
#   CacheHitRate         charted, while no cache is wired in
#   CircuitBreakerState  alarmed with no dimensions, so it watched a series
#                        that never existed; the dashboard asked for breakers
#                        named "dynamodb" and "kinesis", and the only one is
#                        "dynamodb-ratelimit"
#
# CloudWatch matches a metric by name plus its exact dimension set, so each of
# these was an empty panel or an alarm that could never fire. This checks the
# names and the Environment dimension; it cannot check every other dimension,
# so keep each reference next to the call that emits it in mind when editing.
#
# A name counts as emitted when it appears as a string literal in
# src/main/scala, so a helper that is defined but never called would still
# pass. Delete dead emitters rather than leave them: the unwired
# recordDegradedOperation and recordCacheMetrics were removed for this reason.
#
# Exits 1 on any mismatch, so it can gate CI.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

MONITORING="$ROOT/terraform/modules/monitoring/main.tf"
SRC="$ROOT/src/main/scala"

for f in "$MONITORING" "$SRC"; do
  [ -e "$f" ] || { echo "Error: missing $f" >&2; exit 1; }
done

# One line per reference: "<metric name><TAB><has Environment: yes|no><TAB><where>".
references() {
  # Dashboard rows that name the app namespace directly.
  grep -n '\[local\.namespace, "' "$MONITORING" | while IFS= read -r line; do
    name=$(printf '%s' "$line" | sed -E 's/.*\[local\.namespace, "([A-Za-z0-9_]+)".*/\1/')
    env=no; printf '%s' "$line" | grep -q '"Environment"' && env=yes
    printf '%s\t%s\tline %s (dashboard)\n' "$name" "$env" "${line%%:*}"
  done

  # SEARCH expressions over the app namespace.
  grep -n 'SEARCH(' "$MONITORING" | while IFS= read -r line; do
    printf '%s' "$line" | grep -q 'search_ns' || continue
    name=$(printf '%s' "$line" | sed -E 's/.*MetricName=\\"([A-Za-z0-9_]+)\\".*/\1/')
    env=no; printf '%s' "$line" | grep -q 'Environment=\\"' && env=yes
    printf '%s\t%s\tline %s (dashboard search)\n' "$name" "$env" "${line%%:*}"
  done

  # Alarms: pair each metric_name with the namespace that follows it, and note
  # whether the alarm sets an Environment dimension.
  awk '
    /^resource "aws_cloudwatch_metric_alarm"/ { inalarm = 1; n = 0; env = "no"; start = NR; next }
    inalarm && /metric_name *=/ {
      match($0, /"[A-Za-z0-9_]+"/); pending = substr($0, RSTART + 1, RLENGTH - 2); next
    }
    inalarm && /namespace *=/ && pending != "" {
      if ($0 ~ /local\.namespace/) { names[++n] = pending }
      pending = ""; next
    }
    inalarm && /Environment *=/ { env = "yes" }
    inalarm && /^}/ {
      for (i = 1; i <= n; i++) printf "%s\t%s\tline %d (alarm)\n", names[i], env, start
      inalarm = 0
    }
  ' "$MONITORING"
}

REFS=$(references)
status=0

printf 'App metrics referenced by terraform/modules/monitoring: %s\n\n' "$(echo "$REFS" | grep -c .)"

while IFS=$'\t' read -r name env where; do
  [ -n "$name" ] || continue
  if ! grep -rqF "\"$name\"" "$SRC"; then
    printf 'NOT EMITTED   %-26s %s\n' "$name" "$where"
    status=1
  elif [ "$env" != yes ]; then
    printf 'NO ENVIRONMENT %-25s %s\n' "$name" "$where"
    status=1
  else
    printf 'ok            %-26s %s\n' "$name" "$where"
  fi
done <<< "$REFS"

echo
if [ "$status" -ne 0 ]; then
  echo "FAIL: the monitoring module references metrics the app does not emit, or omits"
  echo "the Environment dimension every datum carries. CloudWatch would show nothing."
else
  echo "OK: every app metric in the monitoring module is emitted, with Environment."
fi
exit "$status"
