#!/usr/bin/env bash
# Capture one representative end-to-end OTel trace from Jaeger and write it to
# docs/exemplar-trace.json. Intended to be run once after a change that should
# be reflected in the committed exemplar.
#
# Requires the obs profile to be running:
#   docker compose --profile obs up -d
#
# Usage:
#   ./scripts/export-exemplar-trace.sh              # uses default Jaeger URL
#   JAEGER_URL=http://localhost:16686 ./scripts/export-exemplar-trace.sh
set -euo pipefail

JAEGER_URL="${JAEGER_URL:-http://localhost:16686}"
SERVICE="${OTEL_SERVICE_NAME:-gate}"
OUT="${OUT:-docs/exemplar-trace.json}"
API_KEY="${GATE_API_KEY:-test-api-key}"
BASE_URL="${GATE_BASE_URL:-http://localhost:8080}"

echo "==> Driving a small burst of traffic against $BASE_URL ..."
for i in $(seq 1 20); do
  curl -s -o /dev/null -X POST "$BASE_URL/v1/ratelimit/check" \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $API_KEY" \
    -d "{\"key\":\"exemplar-trace-user-$i\",\"cost\":1}"
done

echo "==> Waiting 3s for Jaeger to index the spans ..."
sleep 3

echo "==> Querying Jaeger at $JAEGER_URL for service=$SERVICE ..."
TRACES_JSON="$(curl -sf "$JAEGER_URL/api/traces?service=$SERVICE&limit=50")"

# Pick the trace with the most spans (likely the most interesting path).
TRACE_ID="$(printf '%s' "$TRACES_JSON" | \
  python3 -c '
import json, sys
data = json.load(sys.stdin)
traces = data.get("data", [])
if not traces:
    sys.exit("no traces found")
best = max(traces, key=lambda t: len(t.get("spans", [])))
print(best["traceID"])
')"

echo "==> Best trace ID: $TRACE_ID"
echo "==> Writing $OUT ..."
curl -sf "$JAEGER_URL/api/traces/$TRACE_ID" | \
  python3 -m json.tool > "$OUT"

SPAN_COUNT="$(python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); print(len(d["data"][0]["spans"]))' "$OUT")"
echo "==> Done. Trace $TRACE_ID ($SPAN_COUNT spans) written to $OUT"
