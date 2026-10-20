#!/usr/bin/env bash
# check-usage-pipeline.sh - check that the usage aggregator committed every
# event the service published, once, per event type (ADR-007).
#
# It compares two counters that should end equal once the aggregator catches up:
#   gate_events_published_total{event_type}  summed over every service replica
#   gate_usage_records_total{event_type}     the aggregator's, counted after commit
#
# Equal means nothing published was lost or counted twice between the service
# and the usage table. Dropped events (gate_events_dropped_total) never reach the
# stream, so the check reports them and calls the run inconclusive.
#
# Exits 0 when equal, 1 when they disagree after the wait, 2 when inconclusive.
#
# Environment:
#   ADMIN_API_KEY     a key holding AdminMetrics (default: the built-in admin-api-key)
#   AGGREGATOR_URL    default http://localhost:9095
#   APP_METRICS_URLS  space-separated /metrics URLs, one per replica. Without it,
#                     every replica of the compose service is asked from inside its
#                     container, so a scaled stack is summed correctly.
#   WAIT_SECONDS      how long to wait for the aggregator to catch up (default 90)
set -euo pipefail

ADMIN_API_KEY="${ADMIN_API_KEY:-admin-api-key}"
AGGREGATOR_URL="${AGGREGATOR_URL:-http://localhost:9095}"
WAIT_SECONDS="${WAIT_SECONDS:-90}"
COMPOSE="${COMPOSE:-docker compose}"

app_metrics() {
  if [ -n "${APP_METRICS_URLS:-}" ]; then
    for url in $APP_METRICS_URLS; do
      curl -sf -H "Authorization: Bearer $ADMIN_API_KEY" "$url"
    done
  else
    local n
    n=$($COMPOSE ps -q rate-limiter | wc -l)
    [ "$n" -gt 0 ] || { echo "No rate-limiter containers are running" >&2; exit 2; }
    for i in $(seq 1 "$n"); do
      $COMPOSE exec -T --index "$i" rate-limiter \
        curl -sf -H "Authorization: Bearer $ADMIN_API_KEY" http://localhost:8080/metrics
    done
  fi
}

# Sums a counter by its event_type label across every exposition on stdin.
sum_by_type() {
  python3 -c '
import re, sys, json
name = sys.argv[1]
totals = {}
pat = re.compile(r"^" + re.escape(name) + r"\{(.*)\}\s+(\S+)")
for line in sys.stdin:
    m = pat.match(line)
    if not m:
        continue
    labels = dict(re.findall(r"(\w+)=\"([^\"]*)\"", m.group(1)))
    t = labels.get("event_type", "")
    totals[t] = totals.get(t, 0) + float(m.group(2))
print(json.dumps({k: int(v) for k, v in totals.items() if v}))
' "$1"
}

dropped_total() {
  python3 -c '
import sys
print(int(sum(float(l.split()[-1]) for l in sys.stdin if l.startswith("gate_events_dropped_total"))))'
}

# The service counts an event published once Kinesis takes it, and its queue
# can still be draining after the traffic stops (LocalStack is slow). Equal
# counts mid-drain prove nothing about the rest, so the published side must
# also hold still across two reads, SETTLE_SECONDS apart, before it compares.
SETTLE_SECONDS="${SETTLE_SECONDS:-10}"
deadline=$(( $(date +%s) + WAIT_SECONDS ))
previous=""
while :; do
  app=$(app_metrics)
  published=$(printf '%s\n' "$app" | sum_by_type gate_events_published_total)
  dropped=$(printf '%s\n' "$app" | dropped_total)
  committed=$(curl -sf "$AGGREGATOR_URL/metrics" | sum_by_type gate_usage_records_total)
  [ "$published" = "$previous" ] && [ "$published" = "$committed" ] && break
  [ "$(date +%s)" -ge "$deadline" ] && break
  previous="$published"
  sleep "$SETTLE_SECONDS"
done

python3 - "$published" "$committed" "$dropped" <<'PY'
import json, sys
published, committed, dropped = json.loads(sys.argv[1]), json.loads(sys.argv[2]), int(sys.argv[3])
types = sorted(set(published) | set(committed))
print(f"{'event_type':<24}{'published':>12}{'committed':>12}")
for t in types:
    print(f"{t:<24}{published.get(t, 0):>12}{committed.get(t, 0):>12}")
print(f"{'dropped before Kinesis':<24}{dropped:>12}")
equal = published == committed
if dropped:
    print(f"INCONCLUSIVE: {dropped} events never reached the stream")
    sys.exit(2)
if not published:
    print("INCONCLUSIVE: nothing was published; send some traffic first")
    sys.exit(2)
if equal:
    print(f"PASS: every one of {sum(published.values())} published events was committed once")
    sys.exit(0)
print("FAIL: published and committed disagree (lag past the wait, a loss, or a double count)")
sys.exit(1)
PY
