#!/bin/bash 

# Load Testing script for Rate Limiter Platform
# Uses k6 for load testing with multiple scenarios 
# 
# Usage:
# ./scripts/load-test.sh <environment> [test-type]
#
# Examples:
# ./scripts/load-test.sh dev quick  # 30 second quick test
# ./scripts/load-test.sh dev  # Quick smoke test 
# ./scripts/load-test.sh staging baseline # Baseline test 
# ./scripts/load-test.sh prod stress # Stress test (careful!) 
#
# Environment:
#   API_KEY   the key to send (default: the built-in test-api-key, which only
#             docker compose serves). Against a deployed stack:
#               source .demo-keys.env && APP_URL=http://<alb> ./scripts/load-test.sh demo quick
#   APP_URL   the base URL. Without it the script looks up the ALB named
#             rate-limiter-<environment>, and falls back to localhost:8080.
#
# This is a smoke-level load test, not a benchmark: one run, no repetitions.
# Its latency thresholds are loose "is it broken" bounds. A p99 over a few
# hundred requests is the third-worst request, and moves a lot between runs.

set -euo pipefail

# Configuration
ENVIRONMENT="${1:-dev}"
TEST_TYPE="${2:-baseline}"
RESULTS_DIR="./test-results/$(date +%Y%m%d-%H%M%S)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Get ALB DNS based on environment
get_alb_dns() {
    local env=$1
    aws elbv2 describe-load-balancers \
        --names "rate-limiter-${env}" \
        --query 'LoadBalancers[0].DNSName' \
        --output text \
        --region us-east-1 2>/dev/null || echo "localhost:8080"
}

# Check if k6 is installed
check_k6() {
    if ! command -v k6 &> /dev/null; then
        echo -e "${RED}k6 is not installed${NC}"
        echo "Install k6: https://k6.io/docs/getting-started/installation/"
        echo ""
        echo "macOS: brew install k6"
        echo "Linux: sudo apt-get install k6"
        exit 1
    fi
}

# Create results directory
mkdir -p "$RESULTS_DIR"

echo -e "${GREEN}=== Rate Limiter Load Test ===${NC}"
echo "Environment: $ENVIRONMENT"
echo "Test Type: $TEST_TYPE"
echo "Results: $RESULTS_DIR"
echo ""

# Get target URL
BASE_URL="${APP_URL:-http://$(get_alb_dns "$ENVIRONMENT")}"
# The key reaches k6 through the environment, never the command line, so it is
# not echoed and does not show in the process list. It was hard-coded to the
# built-in key, which a deployed stack refuses: every request answered 401.
export API_KEY="${API_KEY:-test-api-key}"
echo "Target: $BASE_URL"
echo ""

# Verify service is healthy
echo "Verifying service health..."
if curl -sf "$BASE_URL/health" > /dev/null; then
    echo -e "${GREEN}✓ Service is healthy${NC}"
else
    echo -e "${RED}✗ Service health check failed${NC}"
    exit 1
fi
echo ""

# Run k6 load test
check_k6

# Create k6 test script
cat > "$RESULTS_DIR/test-script.js" << 'EOF'
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Trend, Counter } from 'k6/metrics';

// Custom metrics
const rateLimitAllowed = new Counter('rate_limit_allowed');
const rateLimitRejected = new Counter('rate_limit_rejected');
const idempotencyNew = new Counter('idempotency_new');
const idempotencyDuplicate = new Counter('idempotency_duplicate');
const latencyTrend = new Trend('custom_latency');
// Answers made by degradation mode (X-Gate-Degraded), which never reached the
// store. Any at all means the run measured an outage, not the service.
const degradedAnswers = new Rate('degraded_answers');

// Test configuration from environment
const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const TEST_TYPE = __ENV.TEST_TYPE || 'baseline';
const API_KEY = __ENV.API_KEY || 'test-api-key';

// A 429 is a decision, and a 202 is an idempotency key in progress. k6 counts
// every status of 400 and above as a failed request unless told otherwise, so
// a limiter doing its job would have failed the error-rate threshold.
http.setResponseCallback(http.expectedStatuses(200, 202, 429));

// Warmup scenario - runs before main test
// Increased duration to reduce cold starts in local Docker/LocalStack environment
const warmupScenario = {
    executor: 'constant-vus',
    vus: 5,
    duration: '30s', // Increased from 10s to better warm up LocalStack/DynamoDB
    startTime: '0s',
};

// Test scenarios
const scenarios = {
    quick: {
        executor: 'constant-vus',
        vus: 10,
        duration: '30s',
        startTime: '30s', // Start after warmup
    },
    smoke: {
        executor: 'constant-vus',
        vus: 5,
        duration: '1m',
        startTime: '30s', // Start after warmup
    },
    baseline: {
        executor: 'ramping-vus',
        startVUs: 0,
        startTime: '30s', // Start after warmup
        stages: [
            { duration: '2m', target: 50 },   // Ramp up
            { duration: '5m', target: 50 },   // Stay at 50 users
            { duration: '2m', target: 100 },  // Ramp to 100
            { duration: '5m', target: 100 },  // Stay at 100
            { duration: '2m', target: 0 },    // Ramp down
        ],
    },
    stress: {
        executor: 'ramping-vus',
        startVUs: 0,
        startTime: '30s', // Start after warmup
        stages: [
            { duration: '2m', target: 100 },
            { duration: '5m', target: 100 },
            { duration: '2m', target: 200 },
            { duration: '5m', target: 200 },
            { duration: '2m', target: 300 },
            { duration: '5m', target: 300 },
            { duration: '2m', target: 0 },
        ],
    },
    spike: {
        executor: 'ramping-vus',
        startVUs: 0,
        startTime: '30s', // Start after warmup
        stages: [
            { duration: '10s', target: 100 },
            { duration: '1m', target: 100 },
            { duration: '10s', target: 1000 }, // Spike!
            { duration: '3m', target: 1000 },
            { duration: '10s', target: 100 },
            { duration: '3m', target: 100 },
            { duration: '10s', target: 0 },
        ],
    },
    soak: {
        executor: 'constant-vus',
        vus: 100,
        duration: '30m', // Long-running
        startTime: '30s', // Start after warmup
    },
    // highContention: all VUs target a single fixed key, stressing OCC retry logic.
    // Measures: throughput degradation, OCC retry rate (RateLimitOCCRetry metric),
    // tail latency under contention. Compare with baseline to see the cost of hot keys.
    highContention: {
        executor: 'constant-vus',
        vus: 50,
        duration: '60s',
        startTime: '30s', // Start after warmup
    },
};

export const options = {
    scenarios: {
        warmup: warmupScenario,
        [TEST_TYPE]: scenarios[TEST_TYPE],
    },
    thresholds: {
        // Applied to the main scenario only, not the warmup.
        //
        // The latency bounds say "not broken", nothing finer: one run of a few
        // hundred requests cannot resolve more. There used to be a p(99)<200
        // per endpoint as well, tighter than the p(99)<2000 beside it and
        // failed by a normal LocalStack run, so a quick test rarely passed.
        [`http_req_duration{scenario:${TEST_TYPE}}`]: ['p(95)<500', 'p(99)<2000'],
        [`http_req_failed{scenario:${TEST_TYPE}}`]: ['rate<0.01'],  // Error rate < 1%
        // Every answer has to be a real decision.
        checks: ['rate>0.99'],
        degraded_answers: ['rate==0'],
    },
    summaryTrendStats: ['min', 'avg', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

// Share of hot-key checks that were refused. A refusal is an empty bucket or
// an exhausted OCC retry; the two cannot be told apart from the answer, so
// this is not an OCC rate (RateLimitOCCAttempts in CloudWatch is).
const hotKeyRefused = new Rate('hot_key_refused');

// Test data generators
function randomUserId() {
    return `user:${Math.floor(Math.random() * 1000)}`;
}

function randomIdempotencyKey() {
    return `operation:${Math.random().toString(36).substring(7)}`;
}

// Rate limit check
export function rateLimitCheck() {
    const payload = JSON.stringify({
        key: randomUserId(),
        cost: 1,
    });

    const params = {
        headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${API_KEY}`,
        },
        tags: { endpoint: 'ratelimit' },
    };

    const startTime = Date.now();
    const res = http.post(`${BASE_URL}/v1/ratelimit/check`, payload, params);
    const duration = Date.now() - startTime;

    latencyTrend.add(duration);
    degradedAnswers.add(res.headers['X-Gate-Degraded'] === 'true');

    const checks = check(res, {
        'status is 200 or 429': (r) => r.status === 200 || r.status === 429,
        'has allowed field': (r) => {
            try {
                const body = JSON.parse(r.body);
                return 'allowed' in body;
            } catch {
                return false;
            }
        },
    });
    
    // Latency check is tracked separately via thresholds, not as a hard failure
    // Only log if there's an actual HTTP error, not just slow requests
    if (!checks && res.status >= 400) {
        console.error(`HTTP error: ${res.status} - ${res.body.substring(0, 100)}`);
    }

    // Track allowed vs rejected. A refusal is a 429; it used to be looked for
    // in a 200, so it was never counted.
    if (res.status === 200) {
        rateLimitAllowed.add(1);
    } else if (res.status === 429) {
        rateLimitRejected.add(1);
    }

    return res;
}

// Idempotency check
export function idempotencyCheck() {
    const key = randomIdempotencyKey();
    
    const payload = JSON.stringify({
        idempotencyKey: key,
        ttl: 3600,
    });

    const params = {
        headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${API_KEY}`,
        },
        tags: { endpoint: 'idempotency' },
    };

    const res = http.post(`${BASE_URL}/v1/idempotency/check`, payload, params);

    const checks = check(res, {
        // 202 is a key another request has in progress: a valid answer.
        'status is 200 or 202': (r) => r.status === 200 || r.status === 202,
        'has status field': (r) => {
            try {
                const body = JSON.parse(r.body);
                return 'status' in body;
            } catch {
                return false;
            }
        },
    });
    
    // Only log actual HTTP errors, not validation failures
    if (!checks && res.status >= 400) {
        console.error(`HTTP error: ${res.status} - ${res.body.substring(0, 100)}`);
    }

    // Track new vs duplicate
    if (res.status === 200) {
        const body = JSON.parse(res.body);
        if (body.status === 'new') {
            idempotencyNew.add(1);
        } else if (body.status === 'duplicate') {
            idempotencyDuplicate.add(1);
        }
    }

    return res;
}

// highContention: single fixed key, no sleep, maximum pressure on OCC retry path
export function highContentionCheck() {
    const FIXED_KEY = 'contention:single-hot-key';
    const payload = JSON.stringify({
        key: FIXED_KEY,
        cost: 1,
    });

    const params = {
        headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${API_KEY}`,
        },
        tags: { endpoint: 'ratelimit', scenario: 'highContention' },
    };

    const res = http.post(`${BASE_URL}/v1/ratelimit/check`, payload, params);

    check(res, {
        'status is 200 or 429': (r) => r.status === 200 || r.status === 429,
    });

    degradedAnswers.add(res.headers['X-Gate-Degraded'] === 'true');
    if (res.status === 200) {
        rateLimitAllowed.add(1);
        hotKeyRefused.add(false);
    } else if (res.status === 429) {
        rateLimitRejected.add(1);
        hotKeyRefused.add(true);
    }

    // No sleep — we want maximum concurrency pressure on this single key
}

// Main test function
export default function() {
    if (TEST_TYPE === 'highContention') {
        highContentionCheck();
        return;
    }

    // Mix of rate limit and idempotency checks (80/20 split) for all other scenarios
    const rand = Math.random();
    
    if (rand < 0.8) {
        rateLimitCheck();
    } else {
        idempotencyCheck();
    }

    // Think time (simulate realistic usage)
    sleep(Math.random() * 2);
}

// Setup function (runs once per VU)
export function setup() {
    // Warmup request
    const res = http.get(`${BASE_URL}/health`);
    check(res, {
        'warmup successful': (r) => r.status === 200,
    });
    
    return { startTime: Date.now() };
}

// Teardown function (runs once at end)
export function teardown(data) {
    const duration = (Date.now() - data.startTime) / 1000;
    console.log(`Test completed in ${duration.toFixed(2)} seconds`);
}
EOF

# Run the test
echo -e "${YELLOW}Starting load test...${NC}"
echo ""

# A failed threshold makes k6 exit non-zero. Under `set -e` that ended the
# script right here, before the summary below was ever printed, so the summary
# only appeared for runs that had nothing to explain.
set +e
k6 run \
    --out json="$RESULTS_DIR/results.json" \
    --summary-export="$RESULTS_DIR/summary.json" \
    -e BASE_URL="$BASE_URL" \
    -e TEST_TYPE="$TEST_TYPE" \
    "$RESULTS_DIR/test-script.js" \
    2>&1 | tee "$RESULTS_DIR/output.log"

EXIT_CODE=${PIPESTATUS[0]}
set -e

echo ""
echo -e "${GREEN}=== Test Complete ===${NC}"
echo "Results saved to: $RESULTS_DIR"
echo ""

# Generate HTML report if possible
if command -v k6-reporter &> /dev/null; then
    echo "Generating HTML report..."
    k6-reporter "$RESULTS_DIR/results.json" \
        --output "$RESULTS_DIR/report.html"
    echo "Report: $RESULTS_DIR/report.html"
fi

# Parse and display key metrics
if [ -f "$RESULTS_DIR/summary.json" ]; then
    echo ""
    echo -e "${YELLOW}=== Key Metrics ===${NC}"
    
    # Use jq to parse summary if available
    if command -v jq &> /dev/null; then
        cat "$RESULTS_DIR/summary.json" | jq -r '
            "Requests: \(.metrics.http_reqs.values.count)",
            "Duration: \(.metrics.http_req_duration.values.avg)ms (avg)",
            "P95 Latency: \(.metrics.http_req_duration.values["p(95)"])ms",
            "P99 Latency: \(.metrics.http_req_duration.values["p(99)"])ms",
            "Error Rate: \(.metrics.http_req_failed.values.rate * 100)%",
            "RPS: \(.metrics.http_reqs.values.rate)"
        '
        
        echo ""
        echo "Custom Metrics:"
        cat "$RESULTS_DIR/summary.json" | jq -r '
            "Rate Limit Allowed: \(.metrics.rate_limit_allowed.values.count // 0)",
            "Rate Limit Rejected: \(.metrics.rate_limit_rejected.values.count // 0)",
            "Idempotency New: \(.metrics.idempotency_new.values.count // 0)",
            "Idempotency Duplicate: \(.metrics.idempotency_duplicate.values.count // 0)"
        '
    fi
fi

echo ""

# Check if test passed thresholds
if [ "$EXIT_CODE" -eq 0 ]; then
    echo -e "${GREEN}✓ All thresholds passed${NC}"
else
    echo -e "${RED}✗ Some thresholds failed (exit code: $EXIT_CODE)${NC}"
    echo "Check $RESULTS_DIR/output.log for details"
fi

echo ""
echo "To view detailed results:"
echo "  cat $RESULTS_DIR/summary.json | jq"
echo ""

exit "$EXIT_CODE"

