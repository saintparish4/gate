# Gate — Distributed Rate Limiting & Compliance Platform

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Scala](https://img.shields.io/badge/scala-3.7.4-red.svg)](https://www.scala-lang.org/)
[![CI](https://github.com/saintparish4/keyra/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/saintparish4/keyra/actions/workflows/ci.yml)

A distributed rate limiter, idempotency service, and token quota engine built with Scala 3, Cats Effect, and DynamoDB. Enforces per-key request limits and multi-level LLM token quotas correctly across multiple stateless instances — without a lock service.

### Why Gate?

- **Public or partner APIs** — Enforce per-tenant RPS and burst limits across many stateless containers with one source of truth in DynamoDB ([token bucket + OCC](#optimistic-concurrency-control-flow)).
- **AI / LLM gateways** — Stack rate limits with multi-level token quotas (user / agent / org) so spend and abuse stay bounded ([`POST /v1/quota/check`](#api-endpoints) when enabled with `TOKEN_QUOTA_ENABLED=true`; see [Token quotas](#token-quotas-ai-workloads)).
- **Money-moving or side-effecting workflows** — Idempotency keys so retries and double-clicks do not double-charge or double-ship ([`/v1/idempotency/*`](#api-endpoints); [behaviour](#idempotency)).

Run locally with LocalStack; see [Quickstart](#quickstart-in-5-minutes) below.

### What this system guarantees

| Guarantee | Mechanism |
|-----------|-----------|
| **At-most-X RPS per key, globally** | Token bucket (or leaky bucket) per key stored in DynamoDB; OCC (version-based conditional writes) ensures only one instance consumes tokens per logical state change. Tokens never exceed capacity; over any window, allowed requests respect the configured rate. |
| **Idempotent operations within TTL** | First-writer-wins via DynamoDB conditional `PutItem`. The first successful write for a key wins; duplicates within the TTL window receive the stored response. SHA-256 request fingerprinting detects same-key different-body conflicts (409). TTL auto-expiry prevents unbounded table growth. |
| **Multi-level token quotas** | Per-user, per-agent, and per-org LLM token quotas enforced simultaneously. Agent quota is capped at 80% of user quota. Pre-request estimation and post-response reconciliation within 5 seconds. |
| **Stateless instances** | All rate-limit, idempotency, and token quota state lives in DynamoDB. Any instance can serve any request; crashes and restarts lose no consistency. |

### What this system is designed to survive

| Failure mode | Behaviour |
|-------------|-----------|
| **Partial DynamoDB outage** | Circuit breaker (5 failures → open, 30s reset) stops hammering a failing store. Bulkhead limits concurrent DynamoDB calls to 100. Retry policy with exponential backoff (100ms base, 2× multiplier, max 10s). Configurable degradation mode: `reject-all` (fintech-safe default) or `allow-all` (AI infra). |
| **Instance crash** | No in-process state. The next instance reads current DynamoDB state and continues correctly. |
| **Kinesis failure** | Bounded in-memory queue drains to Kinesis; on transient failure one retry, then drop with `DroppedKinesisEvent` metric (also exported to Prometheus). The request path is never blocked waiting for Kinesis. |
| **OCC exhaustion under extreme contention** | After 10 failed conditional writes for the same key in the same window, the request is **rejected** (429). The service under-issues at the tail rather than over-issuing past the rate limit. |
| **Idempotency TOCTOU race** | If a conditional create fails and the subsequent read returns `None` (TTL deleted between calls), the operation retries up to 3 times instead of silently returning `New`. |

### Three hard problems this is built around

1. **Distributed token-bucket correctness** — coordinating token consumption across instances without a separate lock service. OCC on DynamoDB version fields is the mechanism; see [OCC flow](#optimistic-concurrency-control-flow) and [performance characteristics](docs/ARCHITECTURE.md).
2. **Idempotency under concurrency** — exactly-one-Created semantics when many concurrent callers hit the same key simultaneously. Conditional writes (`attribute_not_exists(pk)`) plus consistent reads are the mechanism; SHA-256 fingerprinting detects body mismatches. See [Idempotency](#idempotency).
3. **Back-pressure on event publishing** — Kinesis publishing must not block the request path or lose events silently. A bounded queue with a background drain fiber and observable drop counter is the mechanism; see [Metrics](#metrics).

---

**Note:** AWS deployment and performance have not been tested end-to-end; the stack is designed for AWS but validated locally with LocalStack/Docker. The deployment path is scripted and documented under [Demo environment](#demo-environment-scripted), but `terraform apply` has not yet been run against a real account.

## Architecture

```mermaid
flowchart LR
    Client[API Client] -->|HTTP| Server[HTTP4s Server]
    Server --> Auth[API Key Auth]
    Auth --> RateLimiter[Token Bucket Engine]
    Auth --> Idempotency[Idempotency Engine]
    Auth --> Quota[Token Quota Engine]
    RateLimiter --> Resilient[Resilience Stack]
    Resilient --> CB[Circuit Breaker]
    CB --> BH[Bulkhead]
    BH --> Retry[Retry Policy]
    Retry -->|"Atomic R/W with OCC"| DynamoDB[(DynamoDB)]
    Idempotency -->|"Conditional Writes"| DynamoDB
    Quota -->|"Conditional Writes"| DynamoDB
    RateLimiter -->|"Fire and Forget"| Kinesis[Kinesis Stream]
    Kinesis --> Analytics[Analytics Pipeline]
    subgraph observability [Observability]
        Prometheus[Prometheus /metrics]
        CloudWatch[CloudWatch]
        OTel[OpenTelemetry Traces]
        Dashboard[Live Dashboard SSE]
    end
    Server --> observability
```

**Key Components:**
- **HTTP4s API** — RESTful endpoints for rate limiting, idempotency, and token quotas
- **Token Bucket / Leaky Bucket Engine** — Rate limiting algorithms with optimistic concurrency control
- **Resilience Stack** — Circuit breaker → bulkhead → retry → graceful degradation (wired in `Main.scala`)
- **Token Quota Engine** — Multi-level (user/agent/org) LLM token quota enforcement
- **DynamoDB Storage** — Distributed state with atomic operations across 3 tables
- **Kinesis Streaming** — Real-time event pipeline for analytics
- **Prometheus + CloudWatch** — Dual-publish metrics; Prometheus scrape at `GET /metrics`
- **OpenTelemetry** — Distributed tracing via otel4s with automatic span propagation
- **ECS Fargate** — Serverless container orchestration (Terraform-provisioned)

### Optimistic Concurrency Control Flow

The rate limiting engine uses OCC to ensure atomic token consumption across distributed instances:

```mermaid
sequenceDiagram
    participant C as Client
    participant API as HTTP4s API
    participant TB as Token Bucket
    participant DB as DynamoDB

    C->>API: POST /v1/ratelimit/check
    API->>TB: checkAndConsume
    TB->>DB: GetItem (consistent read)
    DB-->>TB: Current state (v=3)
    TB->>TB: Refill tokens by elapsed time
    TB->>TB: Deduct cost from tokens
    TB->>DB: PutItem (condition: version=3)
    alt Concurrent modification
        DB-->>TB: ConditionalCheckFailed
        TB->>TB: Exponential backoff
        TB->>DB: Retry with fresh state
    end
    DB-->>TB: Success (v=4)
    TB-->>API: Allowed (tokens=87)
    API-->>C: 200 OK + X-RateLimit-* headers
```

See [Architecture Documentation](docs/ARCHITECTURE.md) for detailed design decisions.

## Quickstart in 5 Minutes

> Prerequisites: Docker 20.10+ and `docker-compose`. No AWS account needed for local development.

**Step 1 — Start LocalStack (DynamoDB + Kinesis emulation)**

```bash
make up
# or: docker compose up -d --build --wait localstack
```

Expected output: LocalStack container starts; the init script creates the DynamoDB tables and Kinesis stream automatically. The LocalStack image is built from `localstack.Dockerfile`, which installs scripts from `localstack-init/` (`init-aws.sh` creates the tables and stream; `gate-entrypoint.sh` runs at container start). If you see `FileNotFoundError: ... init-aws.sh`, run `make up-clean` to rebuild the LocalStack image and try again.

> **Returning to an older checkout?** Both images bake in code — the app image
> its jar, the LocalStack image `init-aws.sh` — and `docker compose up` reuses
> whatever it already built, while `localstack-data` survives `docker compose
> down` without `-v`. A stale stack fails in confusing ways: table names from a
> previous naming scheme make the correctness invariants fail with a flood of
> HTTP 500s. Reset all three with
> `docker compose build && docker compose down -v && docker compose up -d`.

**Step 2 — Verify the service is alive**

```bash
curl http://localhost:8080/health
```

```json
{ "status": "healthy", "version": "0.1.0" }
```

The reported version comes from `build.sbt`. If it disagrees, you are talking to
a stale image and need the reset above.

`/health` is liveness only — it answers 200 whenever the process is up, even if
DynamoDB and Kinesis are unreachable. Use `/ready` for dependency status; it is
what the ALB target group and `deploy-demo.sh` wait on.

**Step 3 — Check a rate limit (allowed)**

```bash
curl -s -X POST http://localhost:8080/v1/ratelimit/check \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer test-api-key" \
  -d '{"key": "user:demo", "cost": 1}' | jq .
```

```json
{
  "allowed": true,
  "tokensRemaining": 99,
  "limit": 100,
  "resetAt": "2025-01-15T10:30:05Z",
  "retryAfter": null
}
```

Response includes standard rate-limit headers: `X-RateLimit-Limit`, `X-RateLimit-Remaining`, `X-RateLimit-Reset`.

**Step 4 — Exhaust the bucket (rate-limited response)**

Run this ~105 times quickly to drain the `basic` tier bucket (100 token capacity):

```bash
# bash / zsh
for i in $(seq 1 105); do
  curl -s -X POST http://localhost:8080/v1/ratelimit/check \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer test-api-key" \
    -d '{"key": "user:demo", "cost": 1}' | jq -r '.allowed'
done
```

After ~100 successful requests, the bucket empties and the service returns 429:

```json
{
  "allowed": false,
  "tokensRemaining": null,
  "limit": 100,
  "resetAt": "2025-01-15T10:30:05Z",
  "retryAfter": 5,
  "message": "Rate limit exceeded"
}
```

**Step 5 — Idempotent POST (first call — New)**

```bash
curl -s -X POST http://localhost:8080/v1/idempotency/check \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer test-api-key" \
  -d '{"idempotencyKey": "payment:demo-001", "ttl": 3600}' | jq .
```

```json
{ "status": "new", "idempotencyKey": "payment:demo-001" }
```

**Step 6 — Replay the same key (duplicate detected)**

```bash
curl -s -X POST http://localhost:8080/v1/idempotency/check \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer test-api-key" \
  -d '{"idempotencyKey": "payment:demo-001", "ttl": 3600}' | jq .
```

```json
{ "status": "in_progress", "idempotencyKey": "payment:demo-001" }
```

The second call sees the existing `Pending` record and returns `in_progress` — the client knows not to rerun the payment. Once the operation is completed via `/v1/idempotency/:key/complete`, further replays return `duplicate` with the stored response body. If a replay sends a different request body, the service returns `409 Conflict` (request hash mismatch).

**Step 7 — Check LLM token quota (optional, requires `TOKEN_QUOTA_ENABLED=true`)**

```bash
curl -s -X POST http://localhost:8080/v1/quota/check \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer test-api-key" \
  -d '{"userId": "user:alice", "estimatedInputTokens": 500}' | jq .
```

```json
{
  "allowed": true,
  "remainingTokens": { "user": 999500, "org": 9999500 },
  "exceededLevel": null,
  "retryAfter": null
}
```

**Step 8 — Scrape Prometheus metrics**

```bash
curl -s http://localhost:8080/metrics | head -20
```

Returns metrics in Prometheus text exposition format: `gate_requests_total`, `gate_dynamodb_latency_seconds`, `gate_token_quota_total`, etc.

---

## API Endpoints

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `POST` | `/v1/ratelimit/check` | Yes | Check and consume rate limit tokens |
| `GET` | `/v1/ratelimit/status/:key` | Yes | Get current bucket state for a key |
| `POST` | `/v1/idempotency/check` | Yes | Check idempotency key (returns `new`, `in_progress`, `duplicate`, or `409 conflict`) |
| `POST` | `/v1/idempotency/:key/complete` | Yes | Complete an idempotent operation with a stored response |
| `POST` | `/v1/quota/check` | Yes | Pre-request token quota check (user/agent/org) |
| `POST` | `/v1/quota/reconcile` | Yes | Post-LLM-call: reconcile estimated vs actual tokens |
| `GET` | `/health` | No | Liveness probe (always 200 if process is alive) |
| `GET` | `/ready` | No | Readiness probe (checks DynamoDB + Kinesis dependencies) |
| `GET` | `/metrics` | No | Prometheus metrics scrape endpoint |
| `GET` | `/v1/ratelimit/dashboard/stats` | No | SSE stream for live rate-limit events |

See **[API Reference](docs/API.md)** for full request/response schemas.

## Operating Gate

The `obs` Docker Compose profile stands up Prometheus, Grafana (auto-provisioned with the Gate dashboard), and Jaeger alongside the running service.

```bash
docker compose --profile obs up -d
```

| Tool        | URL                              | Purpose                                   |
|-------------|----------------------------------|-------------------------------------------|
| Grafana     | <http://localhost:3000>          | `admin` / `admin`; "Gate — Rate Limiting & Quotas" dashboard |
| Prometheus  | <http://localhost:9090>          | Raw metric explorer, scrape targets       |
| Jaeger      | <http://localhost:16686>         | Distributed traces (service `gate`)      |

Dashboard source: [`observability/grafana/dashboards/gate.json`](observability/grafana/dashboards/gate.json). Panels cover request rate by result, rate-limit check p50/p95/p99, DynamoDB latency by operation, circuit breaker state, token consumption, and Kinesis publish vs drop.

![Gate Grafana dashboard](docs/images/grafana-dashboard.png)

> Screenshot placeholder — run `docker compose --profile obs up -d`, drive a few seconds of traffic with `sbt "loadSim/run --scenario normal"`, capture the dashboard, and save it at `docs/images/grafana-dashboard.png`. No build step reads this file; it is purely documentation.

For reproducible latency numbers at fixed RPS and a DynamoDB cost-per-decision breakdown, see [`docs/PERFORMANCE.md`](docs/PERFORMANCE.md).

A committed exemplar OTel trace lives at [`docs/exemplar-trace.json`](docs/exemplar-trace.json) (regenerate with [`scripts/export-exemplar-trace.sh`](scripts/export-exemplar-trace.sh) after starting the `obs` profile). It shows the HTTP → auth → token-bucket → DynamoDB GetItem/PutItem → Kinesis publish path for one `/v1/ratelimit/check` request.

### Correctness proof in CI

Every PR runs [`.github/workflows/ci.yml`](.github/workflows/ci.yml) → `correctness` job, which spins up the full stack and asserts two invariants via `sbt "loadSim/run --scenario correctness"`:

- **Token-bucket non-over-issue** — under 50-way parallel load on a single key, `allowed_count <= capacity + refillRate * duration + 2` (epsilon for clock skew). Direct proof of the "tokens never exceed capacity" guarantee.
- **Idempotency exactly-one-Created** — under 50-way parallel load on K=10 shared keys, exactly K `status=new` responses, zero 409 conflicts, zero HTTP errors. Exercises the conditional `PutItem` + TOCTOU retry path.

A violation fails the build. Scenario source: [`loadSim/src/main/scala/LoadSim.scala`](loadSim/src/main/scala/LoadSim.scala) (`Scenarios.correctness`).

## Infrastructure / Deploy

Terraform provisions the following AWS resources ([`terraform/`](terraform/)):

| Resource | What it does |
|---|---|
| **DynamoDB** `rate-limits` | Token-bucket state per key (OCC on version field) |
| **DynamoDB** `idempotency` | Idempotency key storage with TTL auto-expiry |
| **DynamoDB** `gate-token-quotas` | Multi-level token quota counters (user/agent/org) |
| **Kinesis** `rate-limit-events` | Rate-limit decision event stream |
| **ECS Fargate** | Containerised app; autoscaling 2–10 tasks |
| **ALB** | Application Load Balancer fronting ECS |
| **VPC / subnets** | Private subnets for ECS, public subnets for ALB |

All variables are in [`terraform/variables.tf`](terraform/variables.tf). `container_image` is the only **required** variable (no default); all others have sensible defaults.

**Key variables**

| Variable | Default | Description |
|---|---|---|
| `container_image` | *(required)* | Docker image URI (e.g. ECR) |
| `aws_region` | `us-east-1` | AWS region |
| `environment` | `dev` | Environment tag applied to all resources |
| `ecs_desired_count` | `2` | Number of running ECS tasks |
| `ecs_cpu` / `ecs_memory` | `256` / `512` | Container vCPU units and memory (MB) |
| `dynamodb_billing_mode` | `PAY_PER_REQUEST` | `PAY_PER_REQUEST` or `PROVISIONED` |
| `kinesis_shard_count` | `1` | Number of Kinesis shards |
| `enable_autoscaling` | `true` | ECS target-tracking autoscaling (min 2, max 10 tasks) |
| `enable_secrets_manager` | `false` | Load API keys from Secrets Manager (see [API keys](#api-keys-on-aws)) |
| `alarm_sns_topic_arn` | `""` | SNS topic for CloudWatch alarms (optional) |

### Demo environment (scripted)

Three scripts cover the whole round trip. The image has to exist in ECR before
Terraform runs, because `container_image` is required and has no default:

```bash
export ECR_IMAGE=$(./scripts/publish-image.sh)   # builds linux/amd64, creates the ECR repo, pushes
./scripts/deploy-demo.sh                          # terraform apply + wait for /ready
./scripts/teardown-demo.sh                        # destroy, then verify state is empty
```

The demo runs about **$0.25/hour** (~$6/day) in `us-east-1` — two NAT gateways,
five interface VPC endpoints across two AZs, an ALB, one Fargate task and one
Kinesis shard. `teardown-demo.sh` fails loudly and exits non-zero if anything is
left in Terraform state, and prints the CLI commands to double-check that no NAT
gateway or ALB survived.

Terraform state is local, so tear down from the same machine you deployed from.

`deploy-demo.sh` waits on `/ready`, not `/health`: `/health` is a liveness probe
that answers 200 as soon as the process accepts connections, even when every
backend is unreachable. `/ready` reports per-dependency status, so if a deploy
times out, `curl http://$ALB_DNS/ready` names the failing component.

### API keys on AWS

`enable_secrets_manager` is **off by default**, and the deployed service falls
back to the built-in development keys — fine for a demo, not for anything real.
Terraform does create the secret, but seeds it with a placeholder entry whose
`active` flag is `false`, so enabling Secrets Manager before writing real keys
leaves zero usable keys and every request answers 401. Write keys first:

```bash
aws secretsmanager put-secret-value \
  --secret-id "rate-limiter/demo/api-keys" \
  --secret-string '[{"apiKey":"...","apiKeyId":"key_001","clientName":"Demo","tier":"basic","permissions":["ratelimit_check","ratelimit_status","idempotency_check"],"active":true}]'
```

then redeploy with `-var="enable_secrets_manager=true"`. The secret name is
`<project_name>/<environment>/api-keys` because the app composes its lookup key
from `SECRETS_PREFIX`, `SECRETS_ENVIRONMENT` and `API_KEYS_SECRET_NAME`; the
three must agree with the name Terraform gives the secret. `project_name`
defaults to `rate-limiter` and `demo.tfvars` does not override it, so the demo
environment's resources are all named `rate-limiter-demo-*`.

**Deploy to dev**

```bash
cd terraform
terraform init
terraform plan \
  -var-file=environments/dev.tfvars \
  -var="container_image=ACCOUNT.dkr.ecr.REGION.amazonaws.com/rate-limiter:latest"
terraform apply \
  -var-file=environments/dev.tfvars \
  -var="container_image=ACCOUNT.dkr.ecr.REGION.amazonaws.com/rate-limiter:latest"
```

Example [`terraform/environments/dev.tfvars`](terraform/environments/dev.tfvars):

```hcl
environment             = "dev"
ecs_desired_count       = 1
ecs_cpu                 = 256
ecs_memory              = 512
kinesis_shard_count     = 1
kinesis_retention_hours = 24
# container_image = "123456789.dkr.ecr.us-east-1.amazonaws.com/rate-limiter:latest"
```

After apply, retrieve the ALB DNS name:

```bash
terraform output api_endpoint
```

## Documentation

- **[API Reference](docs/API.md)** — Complete API documentation with examples
- **[Architecture Decisions](docs/ARCHITECTURE.md)** — Design rationale and trade-offs
- **[Operational Runbook](docs/RUNBOOK.md)** — Troubleshooting circuit breaker, OCC exhaustion, idempotency issues, LocalStack
- **[Architecture Decision Records](docs/adr/)** — ADRs documenting key trade-offs:
  - [ADR-001: DynamoDB over Redis](docs/adr/001-dynamodb-over-redis.md)
  - [ADR-002: Hand-rolled circuit breaker](docs/adr/002-hand-rolled-circuit-breaker.md)
  - [ADR-003: Fire-and-forget event publishing](docs/adr/003-fire-and-forget-event-publishing.md)
  - [ADR-004: OCC vs pessimistic locking](docs/adr/004-occ-over-pessimistic-locking.md)

## Configuration

All settings are in [`src/main/resources/application.conf`](src/main/resources/application.conf) and can be overridden via environment variables.

### Per-tier rate limit profiles

Profiles set the burst cap (capacity) and steady-state throughput (refill rate) per client tier. Named profiles override tier defaults when the `profile` field is supplied in a rate-limit check request.

| Profile | Capacity (burst) | Refill rate | TTL |
|---|---|---|---|
| `free` | 20 tokens | 2 req/s | 1 h |
| `basic` | 100 tokens | 10 req/s | 1 h |
| `premium` | 1 000 tokens | 100 req/s | 1 h |
| `enterprise` | 10 000 tokens | 1 000 req/s | 1 h |

Profile lookup is O(1) — profiles are stored as `Map[String, RateLimitProfile]` built at startup. Invalid profiles (capacity < 1, refillRate ≤ 0, empty name) cause a startup failure.

### Token quota configuration

Multi-level LLM token quotas are enforced simultaneously at user, agent, and org levels. Agent limit is validated at startup to not exceed 80% of user limit.

| Level | Default limit | Window | Purpose |
|---|---|---|---|
| `user` | 1,000,000 tokens | 1 hour | Per-user hourly cap |
| `agent` | 500,000 tokens | 1 hour | Per-agent cap (≤ 80% of user) |
| `org` | 10,000,000 tokens | 24 hours | Org-wide daily cap |

### Key environment variable overrides

| Env var | Config key | Default |
|---|---|---|
| `RATE_LIMIT_ALGORITHM` | `rate-limit.algorithm` | `token-bucket` |
| `RATELIMIT_DEFAULT_CAPACITY` | `rate-limit.default-capacity` | `100` |
| `RATELIMIT_DEFAULT_REFILL_RATE` | `rate-limit.default-refill-rate-per-second` | `10.0` |
| `IDEMPOTENCY_DEFAULT_TTL` | `idempotency.default-ttl-seconds` | `86400` (24 h) |
| `IDEMPOTENCY_MAX_TTL_SECONDS` | `idempotency.max-ttl-seconds` | `86400` (24 h) |
| `TOKEN_QUOTA_ENABLED` | `token-quota.enabled` | `false` |
| `TOKEN_QUOTA_USER_LIMIT` | `token-quota.user-limit` | `1000000` |
| `TOKEN_QUOTA_AGENT_LIMIT` | `token-quota.agent-limit` | `500000` |
| `TOKEN_QUOTA_ORG_LIMIT` | `token-quota.org-limit` | `10000000` |
| `KINESIS_ENABLED` | `kinesis.enabled` | `true` |
| `KINESIS_QUEUE_SIZE` | `kinesis.queue-size` | `10000` |
| `METRICS_ENABLED` | `metrics.enabled` | `true` |
| `METRICS_MAX_BUFFER_SIZE` | `metrics.max-buffer-size` | `50000` |
| `METRICS_FLUSH_THRESHOLD` | `metrics.flush-threshold` | `1000` |
| `PROMETHEUS_ENABLED` | `prometheus.enabled` | `true` |
| `TRACING_ENABLED` | `tracing.enabled` | `true` |
| `OTEL_SERVICE_NAME` | `tracing.service-name` | `gate` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `tracing.exporter-endpoint` | `http://localhost:4317` |
| `AUTH_ENABLED` | `security.authentication.enabled` | `true` |
| `CIRCUIT_BREAKER_ENABLED` | `resilience.circuit-breaker.enabled` | `true` |
| `BULKHEAD_MAX_CONCURRENT` | `resilience.bulkhead.max-concurrent` | `100` |
| `DEGRADATION_MODE` | `resilience.degradation-mode` | `reject-all` |
| `STORAGE_BACKEND` | `storage.backend` | `dynamodb` |

See [`application.conf`](src/main/resources/application.conf) for the complete reference.

## Health and Readiness

The service exposes two probe endpoints on the same HTTP port (`8080`):

### `GET /health` — liveness

Returns `200 OK` whenever the process is alive. No dependency checks are performed. Use for container liveness probes; failure means the container should be restarted.

```json
{ "status": "healthy", "version": "0.1.0" }
```

### `GET /ready` — readiness

Pings all upstream dependencies and returns `200 OK` only when every component reports `ok`. Returns `503 Service Unavailable` with `"status": "degraded"` when any of them fails, and the failing component carries the error in `details`. Use for load-balancer health checks and Kubernetes readiness probes — failure removes the instance from rotation without restarting it.

**Components checked:**

| `name` | Dependency |
|---|---|
| `dynamodb_ratelimit` | DynamoDB rate-limits table |
| `dynamodb_idempotency` | DynamoDB idempotency table |
| `kinesis` | Kinesis stream |

**200 response:**
```json
{
  "status": "ok",
  "components": [
    { "name": "dynamodb_ratelimit", "status": "ok", "details": null },
    { "name": "dynamodb_idempotency", "status": "ok", "details": null },
    { "name": "kinesis", "status": "ok", "details": null }
  ]
}
```

**503 response (one or more dependencies failing):**
```json
{
  "status": "degraded",
  "components": [
    { "name": "dynamodb_ratelimit", "status": "error", "details": "Received an UnknownHostException when attempting to interact with a service..." },
    { "name": "dynamodb_idempotency", "status": "ok", "details": null },
    { "name": "kinesis", "status": "ok", "details": null }
  ]
}
```

Each failure is logged at `WARN` level so log-based alerts can be wired upstream.

## Metrics

Metrics are dual-published to **CloudWatch** (AWS) and **Prometheus** (`GET /metrics`).

### Prometheus endpoint

`GET /metrics` returns all metrics in Prometheus text exposition format, scrapeable by Prometheus, Grafana Agent, or any OpenMetrics-compatible collector.

### Key metrics

| Metric name | Type | Labels | Description |
|---|---|---|---|
| `gate_requests_total` | Counter | `key`, `result` | Total rate limit requests (allowed/rejected) |
| `gate_idempotency_total` | Counter | `result` | Total idempotency checks |
| `gate_token_quota_total` | Counter | `level`, `result` | Token quota checks (available/exceeded) |
| `gate_tokens_consumed` | Gauge | `user`, `agent`, `org` | Current token consumption per entity |
| `gate_dynamodb_latency_seconds` | Histogram | `operation` | DynamoDB operation latency |
| `gate_rate_limit_check_seconds` | Histogram | — | End-to-end rate-limit check latency |
| `gate_idempotency_check_seconds` | Histogram | — | End-to-end idempotency check latency |
| `gate_token_quota_check_seconds` | Histogram | — | Token quota check latency |
| `gate_events_published_total` | Counter | `event_type` | Kinesis events published |
| `gate_events_dropped_total` | Counter | — | Events dropped after retry exhaustion |
| `gate_circuit_breaker_state` | Gauge | `name` | Circuit breaker state (0=closed, 0.5=half-open, 1=open) |

### CloudWatch metrics

| Metric name | Unit | Description |
|---|---|---|
| `RateLimitAllowed` | Count | Requests allowed through |
| `RateLimitBlocked` | Count | Requests rejected (rate limit exceeded) |
| `RateLimitOCCRetry` | Count | OCC conditional-write retries; high values indicate key contention |
| `RateLimitLatency` | Milliseconds | End-to-end latency of a rate-limit check |
| `DroppedKinesisEvent` | Count | Events dropped because the drain queue was full |
| `CorruptStateRead` | Count | DynamoDB items with unparseable state |

### Flush behaviour

Metrics are buffered in an `immutable.Queue` and flushed to CloudWatch:
- **Periodic:** every `metrics.flush-interval = 60s`
- **Threshold:** when buffer reaches `metrics.flush-threshold = 1000` entries (async flush, CAS-guarded against stampede)
- **Shutdown:** `Resource.onFinalize` flushes remaining entries before the process exits

Buffer is capped at `metrics.max-buffer-size = 50000` entries; oldest entries are dropped when full.

### CloudWatch namespace

Default namespace: `RateLimiter`. Override with `METRICS_NAMESPACE`.

## Distributed Tracing

OpenTelemetry tracing is integrated via [otel4s](https://typelevel.org/otel4s/). Spans are automatically created around rate-limit checks, idempotency checks, and token quota operations via `TracingMiddleware`.

Trace IDs are propagated through Kinesis events for end-to-end audit correlation.

**Configuration:**

| Env var | Default | Description |
|---|---|---|
| `TRACING_ENABLED` | `true` | Enable/disable OTel tracing |
| `OTEL_SERVICE_NAME` | `gate` | Service name in traces |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4317` | OTLP gRPC endpoint (Jaeger, Tempo, etc.) |

To view traces locally, run Jaeger:

```bash
docker run -d --name jaeger \
  -p 16686:16686 -p 4317:4317 \
  jaegertracing/all-in-one:latest
```

Then open `http://localhost:16686` and search for service `gate`.

## Features

### Rate Limiting

- **Token Bucket Algorithm** — Default; allows burst up to capacity.
- **Leaky Bucket Algorithm** — Optional; smooths output (steady drain), no large burst. Select via `rate-limit.algorithm`.
- **Configurable Limits** — Per-user, per-API-key, per-endpoint limits via named profiles.
- **Standard Response Headers** — `X-RateLimit-Limit`, `X-RateLimit-Remaining`, `X-RateLimit-Reset` on every allowed response.
- **Resilience Stack** — Circuit breaker → bulkhead → retry → graceful degradation wrapping DynamoDB access.

### Rate limit algorithms

| Aspect     | Token bucket (default)   | Leaky bucket        |
|-----------|--------------------------|---------------------|
| Bursts    | Allows burst to capacity | Smooths; no burst   |
| Fairness  | Burst then refill        | Steady drain        |
| Complexity| Low (2 state vars)       | Low (2 state vars)  |
| DynamoDB  | One item, OCC            | Same                |
| Use case  | APIs that allow burst    | Strict smooth rate  |

Set `rate-limit.algorithm = "token-bucket"` or `"leaky-bucket"` in configuration (or `RATE_LIMIT_ALGORITHM` env).

### Idempotency

- **First-Writer-Wins** — Atomic operations using DynamoDB conditional writes
- **Request Fingerprinting** — SHA-256 hash of request body; same key + different body → `409 Conflict`
- **TOCTOU Race Protection** — Retry loop (max 3 attempts) handles the edge case where a TTL-deleted record causes a false `New`
- **Response Caching** — Store and replay responses for duplicate requests
- **TTL-Based Cleanup** — Automatic expiration of old keys
- **Safe Retries** — Clients can safely retry failed requests

**Idempotency – schema and semantics**

- **Key:** One item per key; partition key is `idempotency#<key>`. Consistent reads ensure up-to-date status.
- **TTL:** Set at creation (`now + ttlSeconds`). DynamoDB TTL deletes expired items so the table does not grow without bound.
- **First writer wins:** First successful create (no existing key, or status `Failed`) gets **New**; others get **InProgress** or **Duplicate** with stored response. Complete is conditional on `status = Pending`.
- **Request fingerprinting:** A SHA-256 hash of the request body is stored with the key. If a duplicate check arrives with a different body hash, the service returns **KeyConflict** (409).
- **Config:** Per-request TTL via `POST /v1/idempotency/check` body field `ttl`, or server default (e.g. 24h). Server enforces a maximum TTL (`idempotency.max-ttl-seconds` or `IDEMPOTENCY_MAX_TTL_SECONDS`); client-supplied TTL above that is capped.

### Token Quotas (AI Workloads)

Multi-level LLM token quota enforcement for AI infrastructure:

- **Three levels** — User, agent, and org quotas enforced simultaneously per request.
- **Pre-request estimation** — `POST /v1/quota/check` validates estimated token usage before calling the LLM.
- **Post-response reconciliation** — `POST /v1/quota/reconcile` adjusts counters based on actual usage.
- **Agent cap** — Agent limit is validated at startup to be ≤ 80% of user limit.
- **Retry-After** — `429` responses include `Retry-After` header with seconds until the quota window resets.
- **Metrics** — `gate_tokens_consumed{user, agent, org}` gauge exported to Prometheus and CloudWatch.

### Resilience

The rate-limit store is wrapped with a layered resilience stack (configured via `application.conf`):

| Layer | Default | Purpose |
|---|---|---|
| **Circuit Breaker** | 5 failures → open, 30s reset, 3 half-open calls | Stop cascading failures from DynamoDB outages |
| **Bulkhead** | 100 concurrent, 500ms max wait | Limit concurrent DynamoDB calls |
| **Retry** | 3 retries, 100ms base delay, 2× backoff, 10s max | Recover from transient failures |
| **Graceful Degradation** | `reject-all` | Behaviour when circuit is open: `reject-all` (safe for fintech), `allow-all` (AI infra), or `use-cached` |

### Observability

- **Structured Logging** — JSON logs with correlation IDs
- **Prometheus Metrics** — `GET /metrics` endpoint with counters, gauges, and histograms
- **CloudWatch Metrics** — Dual-publish to CloudWatch for AWS-native dashboards
- **Distributed Tracing** — OpenTelemetry via otel4s; spans around all critical operations
- **Health Endpoints** — `/health` (liveness) and `/ready` (readiness) probes
- **Live Dashboard** — `GET /v1/ratelimit/dashboard/stats` streams rate-limit events via SSE

### Analytics

- **Event Streaming** — Kinesis pipeline for real-time events (rate-limit decisions, idempotency hits, quota exceeded, circuit breaker state changes)

## Technology Stack

**Core:**
- Scala 3.7.4
- Cats Effect 3.6.3 (functional effects)
- HTTP4s 0.23.32 (HTTP server/client)
- Circe 0.14.15 (JSON serialization)
- PureConfig 0.17.9 (configuration management)
- Log4Cats 2.7.1 (structured logging)

**Observability:**
- Prometheus Client 0.16.0 (metrics exposition)
- otel4s 0.15.2 + OpenTelemetry SDK 1.60.1 (distributed tracing)

**AWS Services:**
- ECS Fargate (container orchestration)
- DynamoDB (NoSQL state storage — 3 tables)
- Kinesis (event streaming)
- CloudWatch (metrics & logs)
- Application Load Balancer (traffic distribution)
- Secrets Manager (API key storage, optional)

**Infrastructure:**
- Terraform (DynamoDB, Kinesis, ECS Fargate, ALB; see [Infrastructure](#infrastructure--deploy))
- Docker (containerization)
- LocalStack (local AWS emulation)

### Local Development

```bash
# Clone repository
git clone https://github.com/saintparish4/keyra.git
cd keyra

# Start local environment (LocalStack)
docker-compose up -d

# Run application
sbt run

# Test endpoints
curl http://localhost:8080/health
curl http://localhost:8080/metrics
curl -X POST http://localhost:8080/v1/ratelimit/check \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer test-api-key" \
  -d '{"key": "user:123", "cost": 1}'
```

### Load Testing

**k6 scenarios** (requires [k6](https://k6.io/docs/get-started/installation/)):

```bash
# Quick smoke test (30s, 10 VUs, many unique keys)
./scripts/load-test.sh dev quick

# Baseline: ramp 0→50→100 VUs over 16 minutes, many unique keys
./scripts/load-test.sh dev baseline

# High-contention: 50 VUs all hammering the same key for 60s
# Purpose: stress OCC retry path, measure latency and rejection rate under hot-key load
./scripts/load-test.sh dev highContention
```

**Scala load simulator** (no k6 required):

```bash
# Start the server first:
make dev   # or: docker compose up -d && sbt run

# Then run a scenario:
sbt "loadSim/run --scenario normal"          # 20 VUs, 500 unique keys, 60s
sbt "loadSim/run --scenario burst"           # 50 VUs, 50 keys (more contention), 60s
sbt "loadSim/run --scenario idempotency"     # 30 VUs, 5 shared keys — verifies exactly-one-Created
sbt "loadSim/run --scenario realistic"       # 40 VUs, 80% rate-limit / 20% idempotency
sbt "loadSim/run --scenario highContention"  # 50 VUs, 1 fixed key — OCC stress test
```

| Scenario | VUs | Keys | Purpose |
|----------|-----|------|---------|
| `normal` | 20 | 500 | Base throughput, many unique keys |
| `burst` | 50 | 50 | Token refill behaviour under burst |
| `idempotency` | 30 | 5 | Exactly-one-Created under concurrency |
| `realistic` | 40 | 200 | Mixed traffic (80/20 split) |
| `highContention` | 50 | **1** | OCC retry stress, latency and rejection rate |

See [Benchmark Results](#benchmark-results) for observed numbers from each scenario.

**Note:** these numbers come from the local Docker stack. The Terraform stack has
never been applied, so there are no figures from a real AWS deployment. Scripted
deployment instructions are in [Demo environment](#demo-environment-scripted).

<a name="benchmark-results"></a>

## Benchmark Results

> For **fixed-RPS latency numbers** (p50/p95/p99 at a stated input RPS) and a DynamoDB cost-per-decision breakdown, see [`docs/PERFORMANCE.md`](docs/PERFORMANCE.md). The numbers below are from ad-hoc runs at whatever RPS the loop produced and are kept for the OCC-contention story (Scenario 2), not as authoritative throughput figures.

> Numbers are from local testing with LocalStack + Docker on a 6-core dev machine. AWS production performance will differ based on region and DynamoDB configuration.

### Scenario 1 — Many keys (normal load)

Scala loadSim: 20 VUs, 500 unique keys, 60 seconds. k6 quick: 10 VUs, 1,000 keys, 30 seconds.

| Metric | loadSim `normal` | k6 `quick` |
|--------|-----------------|------------|
| Total requests | 3,014 | 300 |
| Throughput | ~50 RPS | ~10 RPS |
| Allowed | 100% | 100% |
| Blocked | 0% | 0% |
| Errors | 0 | 0% |
| P50 latency | — | 49 ms |
| P95 latency | — | 98 ms |
| P99 latency | — | 305 ms |

**Takeaway:** With many unique keys, virtually no OCC contention occurs — every request succeeds on the first conditional write attempt. All tokens remain available because VUs spread across hundreds of keys never exhaust any single bucket. Latency is dominated by LocalStack networking overhead; expect significantly lower latency against real DynamoDB in the same region.

---

### Scenario 2 — Single hot key (highContention)

50 VUs all targeting `contention:single-hot-key` for 60 seconds. Every request races on the same DynamoDB item — worst case for OCC.

| Metric | loadSim `highContention` | k6 `highContention` |
|--------|-------------------------|---------------------|
| Total requests | 284 | 700 |
| Throughput | ~4 RPS | ~8 RPS |
| Allowed | 104 (36%) | 409 (58%) |
| Blocked (429) | 180 (63%) | 291 (42%) |
| P50 latency | — | 3.33 s |
| P90 latency | — | 9.97 s |
| P95 latency | — | 11.25 s |
| P99 latency | — | 13.25 s |
| Avg latency | 9,693 ms | 4,400 ms |
| Max latency | 17,836 ms | 15,770 ms |

**Takeaway:** Throughput drops from ~50 RPS to 4–8 RPS — a **6–12× reduction** — solely from OCC contention on a single key. With 50 VUs racing on one DynamoDB item, most conditional writes fail and must retry (re-read + re-write), up to 10 times per attempt. P99 latency climbs to 13 seconds. The allowed/blocked split varies between runs (36–58% allowed) depending on LocalStack state and timing; the bucket refills between contended rounds so it is not permanently empty.

The k6 thresholds intentionally fail under this scenario — they are tuned for normal multi-key traffic:

```
✗ p(99)<200ms   → actual 13.25s     (OCC retries dominate)
✗ p(95)<500ms   → actual 11.25s
✗ error rate<1% → actual 41.57%     (429s counted as failures by k6)
```

To observe OCC retries in production: watch the `RateLimitOCCRetry` CloudWatch metric or `gate_requests_total{result="rejected"}` in Prometheus. A spike that coincides with latency spikes confirms the retry overhead as the source. This is the honest cost of distributed correctness without a lock service — the system stays **safe** (never over-issues) at the expense of throughput and tail latency under hot-key load.

---

### Scenario 3 — All loadSim scenarios summary

| Scenario | VUs | Keys | Total | RPS | Allowed | Blocked | Notes |
|----------|-----|------|-------|-----|---------|---------|-------|
| `normal` | 20 | 500 | 3,014 | ~50 | 100% | 0% | Baseline — no contention |
| `burst` | 50 | 50 | 1,935 | ~32 | 100% | 0% | More keys-per-VU contention lowers RPS |
| `idempotency` | 30 | 5 | 524 | ~17 | 5 created | 519 dup | Exactly-one-Created proven |
| `realistic` | 40 | 200 | 2,144 | ~35 | 78% RL | 21% idem | Mixed traffic matches 80/20 target |
| `highContention` | 50 | **1** | 284 | ~4 | 36% | 63% | OCC stress — avg 9.7s latency |

---

### In-memory vs Distributed comparison

The `InMemoryRateLimitStore` uses a `Ref[F, Map[String, BucketState]]` — no network I/O, just in-process atomic state.

| Metric | In-memory store | DynamoDB store (LocalStack) | Ratio |
|--------|----------------|----------------------------|-------|
| P50 latency | < 1 ms | ~49 ms (k6 measured) | ~50× |
| P99 latency | < 5 ms (est.) | ~305 ms (k6 normal) | ~60× |
| Throughput (20 VUs) | ~2,000+ RPS (est.) | ~50 RPS (measured) | ~40× |
| OCC retry rate | 0% (no OCC needed) | ~0% (normal), ~40% (hot key) | — |
| Single-instance correctness | Yes | Yes | — |
| Multi-instance correctness | **No** (state is per-process) | **Yes** | — |

**Interpretation:** The DynamoDB store is ~50× slower at P50 on a single machine — all of that overhead is the distributed coordination cost (2 DynamoDB round-trips per check via LocalStack). For rate limiting, this is acceptable: a 49 ms P50 for a rate-limit check at the edge of your API is small relative to the business logic it protects. Against real DynamoDB in the same region (not LocalStack), expect 5–10 ms P50 instead.

The in-memory store is useful for local development and unit tests where multi-instance correctness is not needed. Select it with `STORAGE_BACKEND=in-memory`. The critical difference: if you run two instances with the in-memory store, each instance tracks its own token bucket independently — a client can double-consume by round-robin across instances. The DynamoDB store coordinates globally via a single DynamoDB item per key as the source of truth.

## Design Highlights

### Functional Programming

```scala
// Pure, composable rate limiting
def checkRateLimit[F[_]: Async](
  key: String,
  cost: Int
): F[RateLimitDecision] =
  for
    state <- store.get(key)
    refilled = refillTokens(state, Clock[F].realTime)
    decision <- 
      if refilled.tokens >= cost then
        store.put(refilled.consume(cost))
          .as(Allowed(refilled.tokens - cost))
      else
        Async[F].pure(Rejected(calculateRetryAfter(refilled)))
  yield decision
```

### Resource Safety

```scala
// Automatic cleanup with Resource
val resources: Resource[IO, (DynamoClient, KinesisClient)] =
  for
    dynamo <- DynamoClient.resource[IO](config)
    kinesis <- KinesisClient.resource[IO](config)
  yield (dynamo, kinesis)

resources.use { case (dynamo, kinesis) =>
  // Application runs here
  // Resources automatically closed on shutdown/error
}
```

### Observability

```scala
// Structured logging with context
logger.info(
  "Rate limit check completed",
  Map(
    "requestId" -> requestId,
    "apiKey" -> apiKey,
    "allowed" -> allowed,
    "tokensRemaining" -> tokensRemaining,
    "latencyMs" -> latencyMs
  )
)
```

## Status

Gate is at **0.1.0**. Rate limiting, LLM token quotas, and idempotency are
implemented, tested at unit, property, integration, and end-to-end level, and
gated in CI by three invariants that run against a live stack on every change:
token-bucket non-over-issue, idempotency exactly-one-Created, and token-quota
non-over-admission.

Not yet proven: the Terraform stack has never been applied, so there are no
numbers from a real AWS deployment. Treat the performance figures here as
LocalStack measurements.

## Contributing

Feedback is welcome!

1. Open an issue to discuss proposed changes
2. Fork the repository
3. Create a feature branch
4. Submit a pull request
