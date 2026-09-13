## Architecture Overview

**Note:** This document describes the intended architecture. AWS deployment has not been tested. Currently only local development with docker-compose (LocalStack) is validated.

**Local development (Docker / LocalStack):** The `localstack` service in `docker-compose.yml` is built from `localstack.Dockerfile`. That image extends `localstack/localstack` and adds scripts from `localstack-init/`: `init-aws.sh` creates the DynamoDB tables (`rate-limits`, `idempotency`, `gate-token-quotas`) and the Kinesis stream (`rate-limit-events`); `gate-entrypoint.sh` copies the init script into LocalStack’s `ready.d` at container start so resources are created automatically. See README “Quickstart” and `make up` / `make up-clean`.

**Cross-references:**
- Token-bucket refill formula and invariants: [`core/TokenBucket.scala`](../src/main/scala/core/TokenBucket.scala) — both the DynamoDB store and the in-memory store delegate to it. The canonical formula is reproduced in [Rate limit check path](#rate-limit-check-path) below.
- OCC conditional-write policy, retry count, and high-contention behaviour: [`storage/DynamoDBRateLimitStore.scala`](../src/main/scala/storage/DynamoDBRateLimitStore.scala). Two-round-trip cost analysis is in [Performance characteristics](#performance-characteristics).
- Leaky-bucket implementation: [`storage/LeakyBucketRateLimitStore.scala`](../src/main/scala/storage/LeakyBucketRateLimitStore.scala).

### High-Level Design

```
┌─────────────────────────────────────────────────────────────┐
│                        API Clients                           │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    Application Load Balancer                 │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│              Rate Limiter Service (ECS/Lambda)               │
│                                                               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │  HTTP API    │  │  Rate Limit  │  │ Idempotency  │      │
│  │  (HTTP4s)    │  │  Engine      │  │   Store      │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
└─────────────────────────────────────────────────────────────┘
         │                    │                    │
         │                    │                    │
         ▼                    ▼                    ▼
┌─────────────────┐  ┌──────────────────┐  ┌─────────────────┐
│   CloudWatch    │  │    DynamoDB      │  │    Kinesis      │
│   (Metrics)     │  │  (State Store)   │  │  (Events)       │
└─────────────────┘  └──────────────────┘  └─────────────────┘
                                                    │
                                                    ▼
                                          ┌──────────────────┐
                                          │  Data Analytics  │
                                          │  (S3 + Athena)   │
                                          │  [NOT YET WIRED] │
                                          └──────────────────┘
```

> **Note:** The S3 + Athena analytics layer is defined in Terraform (`terraform/modules/kinesis/main.tf`, gated behind `enable_kinesis_firehose` and `enable_audit_compliance`) but has not been deployed or validated. Currently, Kinesis events are consumed only by the live SSE dashboard and structured log output.

---

### Design doc

#### Components

| Component | Role |
|-----------|------|
| **HTTP API (HTTP4s)** | REST endpoints: rate limit check, idempotency check/complete, health, ready, dashboard. Auth via API key; tier selects rate-limit profile. |
| **Rate Limit Engine** | Token-bucket logic: get state from DynamoDB, refill by elapsed time, consume tokens, write back with OCC (version-based conditional write). |
| **Idempotency Store** | First-writer-wins: conditional create (`attribute_not_exists(pk)` or `status = Failed`), then complete via conditional update (`status = Pending`). |
| **DynamoDB** | Two tables: rate-limit state (per key) and idempotency records. All reads use **consistent read** for correct, up-to-date state. |
| **Kinesis** | Event stream for rate-limit and idempotency events. Publishing is **fire-and-forget**; failures are logged and do not affect the API response. |
| **CloudWatch** | Metrics (allowed/blocked, latency, circuit breaker state) and optional dashboards when deployed to AWS. |

#### Data models (DynamoDB)

**Rate limit table**

| Attribute | Type | Description |
|-----------|------|-------------|
| `pk` | S (partition key) | `"ratelimit#<key>"` — one item per rate-limit key. |
| `tokens` | N | Current token count (double stored as string). |
| `lastRefillMs` | N | Last refill timestamp (milliseconds, from Clock.realTime; used for elapsed-time refill). |
| `version` | N | Incremented on every successful write; used for OCC. |
| `ttl` | N | Expiration time (epoch seconds) for DynamoDB TTL cleanup. |

**Idempotency table**

| Attribute | Type | Description |
|-----------|------|-------------|
| `pk` | S (partition key) | `"idempotency#<key>"` — one item per idempotency key. |
| `clientId` | S | Client (e.g. API key) that created the record. |
| `status` | S | `Pending` \| `Completed` \| `Failed`. |
| `response` | S | JSON-encoded stored response (when `Completed`). |
| `createdAt` | N | Creation time (epoch ms). |
| `updatedAt` | N | Last update time (epoch ms). |
| `version` | N | Version for conditional updates. |
| `ttl` | N | Expiration (epoch seconds) for DynamoDB TTL cleanup. |

#### Idempotency – schema and semantics

**Key schema**

- Partition key: `pk = "idempotency#<key>"` where `<key>` is the client-supplied idempotency key (e.g. `payment:abc-123`). One DynamoDB item per idempotency key. All reads use **consistent read** so callers see up-to-date status.

**TTL policy**

- Each item has a `ttl` attribute (N, epoch seconds). It is set at **create time** to `now_epoch_seconds + ttlSeconds`, where `ttlSeconds` is supplied by the client on `POST /v1/idempotency/check` (or a server default, e.g. 24 hours). DynamoDB TTL deletes items after the `ttl` time has passed; deletion is asynchronous (typically within a few days). Until deletion, the key is treated as the same logical request for replay; after expiry, a new request with the same key is allowed (first writer wins again).

**First writer wins**

- **Check:** The first successful **PutItem** with condition `attribute_not_exists(pk) OR status = :failed` wins. That writer creates a record in status `Pending` and receives **New** — the client should perform the operation. Any later caller for the same key gets **GetItem** and receives **InProgress** (status `Pending`) or **Duplicate** (status `Completed`, with stored response). If the record was in status `Failed`, a new writer can win by creating a new `Pending` record (condition allows `status = Failed`).
- **Complete:** Only the creator can complete: **UpdateItem** with condition `status = :pending` sets `status = Completed` and stores the response. Duplicate complete calls fail the condition and do not overwrite.

**Same request retried vs new unique request**

- **Same request (retry/replay):** Same idempotency key and the item still exists (within TTL). The client is retrying; the service returns **InProgress** (work not yet completed) or **Duplicate** (work already completed, with stored response). No second operation is executed.
- **New unique request:** Either (1) a different idempotency key, or (2) the same key but the previous item has expired (TTL passed and DynamoDB has removed it). In case (2), a new **PutItem** succeeds (`attribute_not_exists(pk)`), the client receives **New**, and may perform a new operation. Clients must use a unique key per logical operation and the same key for retries of that operation.

**Unbounded growth prevention**

- **TTL on every item:** Every idempotency record has a `ttl` set at creation. DynamoDB TTL automatically deletes items after that time, so the table does not grow without bound for old keys.
- **Optional max TTL:** Configuration can cap the allowed `ttlSeconds` (e.g. max 24h or 7 days) so clients cannot set arbitrarily long retention. Server enforces a maximum TTL (e.g. config `idempotency.max-ttl-seconds`); client-supplied TTL above that is capped so retention is bounded.

#### Main flows

**Rate limit check path**

1. Client sends `POST /v1/ratelimit/check` with `key`, `cost`, optional `profile`.
2. API resolves rate-limit profile from client tier (or request).
3. **GetItem** (consistent read) on rate-limit table for `pk = "ratelimit#<key>"`; if missing, treat as full bucket.
4. Compute refill: `elapsed_sec = (now_ms - lastRefillMs) / 1000`, `tokens_to_add = elapsed_sec * refillRate`, `refilled = min(capacity, current_tokens + tokens_to_add)`.
5. If `refilled >= cost`: **PutItem** with new tokens and `version + 1`, condition `version = :expectedVersion` (or `attribute_not_exists(pk)` for first write).
6. On `ConditionalCheckFailedException`: retry with 1ms delay, up to 10 times; then **reject** (safe: no over-issue).
7. Publish rate-limit event to Kinesis (fire-and-forget); return 200 (allowed) or 429 (rejected).

**Idempotent POST path**

1. Client sends `POST /v1/idempotency/check` with `idempotencyKey`, optional `ttl`.
2. **PutItem** with condition `attribute_not_exists(pk) OR status = :failed`. If condition succeeds → **New** (client should perform work).
3. If condition fails: **GetItem** (consistent read) for existing record. Return **InProgress** (status `Pending`), **Duplicate** (status `Completed`, with stored response), or **New** (status `Failed`, allow retry).
4. After client completes work: `POST /v1/idempotency/:key/complete` with response body. **UpdateItem** with condition `status = :pending`; set `status = Completed`, store response. Duplicate complete calls fail the condition and do not overwrite.

**Event streaming path**

1. After a rate-limit decision or idempotency check/complete, the API builds an event (e.g. `RateLimitChecked`, `IdempotencyChecked`).
2. Event is published to Kinesis (`PutRecord` or `PutRecords`) in a **fire-and-forget** manner (e.g. spawned fiber); the HTTP response is not delayed by Kinesis.
3. Failures (throttling, network) are logged; no retry to the client and no back-pressure on the request path. The live SSE dashboard consumes events in-process. S3/Athena consumption via Kinesis Firehose is defined in Terraform but not yet deployed (see [ADR-003](adr/003-fire-and-forget-event-publishing.md)).

#### Rate limit algorithms

The service supports two algorithms behind the same RateLimitStore abstraction:

- **Token bucket (default):** Refill by elapsed time; allows burst up to capacity. State: tokens, lastRefillMs, version. See "Rate limit table" and "Rate limit check path" above. The canonical refill formula and all invariants are defined in [`core/TokenBucket.scala`](../src/main/scala/core/TokenBucket.scala) — both the DynamoDB store and the in-memory store delegate to it.
- **Leaky bucket:** Drain (leak) by elapsed time; smooths output, no large burst. State: level, lastLeakMs, version. Same DynamoDB schema (tokens→level, lastRefillMs→lastLeakMs); same OCC and retry policy. Implemented in [`storage/LeakyBucketRateLimitStore.scala`](../src/main/scala/storage/LeakyBucketRateLimitStore.scala); enabled via `rate-limit.algorithm = "leaky-bucket"` in config.

Trade-offs:

| Aspect     | Token bucket           | Leaky bucket              |
|-----------|------------------------|----------------------------|
| Bursts    | Allows burst to capacity | Smooths; no large burst  |
| Fairness  | Burst then refill      | Steady drain over time    |
| Complexity| Low (2 state vars)     | Low (2 state vars)        |
| DynamoDB  | One item, OCC          | Same one item, OCC        |
| Use case  | APIs that allow burst  | Strict smooth rate        |

Selection is by configuration: rate-limit.algorithm = "token-bucket" | "leaky-bucket". When using DynamoDB, both can share the same table (one algorithm per deployment).

---

### Design alternatives

**Redis vs DynamoDB**

- **Redis:** Lower latency and natural fit for counters/windows, but adds a separate stateful service, persistence and failover configuration, and (typically) single-region concerns. Good when latency is critical and the team already operates Redis.
- **DynamoDB (chosen):** Single AWS-consistent stack, built-in durability and auto-scaling, TTL for automatic cleanup of rate-limit and idempotency rows, and **strong consistency** via consistent read. Slightly higher latency than Redis (~5–10 ms vs ~1 ms) is acceptable for rate limiting. Avoids operating a separate cache/store.

**Locks vs OCC**

- **Pessimistic locking:** Would require a distributed lock store (e.g. Redis, DynamoDB-based lock table) and lock lifecycle (acquire, extend, release). Adds complexity, deadlock/lease handling, and another failure mode.
- **OCC (chosen):** Uses DynamoDB conditional writes only: no separate lock service. Under low-to-moderate contention (typical for per-key rate limits), retries (1 ms delay, max 10) usually succeed. Under high contention we **reject** after max retries to preserve correctness (no over-issuing). Simpler and good enough for the target workload.

<a name="performance-characteristics"></a>

**Performance characteristics**

Each rate-limit check requires exactly **two DynamoDB round-trips** in the uncontended case:

1. `GetItem` (consistent read) — fetch current token-bucket state for `pk = "ratelimit#<key>"`.
2. `PutItem` (conditional write) — write updated state with `version = :expectedVersion` (or `attribute_not_exists(pk)` for first write).

Under contention (concurrent writes to the same key), a `ConditionalCheckFailedException` triggers a retry: the caller re-reads (1 more `GetItem`) and re-writes (1 more `PutItem`). With up to 10 retries, the **worst-case cost per check is 20 DynamoDB round-trips**. This is the honest price of distributed correctness without a lock service.

At typical per-user or per-API-key traffic (low single-key concurrency), the common case is 2 round-trips. The 20-round-trip scenario occurs only when many instances race on the exact same key in the same millisecond window.

*Known improvement path:* A single-round-trip implementation is possible by replacing `GetItem + conditional PutItem` with a single `UpdateItem` expression (`SET tokens = tokens - :cost IF tokens >= :cost`). This halves DynamoDB consumption under normal load and improves latency, but requires restructuring the refill logic into a DynamoDB expression. This is tracked as a future optimisation.

---

### Failure scenarios and trade-offs

**Eventual vs strong consistency**

- **Choice:** All DynamoDB reads in this service use **consistent read** (`GetItem` with `consistentRead(true)`). That gives read-your-writes and accurate token counts / idempotency state without replica lag.
- **Trade-off:** Consistent reads consume more capacity and have slightly higher latency than eventually consistent reads; we accept that for correctness.

**Over-issuing tokens under races**

- **Behavior:** Two instances can read the same state, both compute refill and consume; only one PutItem (with version condition) succeeds. The other retries with a fresh read. After **max 10 retries** we **reject** the request.
- **Result:** We may **under-issue** (reject when a retry could have succeeded) but we do **not** over-issue; the rate limit is enforced safely.

**Partition hot-spotting (single key = one partition)**

- **DynamoDB:** Partition key = `"ratelimit#<key>"` or `"idempotency#<key>"`. One logical key maps to one partition. Throughput for that partition is limited by DynamoDB’s partition capacity.
- **Impact:** A single very hot key (e.g. one user or one shared API key) will hit one partition and can throttle. Mitigations: use capacity modes or on-demand billing, spread load with multiple keys where possible, and monitor throttles and backoff/retries.
