# ADR-004: Optimistic Concurrency Control Over Pessimistic Locking

**Context:** Multiple stateless service instances must atomically read-modify-write the same token-bucket state in DynamoDB without over-issuing tokens. We needed a concurrency control strategy.

## Decision

Use optimistic concurrency control (OCC) via DynamoDB conditional writes on a `version` field. No distributed lock service.

## How It Works

1. **Read:** `GetItem` with `consistentRead(true)` fetches the current bucket state, including `version = N`.
2. **Compute:** Refill tokens by elapsed time, then deduct the request cost.
3. **Write:** `PutItem` with condition `version = N` (or `attribute_not_exists(pk)` for first write). If the condition succeeds, the write is atomic and the version becomes `N+1`.
4. **Conflict:** If another instance wrote `version = N+1` between our read and write, DynamoDB raises `ConditionalCheckFailedException`. We retry from step 1 with `RetryPolicy.occRetry`: up to 10 retries, 1 ms base delay, 1.5× multiplier, 50 ms cap, 20% jitter.
5. **Exhaustion:** After 10 failed retries, the request is **rejected** (HTTP 429). This is a safety choice: the system under-issues rather than over-issues.

All three rate-limit stores (token bucket, leaky bucket, sliding window) use this loop and policy. The token-quota store applies the same read, check, conditional-write pattern across its levels. It uses one conditional `PutItem` for a single level and `TransactWriteItems` for several, with its own loop of up to 25 attempts; on exhaustion it answers 503 with `Retry-After: 1`, not 429.

## Why OCC

- **No lock service.** Pessimistic locking requires a distributed lock store (Redis, ZooKeeper, or a DynamoDB-based lock table) with its own failure modes: lock acquisition timeouts, lock lease expiry, deadlock detection, and a separate monitoring/alerting surface.
- **Simpler failure modes.** The only failure is `ConditionalCheckFailedException`, which is deterministic and retryable. There is no "lock held by a crashed instance" scenario.
- **Good enough under typical contention.** Rate-limit keys are usually per-user or per-API-key. With thousands of keys spread across users, the probability of two instances racing on the same key in the same millisecond is low. The common path is 2 DynamoDB round-trips with zero retries.
- **Correct by construction.** OCC cannot over-issue tokens. If two instances both read `tokens = 5` and both try to consume 3, only one write succeeds. The other retries with fresh state. In the worst case (retry exhaustion), the request is rejected — safe.

## What's Sacrificed

- **Throughput under hot-key contention.** With 50 concurrent writers on a single key, OCC retries dominate: measured 4–8 RPS per key with p99 latency of about 13 seconds, against LocalStack (see the README's Performance section). Each retry costs two more DynamoDB round-trips (re-read + re-write). Worst case: 11 attempts × 2 round-trips = 22 DynamoDB operations per request.
- **Tail latency.** Even at moderate contention (5–10 concurrent writers on one key), the retry loop introduces variable latency. p50 may be fine (5–20 ms) but p99 can spike.
- **DynamoDB cost.** Failed conditional writes are still billed as write capacity units. Under contention, the effective cost per successful rate-limit check increases linearly with retry count.

## Contention Ceiling

The current implementation retries up to 10 times with `RetryPolicy.occRetry` (1 ms base delay, 1.5× backoff, 50 ms cap, 20% jitter). The backoff totals about 110 ms across all 10 retries, so a check that exhausts them spends most of its time in the 22 DynamoDB calls, not in sleeping. After the tenth retry fails, the request is rejected.

This ceiling means: for any single key, sustained concurrent write throughput is bounded by DynamoDB single-item write throughput divided by average attempts per success. The only measurement so far is the one above: about 50 RPS on one key with little contention, falling to 4–8 RPS with 50 concurrent writers, on LocalStack.

## Alternatives Considered

- **DynamoDB Transactions (`TransactWriteItems`).** Provides atomicity but at 2× the write cost. Overkill for a single-item read-modify-write. The token-quota store does use one, because a check must commit the user, agent, and org counters together or not at all.
- **Single-round-trip `UpdateItem` expression.** `SET tokens = tokens - :cost IF tokens >= :cost` eliminates the read round-trip. This halves DynamoDB consumption and latency under normal load but requires restructuring the refill logic into a DynamoDB expression. Tracked as a future optimisation.
- **Redis `DECR` / Lua script.** Atomic single-operation token decrement with no retry loop. Superior throughput under hot-key contention, but introduces Redis as a dependency (see ADR-001).

## When to Reconsider

- **Sustained hot-key traffic.** If a legitimate access pattern produces sustained single-key concurrency above ~50 writes/second (e.g., a global rate limit shared by all users), OCC retry exhaustion will cause frequent 429s unrelated to actual rate-limit quota. Solutions: key sharding (split one logical limit across N DynamoDB items), or the single-round-trip `UpdateItem` optimisation.
- **Cost pressure.** If DynamoDB bills become dominated by failed conditional writes, it signals contention is too high for OCC to be efficient.

## References

- `src/main/scala/storage/DynamoDBRateLimitStore.scala` — OCC retry implementation using `RetryPolicy.occRetry` (also `LeakyBucketRateLimitStore.scala`, `DynamoDBSlidingWindowStore.scala`)
- `src/main/scala/resilience/RetryPolicy.scala` § `occRetry` — 10 retries, 1 ms base, 1.5× backoff, 50 ms cap, 20% jitter
- `src/main/scala/storage/DynamoDBTokenQuotaStore.scala` — the quota store's own loop (25 attempts) and transaction
- `src/main/scala/storage/DynamoDBOps.scala` — `conditionalPut` helper
- `src/test/scala/core/TokenBucketOccBoundSpec.scala` — the no-over-issue bound under adversarial latency
- `docs/ARCHITECTURE.md` — request paths and round-trip counts
- `README.md` § "Performance" — measured contention impact

## Corrections

- 27 September 2026: this ADR named `RetryPolicy.dynamoDB` (25 ms base, 2× backoff, about 6 s of delay). The stores use `RetryPolicy.occRetry`, whose backoff totals about 110 ms. The ADR also gave the `TransactWriteItems` limit as 25 items; DynamoDB now allows 100.
