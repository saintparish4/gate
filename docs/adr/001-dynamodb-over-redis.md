# ADR-001: DynamoDB Over Redis for State Storage

**Context:** Rate-limit and idempotency state needs a durable, distributed store accessible by all service instances.

## Decision

Use DynamoDB as the sole state store for rate-limit buckets, idempotency records, and token quota counters.

## Why DynamoDB

- **Fully managed.** No cluster provisioning, failover configuration, or shard rebalancing. AWS handles replication, backups, and patching.
- **Single-stack consistency.** The rest of the infrastructure (ECS, Kinesis, CloudWatch) is AWS-native. Adding Redis (ElastiCache or self-hosted) introduces a second stateful service with its own failure modes, monitoring, and upgrade lifecycle.
- **OCC fits the access pattern.** Each rate-limit key maps to one DynamoDB item. Conditional writes (`version = :expected`) give optimistic concurrency without a lock service. Under typical per-user/per-key traffic, contention is low and the common path is two round-trips (GetItem + PutItem).
- **TTL for free.** DynamoDB TTL automatically deletes expired rate-limit, idempotency, and quota items. No cron jobs, no manual cleanup.
- **Strong consistency available.** `consistentRead(true)` gives read-your-writes semantics — critical for accurate token counts. Redis Cluster offers eventual consistency by default; achieving strong consistency requires Lua scripts or WAIT commands with caveats.

## What's Sacrificed

- **Latency.** DynamoDB consistent reads typically run 2–10 ms per round-trip in the same region. Redis in-memory reads are sub-millisecond. Each uncontended rate-limit check costs two DynamoDB round-trips (~5–20 ms total), versus ~1–2 ms with Redis.
- **Throughput ceiling per key.** A single DynamoDB item lives on one partition. Under extreme hot-key contention (50+ concurrent writers on one key), OCC retries dominate — measured at 4–8 RPS per key with p99 latency of ~13 seconds locally. Redis atomic `DECR` would handle this without retries.
- **Cost at very high scale.** DynamoDB on-demand pricing is competitive for moderate traffic, but at millions of RPS, provisioned Redis can be significantly cheaper per operation.

## When to Reconsider

- **p99 latency SLA drops below 5 ms.** If the service sits in the critical path of a latency-sensitive API that requires sub-5ms rate-limit checks, DynamoDB cannot meet this in the general case.
- **Single-key throughput exceeds ~100 concurrent writes/sec.** The OCC retry ceiling (10 retries, each requiring a re-read + re-write) breaks down under sustained hot-key contention. If the access pattern is inherently hot-key (e.g., a global rate limit shared across all users), Redis atomic counters or DynamoDB `UpdateItem` expressions (single-round-trip optimisation) should be evaluated.
- **The team already operates Redis.** If ElastiCache is already in production with monitoring, failover, and operational playbooks, the "avoid a second stateful service" argument weakens.

## References

- `src/main/scala/storage/DynamoDBRateLimitStore.scala` — OCC implementation
- `docs/ARCHITECTURE.md` — request paths and round-trip counts
- `docs/PERFORMANCE.md` and `README.md` § "Performance" — measured latency and hot-key throughput
