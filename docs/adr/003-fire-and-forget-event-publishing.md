# ADR-003: Fire-and-Forget Event Publishing

**Context:** Rate-limit decisions, idempotency checks, and audit events are published to Kinesis for analytics, dashboards, and compliance. We needed to decide whether event publishing should block the API response.

## Decision

Kinesis event publishing is fire-and-forget: events are enqueued to a bounded in-memory queue and drained by a background fiber. The HTTP response is never delayed by Kinesis latency or failures.

## Why Fire-and-Forget

- **Latency isolation.** A rate-limit check should complete in 5–20 ms (two DynamoDB round-trips). Kinesis `PutRecord` adds 10–50 ms. Making the response wait for Kinesis would double p50 latency for every request, which is unacceptable for a service whose entire purpose is to be fast and transparent.
- **Failure isolation.** Kinesis throttling, network blips, or shard-level errors should not cause rate-limit checks to fail or return errors to the caller. The rate-limit decision is the primary value; events are secondary.
- **Simplicity.** A bounded queue with a background drain is straightforward to implement and reason about. No distributed transaction coordination between DynamoDB and Kinesis.

## What's Sacrificed

- **At-most-once delivery.** If the process crashes between enqueuing an event and Kinesis acknowledging it, the event is lost. If the bounded queue is full when an event arrives, the event is dropped (with a `DroppedKinesisEvent` metric). There is no persistent outbox or retry-to-disk.
- **No ordering guarantee.** Events are published as fast as the drain fiber can send them. Under burst, ordering relative to the rate-limit decision timestamp may skew by milliseconds.
- **Compliance gap for audit events.** PCI DSS 4.0.1 Requirement 10.7 expects audit records to be reliably retained. Fire-and-forget means audit events *can* be lost. This is partially mitigated by structured logging (audit events are also logged to stdout/CloudWatch Logs), but logs are not a substitute for a durable event store.

## Mitigations

- **Structured log as secondary record.** Every audit event (`AuditEvent`) is logged as a structured INFO line before being enqueued to Kinesis. CloudWatch Logs provides searchability and short-term retention as a fallback.
- **Observable drops.** The `DroppedKinesisEvent` metric (CloudWatch) and `gate_events_dropped_total` (Prometheus) make event loss visible. Alerting on this metric is recommended.
- **Bounded queue with backpressure signal.** The queue size is configurable (`kinesis.queue-size`, default 10,000). When full, new events are dropped rather than causing unbounded memory growth.

## When to Reconsider

- **Regulatory audit requires provable delivery.** If an auditor requires proof that every denied request generated a durable audit record (not just a log line), the system needs either: (a) a transactional outbox pattern (write event to DynamoDB in the same conditional write as the rate-limit state), or (b) Kinesis publishing on the request path with retries and circuit-breaking.
- **Downstream consumers depend on event completeness.** If a billing system or quota reconciliation pipeline consumes Kinesis events and treats missing events as lost revenue, fire-and-forget is the wrong model.

## References

- `src/main/scala/events/Events.scala` — event types including `AuditEvent`
- `src/main/scala/events/EventPublisher.scala` — bounded queue + background drain
- `docs/COMPLIANCE.md` — audit trail claims (see ADR context for caveats)
