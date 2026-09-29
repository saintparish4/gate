# ADR-003: Fire-and-Forget Event Publishing

**Context:** Rate-limit decisions, idempotency checks, and audit events are published to Kinesis for analytics, dashboards, and compliance. We needed to decide whether event publishing should block the API response.

## Decision

Kinesis event publishing is fire-and-forget: events are enqueued to a bounded in-memory queue and drained by a background fiber. The HTTP response is never delayed by Kinesis latency or failures.

## Why Fire-and-Forget

- **Latency isolation.** A rate-limit check costs two DynamoDB round-trips. Making the response also wait for a Kinesis `PutRecord` would add a third network call, and its tail latency, to every request, for data the caller never sees.
- **Failure isolation.** Kinesis throttling, network blips, or shard-level errors should not cause rate-limit checks to fail or return errors to the caller. The rate-limit decision is the primary value; events are secondary.
- **Simplicity.** A bounded queue with a background drain is straightforward to implement and reason about. No distributed transaction coordination between DynamoDB and Kinesis.

## What's Sacrificed

- **At-most-once delivery.** If the process crashes between enqueuing an event and Kinesis acknowledging it, the event is lost. If the bounded queue is full when an event arrives, the oldest queued event is evicted to make room, counted as `DroppedKinesisEvent{reason=queue_full}`. A publish that fails is retried once, then dropped and counted as `DroppedKinesisEvent{reason=publish_failed}`. There is no persistent outbox or retry-to-disk.
- **No ordering guarantee.** Events are published as fast as the drain fiber can send them. Under burst, ordering relative to the rate-limit decision timestamp may skew by milliseconds.
- **Compliance gap for audit events.** PCI DSS 4.0.1 Requirement 10.7 expects audit records to be reliably retained. Fire-and-forget means audit events *can* be lost. This is partially mitigated by logging (each audit decision is also written to stdout, and so to CloudWatch Logs when deployed), but logs are not a substitute for a durable event store.

## Mitigations

- **Log line as secondary record.** Each place that builds an `AuditEvent` (a rejected rate-limit check, a refused profile, an exceeded quota, an idempotency conflict) also logs a plain-text line, `AUDIT decision=<decision> ...` with key=value fields. The log format is a text pattern, not JSON, so a query has to parse the message. CloudWatch Logs provides searchability and retention as a fallback (7 days by default in Terraform, 30 in `prod`).
- **Observable drops.** The `DroppedKinesisEvent{reason}` metric (CloudWatch) and `gate_events_dropped_total` (Prometheus) make event loss visible. The monitoring module has no alarm on it; add one if events matter to you.
- **Bounded queue.** The queue size is configurable (`kinesis.queue-size`, default 10,000). When full, the oldest events are evicted, so memory stays bounded and the queue holds the newest events.

## When to Reconsider

- **Regulatory audit requires provable delivery.** If an auditor requires proof that every denied request generated a durable audit record (not just a log line), the system needs either: (a) a transactional outbox pattern (write event to DynamoDB in the same conditional write as the rate-limit state), or (b) Kinesis publishing on the request path with retries and circuit-breaking.
- **Downstream consumers depend on event completeness.** If a billing system or quota reconciliation pipeline consumes Kinesis events and treats missing events as lost revenue, fire-and-forget is the wrong model.

## References

- `src/main/scala/events/Events.scala` — event types including `AuditEvent`
- `src/main/scala/events/KinesisPublisher.scala` — bounded queue, background drain, one retry, drop metering
- `src/main/scala/events/EventPublisher.scala` — the publisher interface
- `docs/COMPLIANCE.md` — audit trail claims (see ADR context for caveats)

## Corrections

- 27 September 2026: this ADR said a full queue drops the new event and that audit events are logged as structured lines. The queue evicts the oldest event, and before that change evictions were not counted at all. Audit lines are plain text.
