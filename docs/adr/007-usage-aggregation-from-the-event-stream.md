# ADR-007: Aggregate Usage from the Kinesis Event Stream

**Context:** Gate decides, per request, whether a client may spend: a rate-limit token, an idempotency claim, a share of a token quota. Each decision is published as an event (ADR-003), but nothing read the events back. An operator could not answer "how much did client X use yesterday, minute by minute" without querying CloudWatch Logs.

The decision counters in DynamoDB are not that answer. A token bucket holds what is left, not what was spent; a quota counter holds the current window and expires; a reservation record expires after reconcile. They are built to decide, and they forget on purpose.

## Decision

A separate process, the **usage aggregator** (`aggregator/`), reads the Kinesis stream the service already writes and folds it into per-client, per-UTC-minute sums in a DynamoDB table (`gate-usage`). It is its own deployable, with its own image, metrics, and freshness objective (`docs/SLO.md`).

The service gains one event: `quota_reconciled`, carrying the actual input and output tokens of a reconciled reservation. It is published once per reservation; a replayed reconcile (`ReconcileResult.Reconciled(applied = false)`) answers the same but publishes nothing.

## Kinesis, Not Kafka

- **It is already there.** The service publishes to Kinesis today, Terraform provisions the stream, and LocalStack serves it to compose and to CI. Kafka would add a broker (MSK or self-run), a client library, and a second event path to keep in step with the first.
- **The volume does not need Kafka.** One shard takes 1,000 records or 1 MB a second. A single 1-vCPU task is CPU-bound at about 450 decisions a second, so a shard per two tasks or so is ample.
- **What Kafka would buy is not needed here.** Long retention, compaction, and many independent consumer groups at high fan-out are Kafka's strengths. Usage has one consumer, and the stream only has to hold records until it has read them.

Revisit if the stream gains several independent consumers, or retention beyond Kinesis's 365 days becomes a requirement.

## How It Counts Each Record Once

The aggregator does not use the Kinesis Client Library. It reads each shard with `GetRecords` and keeps its own checkpoint, a row in the same table. The rule:

1. Read the shard's checkpoint (a consistent read) and open an iterator just after it.
2. Read a batch, split it into chunks that touch at most 99 buckets, and commit each chunk with **one `TransactWriteItems`**: an `ADD` to every bucket it touches, and a move of the checkpoint from where it was to the chunk's last sequence number, **conditioned on the checkpoint still being where it was**.
3. On any doubt (a failed condition, an error, a timeout, an expired iterator) go back to step 1.

The sums and the checkpoint commit together or not at all, so a timed-out commit is resolved by reading the checkpoint: either it moved, and the chunk counted, or it did not, and the chunk is read again. A second reader on the same shard, or a late-landing commit from an attempt the reader gave up on, fails the condition rather than adding twice. Sums commute, so shards are read in any order and a closed shard after a reshard is simply read to its end.

`ConsumerSpec` checks this against lost answers, failed reads, and two readers on the same shards; it fails when the checkpoint condition is removed.

## What's Sacrificed

- **The totals are a lower bound.** The service publishes at most once (ADR-003): a full queue evicts, a twice-failed put drops, and a crash loses the queue. The aggregator counts exactly what reached the stream, so the totals are short by `gate_events_dropped_total` plus anything lost in a crash. The *event delivery* SLO watches the first; nothing measures the second.
- **Not billing-grade.** For that reason usage from this stream is for visibility, alerting, and showback. Charging a customer from it would undercharge whenever events were lost, silently. Billing needs a transactional outbox written with the decision (ADR-003, "When to Reconsider").
- **Retention bounds an outage.** A reader that is down longer than the stream's retention (24 hours by default) loses the records that expired. The freshness alerts page long before that.
- **One reader at a time is the efficient shape.** Two readers stay correct but spend writes on fenced commits. The Helm chart runs one replica with the `Recreate` strategy.
- **Unreconciled reservations are not usage.** Only reconciled tokens are counted. A caller that never reconciles shows its admitted checks in the service's metrics (`gate_quota_tokens_admitted_total`), not in usage.

## References

- `aggregator/src/main/scala/aggregator/Consumer.scala`: the read-and-commit loop
- `aggregator/src/main/scala/aggregator/DynamoDBUsageStore.scala`: the transaction and its condition
- `aggregator/src/test/scala/aggregator/ConsumerSpec.scala`: exactly once under faults and concurrent readers
- `docs/SLO.md`: usage freshness and event delivery
