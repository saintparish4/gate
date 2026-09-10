# Rate limiting across stateless instances, without a lock service

*A distributed token bucket on DynamoDB conditional writes — what it buys, what
it costs, and the number I decided to publish rather than hide.*

---

The requirement is ordinary: enforce "at most X requests per second for this
key" across N stateless containers, where any container can serve any request
and containers come and go.

The ordinary answer is Redis. `INCR` with a TTL, or a Lua script for a proper
token bucket. It is atomic, it is fast, and it is one round trip.

I did not use Redis. Not because it is wrong — for hot-key workloads it is
clearly better, and I will say so below — but because the trade it asks for was
the wrong one for this system.

## What a lock service actually costs

Reaching for Redis to coordinate token consumption means adding a stateful
component whose failure modes are now yours:

- Redis is down. Do you fail open (over-issue, possibly badly) or fail closed
  (reject everything)? Both answers are bad and you must pick one.
- Redis is up but slow. Your rate limiter's p99 is now Redis's p99, on a
  component you are paying to keep warm.
- Redis is a single point of truth. Making it highly available means Sentinel
  or Cluster, which means failover semantics, split-brain, and a second
  on-call runbook.
- If you use it for *locking* rather than atomic ops, you inherit lock lease
  expiry, acquisition timeouts, and "lock held by an instance that died."

For a service whose entire job is to be more reliable than the thing it
protects, that is a lot of new failure surface. And the state was already
going to be in DynamoDB — which is managed, multi-AZ by default, and has no
failover story I have to write.

So: can DynamoDB alone do it, atomically, with no lock anywhere?

## Optimistic concurrency control

Yes, via conditional writes on a version field.

```
1. GetItem, consistentRead = true          -> state at version N
2. refill tokens by elapsed time
3. deduct the request cost
4. PutItem, condition: version = N         -> becomes N+1
5. if ConditionalCheckFailedException      -> another instance won; retry from 1
```

The correctness argument is short, which is the point. Two instances both read
`tokens = 5` and both try to consume 3. Both attempt to write with
`condition: version = N`. **DynamoDB will accept exactly one.** The loser gets
`ConditionalCheckFailedException`, re-reads, sees `tokens = 2`, and makes a new
decision with real state.

There is no window in which both succeed. Not "unlikely" — the condition is
evaluated inside DynamoDB's own single-item write path, and single-item writes
are serialised.

That is the whole mechanism. No lock, no lease, no TTL to tune, no split-brain,
and exactly one failure mode: a deterministic, retryable exception.

## The decision that actually matters

Retries are bounded — ten attempts, 25 ms base, 2× backoff, 10% jitter.

What happens on the eleventh?

**The request is rejected with a 429.**

This is the design decision I would defend hardest, because the alternative is
so tempting. After ten failed attempts you have a request that has done nothing
wrong, from a caller probably inside their quota, and you could just let it
through. One request. Who would notice?

The answer is that a rate limiter which lets requests through when it is
confused is not a rate limiter. Its entire value is that the invariant holds
when the system is under stress — and heavy contention *is* the stress case.
Degrading to "allow" exactly when contention is highest means the guarantee
evaporates at precisely the moment it is load-bearing.

So the system **under-issues at the tail rather than over-issuing**, and the
429 it returns is honest: it is saying "I could not establish that you are
within your limit," which under contention is true.

The same reasoning drives the degradation mode when DynamoDB itself is
unreachable. It is configurable — `reject-all` or `allow-all` — because the
right answer genuinely differs by domain. `reject-all` is the default, because
a payments API that double-charges is a worse outcome than a payments API that
is briefly unavailable. An AI gateway with a spend cap may reasonably choose
`allow-all` and eat the overage. What is not acceptable is having no position.

## What it costs, stated plainly

OCC is not free, and the bill arrives as tail latency under hot-key
contention. Measured, at 50 concurrent writers on a **single key**:

| | |
|---|---|
| Throughput | 4–8 requests/second, per key |
| p99 latency | ~13 seconds |
| Worst-case DynamoDB ops | 20 per request (10 retries × read + write) |

Those are bad numbers and I am publishing them, because a design document that
only lists advantages is marketing.

They are also narrower than they look. Rate-limit keys are normally per-user or
per-API-key, so contention is spread across thousands of items and the common
path is two round trips with zero retries. The pathological case is a *global*
limit shared by every caller — one item, all the traffic.

If that is your access pattern, OCC is the wrong tool and there are two exits,
both of which I would take before adding Redis:

- **Key sharding.** Split one logical limit across N items and give each 1/N of
  the capacity. Contention drops by N. You lose exactness at the boundary.
- **Single-round-trip `UpdateItem`.** `SET tokens = tokens - :cost IF tokens >= :cost`
  removes the read entirely and with it the retry loop. It halves cost and
  latency on the normal path. The reason it is not the current implementation
  is that time-based refill has to be expressed in DynamoDB expression syntax,
  and I would rather ship a correct loop than a clever expression I cannot test
  as thoroughly.

And if the workload really is sustained single-key contention above ~50
writes/second? Use Redis. The trade genuinely flips. Knowing where your design
stops being the right one is not a weakness in it.

## The part that generalises

Three things I would carry to any distributed-state problem:

**A conditional write is a lock you do not have to operate.** If your state
lives in a store that offers compare-and-set, you very likely do not need a
lock service. You need a retry loop and a bounded one.

**Bound the retries and decide, explicitly, what happens at the bound.** An
unbounded retry loop is a latency bomb. A bounded one forces you to answer
"and then what?" — and that answer *is* your consistency guarantee. Mine is
"reject," written down, tested, and enforced in CI.

**Publish the number that makes your design look worst.** The 13-second p99 is
the most useful line in the whole design document. It tells a reader exactly
where this stops working, which is the only thing that lets them decide whether
it works for them.

---

*Source: [`saintparish4/keyra`](https://github.com/saintparish4/keyra) — Gate,
a distributed rate limiter, idempotency service, and multi-level LLM token
quota engine. Scala 3, Cats Effect, DynamoDB. ~13,500 lines, of which ~6,000
are tests. Three invariants gated in CI against a live stack: token-bucket
non-over-issue, idempotency exactly-one-Created, token-quota
non-over-admission.*
