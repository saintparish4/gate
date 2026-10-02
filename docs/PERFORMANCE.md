# Performance

> **Status: historical.** The latency table below is one 60-second run per
> row, taken on 21 April 2026 against LocalStack on a laptop, before tenant
> scoping, the permission checks and the error-shape changes. It has no
> repetitions and no interval, and its first row ran cold. Do not quote it.
> No latency has been measured against real DynamoDB. A benchmark with
> repetitions on AWS is planned; until it exists, this page records how the
> old numbers were taken and what they cannot show.

Latency for `POST /v1/ratelimit/check` at a stated input RPS, measured with the
`latency` scenario in [loadSim](../loadSim/src/main/scala/LoadSim.scala). All
numbers are recorded with [HdrHistogram](http://hdrhistogram.org/) at
microsecond resolution and reported in milliseconds.

## How to reproduce

```bash
# 1. Start the full stack (LocalStack + app)
docker compose up -d

# 2. Wait for readiness
curl -sf http://localhost:8080/ready

# 3. Run the latency scenario at each target RPS (60 s each, 1,000 unique keys)
sbt "loadSim/run --scenario latency --rps 100  --duration 60"
sbt "loadSim/run --scenario latency --rps 500  --duration 60"
sbt "loadSim/run --scenario latency --rps 1000 --duration 60"
sbt "loadSim/run --scenario latency --rps 2000 --duration 60"
```

Each invocation prints a ready-to-paste markdown row. Paste them into the table
below.

The scenario uses **closed-loop** scheduling (each worker waits for its own
response before issuing the next request). Inter-request interval per worker =
`workers * (1s / target_rps)`. If the server cannot keep up at the target RPS,
"actual RPS" in the output falls below target and p99 rises accordingly — this
is the honest signal that the system is at capacity.

### Before your first run: the meta auth rate limit

Gate's auth middleware has its own per-key request ceiling (see
`AuthRateLimiter` in [`ApiKeyAuth.scala`](../src/main/scala/security/ApiKeyAuth.scala)).
It's keyed **per API key, per minute**, and the production default is 1,000
req/min. The load sim uses a single shared test key, so a real load test would
saturate the meta limit in the first second and get 429s for the rest of the
minute.

The `docker-compose.yml` dev stack already sets `AUTH_RATE_LIMIT_PER_MINUTE`
to a very large value for exactly this reason. If you run the app some other
way (bare JVM, a different compose file), export the env var yourself:

```bash
export AUTH_RATE_LIMIT_PER_MINUTE=10000000
```

The `latency` scenario has a 1% error budget: if that limit is in effect, the
run exits non-zero and prints `HTTP 429` with `rate_limited` as a sample error
instead of emitting a misleading markdown row. It also refuses to emit a row
if any answer came from degradation mode, since those never reached the store.

The scenario reads its key from the `API_KEY` environment variable (default
`test-api-key`), so it can run against a deployed stack after
`source .demo-keys.env`.

## Local results (LocalStack, dev machine)

> These numbers are from a developer laptop running LocalStack in Docker
> Desktop. LocalStack network overhead and single-container contention
> dominate all of these results. This page used to predict p50 of 5-10 ms and
> 1,000+ RPS per instance against real DynamoDB; nothing measured supports
> that. The only observation on AWS so far is from the correctness runs of 1
> October 2026, which are not a benchmark: one 1-vCPU task answered roughly
> 380 to 480 requests a second with its CPU at 100%, in a single run, timed by
> a client whose clock was later found to run about 3% slow.

| Target RPS | Actual RPS | p50 (ms) | p95 (ms) | p99 (ms) | p99.9 (ms) | Error % |
| ---------- | ---------- | -------- | -------- | -------- | ---------- | ------- |
| 100        | 38         | 202.8    | 571.4    | 656.4    | 2768.9     | 0.00    |
| 500        | 390        | 6.8      | 524.3    | 537.6    | 896.0      | 0.00    |
| 1000       | 948        | 6.9      | 59.5     | 217.7    | 808.4      | 0.00    |
| 2000       | 1319       | 31.2     | 212.4    | 409.6    | 1858.6     | 0.00    |

*Numbers above were captured Apr 21 2026 against the docker-compose dev stack
(LocalStack DynamoDB + rate-limiter) on a single developer laptop.*

### Reading the table

- **Each row is one run.** There is no second run of any rate, so there is no
  way to say how much of a difference between two rows, or between this table
  and a later one, is noise.
- **The 100 RPS row measures start-up, not 100 RPS.** It ran first, against a
  cold stack, and achieved 38 RPS at a p50 of 203 ms. This page used to
  explain that as JIT warm-up and LocalStack waking; that was never tested by
  running the row again warm.
- **Actual RPS < target past 500.** The `latency` scenario uses closed-loop
  scheduling: a worker can't send its next request until the previous one
  returns. Once per-request latency > the target inter-request interval, the
  driver cannot reach the target. At 2000 target RPS it reached 1319. Whether
  LocalStack or the service was the limit was not measured.
- **Zero errors, zero 429s across the sweep.** The service stayed responsive
  for 60 s at every rate.

Each invocation prints a ready-to-paste markdown row.

## DynamoDB cost per decision

These figures are derived from the code and from published prices. They are
not measured: nothing here was read from CloudWatch's consumed capacity.

A `/v1/ratelimit/check` on the token-bucket path makes these DynamoDB calls
(see the [OCC sequence diagram](../README.md#optimistic-concurrency-control-flow)):

1. `GetItem` with `ConsistentRead=true` on the single item for the key: 1 read
   request unit.
2. If the check is admitted, a conditional `PutItem` (`version = current`): 1
   write request unit. A refused check does not write.

Under hot-key contention the conditional `PutItem` can fail. A failed
conditional write is still charged, and each of up to 10 retries reads and
writes again; `RateLimitOCCAttempts` in CloudWatch shows how often.

### Estimated spend at AWS on-demand pricing (us-east-1)

| Cost component                | Unit price (on-demand, us-east-1) | Per check | Per 1M checks |
| ----------------------------- | --------------------------------- | --------- | ------------- |
| Strongly consistent `GetItem` | $0.125 per 1M read request units  | 1 RRU     | $0.125        |
| Conditional `PutItem`         | $0.625 per 1M write request units | 1 WRU     | $0.625        |
| **An admitted check**         |                                   | 1 + 1     | **~$0.75**    |
| **A refused check**           |                                   | 1 read    | **~$0.125**   |

Worst case, with all 10 retries on every check: 11 reads and 11 writes, about
$8.25 per 1M checks.

The other two engines do not follow this pattern:

- **Idempotency.** A check is one conditional `PutItem`, charged whether or not
  the claim wins, and a `GetItem` when it loses. `complete` is an `UpdateItem`
  that stores the response inline, charged per KB of the item: up to about 350
  write request units for the largest response the API accepts.
- **Token quota.** A check reads each level (user, agent, org) with a
  consistent `GetItem`, writes them with one `PutItem` for a single level or a
  `TransactWriteItems` for several, which is charged at 2 write request units
  per item, and then writes the reservation with another `PutItem`. Reconcile
  is a transaction.

> Prices were read from the AWS pricing API on 1 October 2026. This page
> previously showed $0.25 and $1.25 per million, twice the current rates, and
> said every engine made exactly one read and one write per decision. Verify at
> <https://aws.amazon.com/dynamodb/pricing/on-demand/> before quoting.

## Notes on honesty

- The `latency` scenario is closed-loop, not open-loop. When the server slows
  down, a closed-loop driver sends less, so the requests that would have waited
  longest are never sent (coordinated omission). Read its tail percentiles as a
  lower bound on what a steady arrival stream would see.
- LocalStack is a DynamoDB *emulator*, not DynamoDB. Use these numbers to
  compare changes against each other, not as a stand-in for production.
- All scenarios are reproducible from source. No hand-tuned JVM flags, no warm
  caches pre-loaded. The only server-side warm-up is the first request in each
  run (which hits the JIT cold).
