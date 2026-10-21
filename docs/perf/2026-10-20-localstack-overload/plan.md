# How gate saturates, fails and recovers on LocalStack (2026-10-20)

Written and committed before any gate run in this study. Changes after this
commit are logged under **Deviations** in `result.md`, with the reason, and the
result is also reported against this plan as written.

## Question

Against LocalStack only, under an AWS-shaped environment: at what offered load
does gate stop answering well (with 1, 2 and 3 replicas), how does it fail past
that point, how long does it take to recover, and does overload ever make it
over-issue or miscount usage?

**It would change:**
- the live AWS run's plan (its rates, durations, what to watch, what to expect);
- whether the failure order and shedding behavior need fixing before an AWS run.

A result that over-issues under overload would stop the AWS run and become a
bug.

## What carries over to AWS, and what does not

Decided now, so the result cannot pick its own scope.

- **Carries over:** gate's CPU per request (cgroup usage ÷ answers, at loads
  below saturation); the order in which gate's own limits engage (bulkhead,
  timeouts, circuit breaker, event queue); what the client sees in each mode;
  whether recovery is clean or metastable; whether invariants A-D and usage
  counting hold under overload.
- **Does not carry over:** any absolute latency, and any saturation rate that
  LocalStack sets. LocalStack is one Python process, roughly 10-50x slower than
  DynamoDB under concurrency (docker-compose.yml), and the saturation it causes
  is LocalStack's, not gate's.

## Instruments (built and checked before this plan)

| Instrument | What it is | Check |
| --- | --- | --- |
| Open-loop generator | `loadSim --scenario openloop`: sends on a fixed schedule whatever the server does; latency counted from the intended send time; one JSON line per request | the calibration below |
| Server split | `Server-Timing: store;dur=…, gate;dur=…` on every answer, and `gate_http_store_seconds` | `RequestTimingSpec`, exact on virtual time |
| Gauges | `gate_http_in_flight`, `gate_events_queue_depth`, `gate_bulkhead_in_flight{name}`, `gate_bulkhead_queued{name}`, sampled each second | — |
| Replica id | `X-Gate-Instance` on every answer | — |
| cgroups | each container's CPU used, CPU throttled (`nr_throttled`, `throttled_usec`) and memory, every second, from the host | — |
| JVM | GC log (`-Xlog:gc*`) and a JFR recording (`settings=profile`) per replica per run | — |

**Calibration** (generator on cpus 6-7, a fake server with a fixed 50 ms
delay on cpus 4-5, 3 repeats per rate, after a discarded 5 s warm-up):

| Offered | p50 measured | p99 measured | send-lag p99 |
| --- | --- | --- | --- |
| 50/s | 52.7-53.2 ms | 59.9-63.2 ms | ≤ 6 ms |
| 200/s | 51.1-51.2 ms | 62.7-63.9 ms | ≤ 5.7 ms |
| 500/s | 50.7 ms | 64.9-67.0 ms | ≤ 8.7 ms |
| 1000/s | 51.1 ms | 919 ms | 722 ms (invalid) |

So, on two hyperthreads, **the harness reads a true 50 ms as +0.7 to +3.2 ms at
p50 and +10 to +17 ms at p99, valid up to 500 requests a second.** Every stage
whose send-lag p99 exceeds 10 ms is marked generator-limited and not reported
as a server measurement.

**The clock.** Against an outside clock (HTTP `Date` from google.com, 315 s),
this laptop's `CLOCK_MONOTONIC` ran 4.7% slow; over a 60 s run it ran 3.5% slow;
the wall clock matched the outside clock to within 1 part in 300. The JVM, the
generator and gate all time with the monotonic clock, and the fake server's
sleep uses it too, so the calibration above cannot see this. **Every reported
duration and rate is rescaled by that run's own wall/monotonic ratio**
(recorded by the generator), with a stated residual of ±1.5%. A run whose
ratio falls outside 1.00-1.08 is marked suspect (a wall-clock step).

## Environment

`docker-compose.perf.yml`, on an AMD Ryzen 5 5625U (4 cores, 8 threads),
WSL2, Docker 29.5.2:

- each gate replica: `cpus: 1.0`, `mem_limit: 2g` (one Fargate task), sharing
  cpus 0-3 with the aggregator, nginx and Prometheus;
- LocalStack and Toxiproxy on cpus 4-5; the generator on cpus 6-7;
- production timeouts (rate limit 2 s, idempotency 2 s, quota 5 s),
  `DEGRADATION_MODE=reject-all`, circuit breaker at 20 failures with a 30 s
  reset, bulkhead 100 concurrent with a 500 ms wait, tracing off;
- the store reached through Toxiproxy in every condition, so a fault run
  differs only by its fault;
- a fresh stack per run (`down -v`, no LocalStack persistence).

Limits of this machine, stated now: three replicas at `cpus: 1.0` each share 2
physical cores (4 threads), so at 3 replicas gate cannot have 3 full cores; a
laptop changes clock speed with temperature, which interleaving spreads across
conditions but does not remove.

## Workload

Rate-limit checks only (`POST /v1/ratelimit/check`, cost 1, the premium test
key) over 10,000 keys, so there is almost no OCC contention. Contention is a
separate, known limit (4-8 RPS on one hot key, ADR-004). Under this workload,
a 429 without `X-Gate-Degraded` is a correct answer; a degraded 429 or any 5xx is
a failure.

## Conditions

**Phase 1: find saturation.** Five blocks; in each block, every condition once,
in an order shuffled with seed 20261020 (block number added).

| Condition | Replicas | Store | Stages (rate × seconds) |
| --- | --- | --- | --- |
| `ramp-r1`, `ramp-r2`, `ramp-r3` | 1, 2, 3 | LocalStack | 10×30 (warm-up, discarded), then 20, 40, 60, 80, 100, 120, 150, 180, 220, 270, 330, 400 × 15 s each |
| `localstack-alone` | none | LocalStack, called directly with a consistent `GetItem` then an `UpdateItem` per request (what a check asks of its store) | the same |
| `inmemory-r1` | 1 | gate's in-memory store, no Kinesis | 50×30 (warm-up), then 100, 200, 300, 400, 500 × 15 s |

The in-memory ramp stops at 500 because the generator is valid only that far;
if gate is still healthy at 500, the result is "≥ 500", said as such.

**Saturation point of a run** (fixed now): the highest offered stage, before
the first failing stage, where (a) the answered rate is at least 95% of the
offered rate, (b) failures (5xx, degraded, no answer) are under 1% of requests,
and (c) p99 latency is under 1 s. The condition's saturation point is the
median of its 5 runs, reported with its range and a bootstrap 95% interval of
the median. The interval cannot be narrower than one stage step; with 5 runs
it is wide, and is reported as wide.

**Phase 2: overload, recovery and faults.** After phase 1, with S = the median
saturation point measured for that replica count. Five blocks, shuffled as
above.

| Condition | Replicas | Profile | With |
| --- | --- | --- | --- |
| `hold1.5-r1`, `hold1.5-r3` | 1, 3 | 0.5S × 30 s, **1.5S × 180 s**, 0.5S × 120 s | invariants A-D started at 30 s |
| `hold3-r1`, `hold3-r3` | 1, 3 | 0.5S × 30 s, **3S × 180 s**, 0.5S × 120 s | invariants A-D started at 30 s |
| `fault-latency` | 3 | 0.5S × 240 s; Toxiproxy adds 1000 ms to every store response from 60 s to 120 s | invariants A-D started at 50 s |
| `fault-pause` | 3 | 0.5S × 240 s; `docker pause` of LocalStack from 60 s to 90 s | invariants A-D started at 50 s |
| `fault-aggregator` | 3 | 0.5S × 240 s; the aggregator killed at 60 s, restarted at 120 s | invariants A-D started at 50 s |

Holds last 180 s because the burn-rate pages have `for: 2m`: a shorter hold
could not show whether they fire. The invariants run beside the load at
concurrency 5 on the generator's cores. Every stage where that makes the
generator's send lag exceed 10 ms is marked generator-limited, as above.

After every run with the LocalStack store, `make usage-check` runs (published
against committed, per event type). Recorded per run: the per-request rows,
cgroup samples, Prometheus series (1 s scrape) and alerts, GC logs and JFR
recordings, invariants A-D, the usage check, image ids and the commit.

**Recovery time** (fixed now): from the moment the offered load drops back
(or the fault is removed) to the start of the first 10 s window in which every
answer is non-failing and p99 latency is under 2× its pre-overload value.

**Little's Law check** (fixed now): at each steady stage below saturation,
the client's L = answered rate × mean client latency, and the server's L = the
mean of `gate_http_in_flight` summed over replicas, against answered rate ×
mean server time. The two should agree within 15%; the client's L should
exceed the server's by the network and queueing outside gate.

## Predictions

Written before any run. Each is marked confirmed or refuted in `result.md`.

1. **LocalStack is the bottleneck.** `localstack-alone` saturates at 60-150
   request pairs a second. Gate's saturation, at every replica count, is at or
   below LocalStack's, and 3 replicas saturate within 30% of 1 replica.
   *Refuted if* `ramp-r3`'s median saturation exceeds 1.3× `ramp-r1`'s.
2. **Gate alone is far faster.** `inmemory-r1` stays healthy at ≥ 500/s on one
   vCPU (the generator's limit). Gate's CPU per check is 0.5-1.5 ms in memory
   and 1.5-3 ms with the AWS SDK and LocalStack (CLAUDE.md: about 450 checks
   per vCPU-second on AWS, about 2.2 ms).
3. **Failure order past saturation**, as offered load rises:
   1. latency first: p99 passes 1 s before any failure appears;
   2. the rate-limit bulkhead fills (100 in flight) and its 500 ms wait starts
      shedding: degraded 429s, `reason=bulkhead`;
   3. store timeouts (2 s) add degraded 429s with `reason=error`;
   4. the circuit breaker opens (20 failures) at 3S but not at 1.5S, and then
      every answer is a fast degraded 429 for 30 s;
   5. the event queue never evicts (batched drain): `gate_events_dropped_total`
      stays 0 in every run.
4. **Shedding is fast.** Once shedding, the median degraded answer returns in
   ≤ 600 ms (the 500 ms bulkhead wait) or, with the breaker open, in < 20 ms.
5. **No over-issue, no miscount.** Invariants A-D never report `VIOLATION`
   (`INCONCLUSIVE` from degraded answers is expected under overload), and the
   usage check never finds published ≠ committed. Dropped-before-Kinesis is
   0.
6. **Recovery is clean.** After a hold, recovery takes ≤ 40 s (the breaker's
   30 s reset plus draining); no run stays degraded after the load drops
   (metastable failure). *Refuted if* any run's recovery exceeds 60 s.
7. **Faults:**
   - `fault-latency` (+1000 ms per store call, so a check takes > 2 s):
     timeouts within seconds, the breaker opens within 10 s, degraded 429s
     until 30 s after the fault ends, then recovery ≤ 40 s after the fault
     ends.
   - `fault-pause`: the same shape, with timeouts rather than errors.
   - `fault-aggregator`: no effect on any answer; usage lag grows to about
     60 s and returns to under 5 s within 30 s of the restart; the usage check
     still finds every event committed once.
8. **What the operator sees.** `GateAvailabilityBurnRateFast` reaches
   `pending` within 30 s of degraded answers starting, and `firing` about 2
   minutes later, in the 3S holds and in `fault-latency`. It does not fire in
   the 1.5S holds unless failures exceed 1.44% for 2 minutes.
9. **Little's Law** holds within 15% at every stage below saturation.
10. **Gate is not CPU-throttled below saturation** (`nr_throttled` grows by
    under 1% of periods); past saturation with LocalStack it is still not, as
    it waits on LocalStack rather than computing.

My guess, and how sure: LocalStack bounds everything, and gate degrades
cleanly. 70% sure of predictions 1, 3 and 5. 50% sure of the exact failure
order in 3 (bulkhead before timeouts). 40% sure that recovery is as clean as
6 says, because retries on timeouts (up to 3) can multiply load on a slow
store.

## Stopping rule

Five runs per condition, fixed. No condition gets more runs because of its
result. A run that fails for an infrastructure reason (the stack does not
start, the generator exits early) is rerun once at the end of its phase, and
the failure is reported. No run is dropped for its numbers.

## Smoke runs

Before phase 1, the harness is exercised end to end at 20 requests a second
for 30 s, once per fault type, only to check that every artifact lands. Those
runs are not analyzed and are deleted.
