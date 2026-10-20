# Invariants across replicas, 20 October 2026

Two local runs, each with **more than one service instance over one shared store**, at commit
`5276e99` plus the uncommitted work later committed as `ebb8e6b`..`HEAD` on `fix/truth`
(the servers report `v0.1.0-108-g5276e99-dirty`). Both use LocalStack, so they show that the
correctness claims hold across processes; they say nothing about AWS latency or throughput.
One run per condition is an anecdote: these are not a reliability history.

## Compose: three replicas behind nginx (`make stack-scaled`)

Three `rate-limiter` containers, round-robin nginx on `:8080`, one LocalStack, the usage
aggregator, Prometheus scraping each replica.

**Correctness, run 1792536390218** ([results](correctness-1792536390218.json)): `PASS`, `errors=0`.

| Invariant | Result |
| --- | --- |
| A, token bucket | allowed 80, bounds 20..86 (server window 31 s) |
| B, idempotency | 118 keys, each answered `new` exactly once |
| C, quota | 4 users, 40/40/40/40 admitted, max 40 |
| D, isolation | A=40, B=40 admitted; 599/599 stolen reconciles refused |

**Spread.** `/v1` answers per replica: 2,418 / 2,413 / 2,412. Three 503s in all, on
`/v1/quota/check` (two on one replica, one on another), logged as `Token quota check failed`.
They were not investigated further. They are why the HTTP metrics now create each
route's failure series at zero (`5431456`): before that, Prometheus read them as an error
ratio of exactly 0.

**Usage pipeline** (`make usage-check`, once the service's queues had drained): every one of
9,462 published events was committed to usage exactly once, type by type, with 0 dropped
before Kinesis. Draining took over a minute after the run ended: one fiber per replica puts
one record at a time, about 15 events/s per replica on LocalStack (`base/next-steps.md`, E1).

**k6** (`scripts/load-test.sh local quick`; [summary](k6-quick-summary.json)): 465 requests,
0 failed, 0 degraded, p95 35.8 ms, p99 94.6 ms. This profile is a smoke test, about 7
requests a second with think time.

**Fixed rate** (`loadSim --scenario latency --rps 100 --duration 60`): 4,510 requests, 75.2
RPS achieved (closed loop, 10 workers), 0 errors, 0 degraded. p50 126.6 ms, p95 185.2 ms,
p99 261.9 ms, p99.9 595.5 ms. LocalStack is the bottleneck here, not the replicas.

## kind: the Helm chart, two pods (`scripts/kind-rehearsal.sh`)

A three-node kind cluster (Kubernetes 1.31.2), LocalStack in the cluster, the chart installed
with `ci/kind-values.yaml`: two fixed replicas on different worker nodes, non-root with a
read-only root filesystem, keys read from LocalStack's Secrets Manager (three random keys,
not the built-in ones), and traffic through a NodePort.

**Correctness, run 1792537446751** ([results](correctness-1792537446751-kind.json)): `PASS`, `errors=0`.

| Invariant | Result |
| --- | --- |
| A, token bucket | allowed 81, bounds 20..84 (server window 30 s) |
| B, idempotency | 119 keys, each answered `new` exactly once |
| C, quota | 3 users, 39/40/40 admitted, max 40; 9 contended |
| D, isolation | A=40, B=40 admitted; 461/461 stolen reconciles refused |

**Spread.** `/v1` answers per pod: 4,554 / 3,594. kube-proxy balances per connection, and
the simulator keeps connections alive.

The laptop's monotonic clock disagreed with its wall clock by 3.8-5.0% during these runs
(`CLAUDE.md`, "Local monotonic clock"). Invariant A uses the server's `Date` headers, so its
verdict does not depend on it.
