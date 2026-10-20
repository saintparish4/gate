# Service level objectives

Gate makes decisions that other services wait on before they spend money. These objectives
say how often a client can rely on getting such a decision, and how quickly. They are
**targets an operator holds the service to, not measurements**. Gate has no latency history
on real DynamoDB under sustained load (`docs/PERFORMANCE.md` is historical), so the numbers
below are where the objectives start, to be revised once a month of real traffic exists.

Every objective uses a **30-day rolling window**. The rules that measure them are
`observability/prometheus/rules/slo.yml`, tested by `observability/prometheus/tests/slo_test.yml`
(`make slo-check`). The Grafana dashboard *Gate — SLOs* (`observability/grafana/dashboards/gate-slo.json`)
shows each objective, its remaining budget, and its burn rate. The Helm chart ships both.

## What is not an SLO

**The limiters never over-issue.** No token bucket admits more than its capacity plus its
refill, no idempotency key runs its operation twice within its lease, and no quota admits
past its limit. That is an invariant, not an objective: it has no error budget, and
availability is never bought with it. When the store cannot be reached, Gate refuses
(`degradation-mode = reject-all`) and that refusal spends availability budget instead. The
invariant is checked by `make correctness` (invariants A-D) and the TLA+ specs in `spec/`.

## The objectives

| SLO | SLI: a good event is | Objective | Budget over 30 days |
| --- | --- | --- | --- |
| [Availability](#availability) | a `/v1` answer that is not a 5xx and does not carry `X-Gate-Degraded: true` | 99.9% | 0.1% of answers |
| [Latency](#latency) | a `/v1` answer whose headers leave the server within 100 ms | 99% | 1% of answers |
| [Usage freshness](#usage_freshness) | a minute in which the usage aggregator's slowest shard is under 60 s behind | 99% | about 7 hours |
| [Event delivery](#event_delivery) | a usage event that reaches Kinesis rather than being dropped | 99.9% | 0.1% of events |

<a id="availability"></a>
### Availability

**SLI.** `gate_http_requests_total` on routes matching `/v1/.+`, counted by every instance
and summed across all of them:

```
bad  = code =~ 5xx, or degraded = "true"
good = everything else, 4xx included
```

A 429 that says a bucket is empty is a correct answer and counts as good. A 429 or 200 that
`GracefulDegradation` gave without reaching the store is marked `X-Gate-Degraded` and counts
as bad: the client got an answer, but not the one the service exists to give. A 400 for an
impossible request is good. `/health`, `/ready`, `/metrics` and the dashboard are left out,
because a client's decision does not depend on them.

**Why 99.9%.** One DynamoDB table in one Region is the only store (ADR-001), and DynamoDB's
own regional SLA is 99.99%. Gate adds OCC rejections under contention, a circuit breaker, and
cold starts (the one failed AWS run in the README lost eight requests in its first seconds).
99.9% leaves room for those and still means a client sees roughly one failure in a thousand.

**Budget.** 0.1% of answers. At a steady rate that is about 43 minutes of total outage a month.

<a id="latency"></a>
### Latency

**SLI.** `gate_http_request_duration_seconds` on `/v1` routes: the share of answers in the
`le="0.1"` bucket. The time runs from the request reaching the service to its response
headers, so it leaves out the load balancer and the network to the client. A client's own
latency objective needs those added.

**Why 100 ms at 99%.** A rate-limit check is a consistent read and a conditional write, two
DynamoDB round trips of a few milliseconds each in-Region. 100 ms leaves room for OCC retries
on a contended key (ADR-004) and a garbage-collection pause, and it is a histogram bucket
boundary, which the SLI needs. On LocalStack, every route misses it under load; that is
LocalStack, and the objective is not meant for it.

**Budget.** 1% of answers may take longer than 100 ms.

<a id="usage_freshness"></a>
### Usage freshness

**SLI.** `gate_usage_millis_behind_latest`, reported by the usage aggregator for each shard
after every read. A minute is bad when the slowest shard is 60 seconds or more behind the
tip of the stream.

**Why it matters.** The aggregator turns the event stream into per-client usage (ADR-007). A
reader who is behind shows usage that is old. A reader more than the stream's retention
behind (24 hours by default) loses records for good, and the totals are short from then on.

**Budget.** 1% of minutes, about 7 hours a month. Separately, `GateUsageAggregatorBehind`
pages after ten minutes more than five minutes behind, and `GateUsageAggregatorAbsent` pages
when no aggregator reports at all.

<a id="event_delivery"></a>
### Event delivery

**SLI.** `gate_events_dropped_total` against `gate_events_published_total`: events the
service decided to publish but evicted from its full queue or failed to put twice. The
service publishes at most once (ADR-003), so a dropped event is missing from usage forever.

**Why 99.9%.** Usage totals are a lower bound on what the service decided, short by exactly
this ratio. At 99.9% a client's monthly totals are within a tenth of a percent. Usage that
must be exact, such as billing, should not rest on this stream; ADR-007 says what should.

**Budget.** 0.1% of events. `GateEventsDropped` opens a ticket when an hour drops more than that.

## Alerts

Availability and latency use multiwindow, multi-burn-rate alerts (the Google SRE workbook,
chapter 5). Burn rate is how many times faster than sustainable the budget is being spent: 1
spends exactly the 30-day budget in 30 days.

| Alert | Long window | Short window | Burn rate | Budget spent before it fires | Severity |
| --- | --- | --- | --- | --- | --- |
| `Gate*BurnRateFast` | 1 h | 5 min | 14.4 | 2% | page |
| `Gate*BurnRateMedium` | 6 h | 30 min | 6 | 5% | page |
| `Gate*BurnRateSlow` | 1 day | 2 h | 3 | 10% | ticket |
| `Gate*BurnRateChronic` | 3 days | 6 h | 1 | 10% | ticket |

The short window has to agree with the long one, so an alert clears within minutes of the
burn stopping rather than an hour later.

The rules send labels `severity` (`page` or `ticket`) and `slo`. Routing them is the
operator's Alertmanager's job; neither compose nor the chart configures receivers.

## When the budget is spent

These are the defaults this repository follows. An operator running Gate sets their own.

- **Budget left:** ship as usual.
- **Budget under 25%:** changes to the request path (`core`, `storage`, `resilience`, `api`)
  wait for a review of what spent it.
- **Budget gone:** only fixes for what spent it ship on the request path until the window
  recovers. The correctness invariant is never relaxed to buy availability.

## Reading them locally

`make obs` starts Prometheus with these rules and Grafana with both dashboards. A 30-day
window over a local stack is mostly empty, so the 30-day tiles read "no traffic" until a
load run fills them, and LocalStack misses the latency objective under any real load. The
burn-rate panels, which use windows from 5 minutes to 6 hours, are the ones to watch during a
local run.
