# Specifications

TLA+ models of gate's three protocols, model-checked with TLC. They state what
"correct" means without leaning on gate's own tests, and they search every
interleaving within small bounds rather than sampling a few. Run them with
`make spec` (about 6 minutes on an 8-core laptop; Java 11+).

| Module | Protocol | Code it follows |
| --- | --- | --- |
| `RateLimitOCC` | token bucket: consistent read, refill, version-conditioned PutItem, OCC retry then reject, wrapper retry after a timeout | `core/TokenBucket.scala`, `storage/DynamoDBRateLimitStore.scala`, `resilience/ResilientWrapper.scala` |
| `Idempotency` | claim, read-back, reclaim of Failed or expired records, complete and fail with and without `claimId` | `storage/DynamoDBIdempotencyStore.scala`, `api/IdempotencyApi.scala` |
| `Quota` | reserve against a fixed window, reservation record, release, reconcile in one transaction | `core/TokenQuotaService.scala`, `storage/DynamoDBTokenQuotaStore.scala` |

`expected.txt` lists every config with its expected verdict: `pass`, or the
invariant TLC must report violated. The violations are the known
counterexamples below. A fix changes its line to `pass` in the same commit, and
a spec edit that stops finding a known counterexample fails the check instead of
looking like a fix.

## What is assumed about DynamoDB

- A conditional PutItem or UpdateItem is atomic on its item, and
  TransactWriteItems applies all of its writes or none.
- A consistent read returns the latest committed write.
- TTL deletes an expired item at any time after its `ttl`. AWS documents
  deletion as typically within a few days, not as a bound, so the model allows
  any delay, including none.
- A write whose call times out may still have committed. The model lets any
  committed write lose its answer.
- Time is whole ticks of one clock, which is also DynamoDB's clock for TTL. Each
  task reads that clock plus its own offset, in `0..MaxSkew`. In
  `RateLimitOCC` the offset can also step once (an NTP step or VM resume).

## Properties

**Rate limit, safety: `NeverOverIssue`.** The reference is RFC 2697's token
bucket in arrival-curve form. Charges conform to a bucket of `(Cap, Rate)` that
starts full exactly when every interval `[t1, t2]` holds at most
`Cap + Rate * (t2 - t1)` of them. Each committed charge is stamped with the real
time of the read that decided it, since a request may be linearized anywhere
between its call and its answer. The allowance is `Rate * MaxSkew`, the minting
`core.TokenBucket` admits to for a forward clock correction.

**Rate limit, efficiency: `NoNeedlessRefusal`.** A refusal for lack of tokens
happens only when the reference bucket, at the same instant and short by the
skew allowance, could not pay either. Refusals after OCC retries run out are
ADR-004's known limit and are not counted here.

**Rate limit, under faults: `ChargedOncePerAnswer`.** A request is charged at
most once, plus once for each committed write whose answer it lost.

**Idempotency, safety: `AtMostOnce` and `NoOverlap`.** Within one idempotency
window, from a claim until its Completed record expires, the operation takes
effect at most once and never runs in two places at once. Clients are assumed
well behaved: they run the operation only after `new`, and retry a call that got
no answer. The promise is conditional on the lease: a claim must end (its
complete or fail lands) within `Ttl - MaxSkew`. TLC confirms the boundary
exactly: it passes at `MaxOpTime = Ttl - MaxSkew` and fails at `Ttl`
(`Idempotency_lease.cfg`).

**Idempotency, honesty: `HonestComplete`.** A complete answered "not pending"
was not in fact stored by that same claim.

**Quota, safety: `CounterCoversLedger` and `NoOverAdmit`.** The reference is a
ledger of what each reservation should contribute to each window:
- its estimate in the window it was charged to;
- replaced by its actual usage if it is reconciled while that window is current;
- otherwise, any overage goes into the window current at reconcile;
- nothing once its charge is released.

The counter may hold more than the ledger, which only under-issues, but never
less.

**Quota, efficiency: `NoPhantomRefusal`.** A check is refused only when the
ledger itself has no room.

## Results

TLC 2.19 (tlaplus v1.7.4), 18 October 2026:

| Config | Bounds | Verdict | Distinct states |
| --- | --- | --- | --- |
| `RateLimitOCC.cfg` | 2 tasks × 2 requests, Cap 2, skew 1, 1 clock step, lost answers, 1 OCC and 1 wrapper retry, 5 ticks | pass | 8,762,608 |
| `RateLimitOCC_ttl.cfg` | the same, plus TTL deletion | **NeverOverIssue violated** (finding 1) | |
| `Idempotency.cfg` | 3 requests, Ttl 3, skew 1, claims ending at the lease edge, lost answers, retries | pass | 3,541,758 |
| `Idempotency_noclaim.cfg` | the same, without `claimId` | **NoOverlap violated** (finding 2) | |
| `Idempotency_lease.cfg` | claims outliving the lease | NoOverlap violated (by design) | |
| `Idempotency_honest.cfg` | a retried complete | **HonestComplete violated** (finding 3) | |
| `Quota.cfg` | 2 checks, Limit 2, Win 2, skew 1, reconcile late, contended, repeated, under and over estimate | pass | 597,346 |
| `Quota_release.cfg` | reservation record write fails, no skew | **CounterCoversLedger violated** (finding 4) | |
| `Quota_ttl.cfg` | TTL deletes the counter | **CounterCoversLedger violated** (finding 5) | |

`TRACE=1 make spec` prints every counterexample in full.

## Findings

None of these is fixed yet. Each is stated as TLC's trace, then the code it
corresponds to. The correspondence is by reading: no finding has been
reproduced against the running service yet.

### 1. Version ABA after a TTL delete: a charge is overwritten (rate limit)

Trace (`RateLimitOCC_ttl.cfg`, Cap 2, TtlSec 3):
1. At t=0, p1 reads the empty key and writes `tokens=1, version=1, ttl=3`.
2. Time passes to t=4. p1 starts its next request and reads the item: version
   1, refilled to 2.
3. TTL deletes the item (`ttl 3 < 4`).
4. p2 finds no item, starts a full bucket, and writes `tokens=1, version=1`.
5. p1's PutItem, conditioned on `version = 1`, matches p2's new item and
   overwrites it. p2's charge is gone.
6. With the retries that follow, the key grants 4 tokens at t=4, where the
   bound is `Cap + Rate*MaxSkew = 3`.

In the code, `getOrInitState` starts a missing key at version 0, so every
incarnation of an item counts versions 1, 2, 3 from scratch. A writer that read
an earlier incarnation matches a later one whenever the version numbers line up.
The leaky bucket, sliding window and quota stores start their versions the same
way (`LeakyBucketRateLimitStore`, `DynamoDBSlidingWindowStore`,
`TokenQuotaState.next`), so they share the hazard, though only the token bucket
was checked.

When it can happen: the key must be idle for its whole TTL (3600 s by default),
then DynamoDB must delete it between one request's read and its write, a window
of milliseconds. One lost charge per occurrence. Severity: low in practice. It
is still a real over-issue, which ADR-004 says never happens.

### 2. A retried `/fail` without `claimId` releases someone else's claim (idempotency)

Trace (`Idempotency_noclaim.cfg`, three requests, same client, same key):
1. r1 claims and runs the operation. It fails without effect, and r1 calls
   `/fail`. The write commits, but the answer is lost.
2. r2 checks, claims the Failed record, and starts running.
3. r1 retries `/fail`. The condition (Pending, same client, live) holds on r2's
   claim, so the record becomes Failed.
4. r3 checks and claims. r2 and r3 now run the operation at the same time. Both
   can take effect.

A lost answer and a client retry are ordinary. ADR-006 made `claimId` optional,
so any client that does not send it is exposed. With `claimId`,
`Idempotency.cfg` passes.

### 3. A retried `/complete` is told 409 though its response was stored (idempotency)

Trace (`Idempotency_honest.cfg`):
1. r1 calls `/complete`. The write commits, but the answer is lost.
2. r1 retries and gets `409 conflict, not pending`, for a record its own claim
   completed.

The data is right; the answer is wrong, and a client that believes it may run
the operation again under a new key. With a `claimId` the store could tell its
own completed record from another's.

### 4. Releasing a charge after a window rollover erases another check's usage (quota)

Trace (`Quota_release.cfg`, Limit 2, Win 2, no skew):
1. p1's check takes `nowMs = 0`. It is slow, and its charge lands at t=2 in a
   counter it starts at `window_start = 0`.
2. p2's check at t=2 sees that window lapsed, rolls the counter over to
   `window_start = 2`, and charges 2.
3. p1's reservation record write fails, so p1 releases its charge, a reserve of
   −1 at its own `nowMs = 0`.
4. `TokenQuotaState.inWindow` computes `0 − 2 < Win`, true for a window that
   starts after `nowMs`. The release subtracts 1 from p2's window.
5. The counter holds 1 while the ledger holds 2, so the next check is admitted
   over the limit.

Two code facts combine. `attemptReserve` reuses the check's `nowMs` for every
retry and for the release. And `inWindow` treats a window starting after
`nowMs` as current. The same reuse makes a reserve's retries charge a window
that has really lapsed, but that only under-issues.

When it can happen: a check must straddle its key's window boundary, and the
reservation PutItem must fail three times on a key nothing else writes. Rare.
It is the same class as the reconcile refund that `2e198ad` fixed.

### 5. A counter's TTL computed from a stale or slow clock (quota)

Trace (`Quota_ttl.cfg`): a check that took `nowMs = 0` writes at t=4, with
`ttl = nowMs + Win + Grace = 3`. The counter is expired as it is written, TTL
deletes it, and the next check starts the same window again from zero.

In the code the grace is 60 s, so this needs a check slower than its window plus
a minute, or a writer whose clock is more than 60 s behind the task that started
the window. Severity: low. Recorded because nothing enforces those bounds.

## What this does and does not show

- **Shown.** Within the bounds above, every interleaving of the modelled steps
  keeps the passing properties, and each counterexample is a concrete schedule.
- **Not shown.**
  - Behaviour beyond the bounds: more tasks, longer histories, larger windows.
  - That the models match the code. That is my reading, and the history checker
    (next step) is what ties the properties to the running service.
  - The leaky bucket and sliding window, which are not modelled.
  - Several quota levels in one transaction (one counter is modelled).
  - The bulkhead, circuit breaker and degradation paths, which only refuse.
- **Evidence level.** Model-checker verdicts on hand-written models. A model
  that is wrong in the same way as the code passes for the same wrong reason.
  Agreement with real DynamoDB and with recorded histories is still to come.

## Cost

- Upkeep: a change to any file in the table at the top should change its spec
  in the same commit, or the spec checks a protocol the code no longer runs.
- `make spec` takes about 6 minutes locally. The rate-limit pass alone is
  3 minutes of it, so it is not yet in CI.
- No AWS spend.
