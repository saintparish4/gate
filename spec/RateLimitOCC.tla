---------------------------- MODULE RateLimitOCC ----------------------------
(***************************************************************************)
(* The token bucket on one DynamoDB item, as storage.DynamoDBRateLimitStore *)
(* and resilience.ResilientWrapper run it:                                  *)
(*                                                                          *)
(*  - a consistent GetItem, then core.TokenBucket.refill and consume on the *)
(*    task's own wall clock (elapsed clamped at zero, the mark never moved  *)
(*    backward), and a PutItem conditioned on the version read, or on       *)
(*    attribute_not_exists(pk) for a key never written;                     *)
(*  - a lost condition re-reads, up to MaxOcc times, then the check is      *)
(*    refused (ADR-004);                                                    *)
(*  - an attempt that times out is retried by the wrapper, up to            *)
(*    MaxWrapRetries times. A timed-out PutItem may still have committed:   *)
(*    DynamoDB applies a write whose response the caller never sees.        *)
(*  - DynamoDB TTL deletes the item at any time after its ttl attribute     *)
(*    (lazily, so "any time after" is the honest model).                    *)
(*                                                                          *)
(* Time is whole ticks of one global clock `now`, which is also DynamoDB's  *)
(* clock for TTL. Each task reads now + skew[p], with skew in 0..MaxSkew,   *)
(* and the skew can step at most MaxSteps times (NTP steps, VM resume).     *)
(*                                                                          *)
(* The reference is RFC 2697's token bucket in its arrival-curve form: a   *)
(* sequence of charges conforms to a bucket of (Cap, Rate) that starts full *)
(* exactly when every interval [t1, t2] holds at most Cap + Rate*(t2 - t1)  *)
(* of them. Each committed charge is stamped with the real time of the read *)
(* that decided it, because that is the instant the code computed it at: a  *)
(* request may be linearized anywhere between its invocation and its        *)
(* answer. (Stamping it with the commit instead reports a read-to-commit    *)
(* delay as minting, which it is not; that was this spec's first            *)
(* counterexample, and the reference's fault.) Rate * MaxSkew is the        *)
(* allowance for the skew the code admits to minting (core.TokenBucket,     *)
(* "Clock corrections").                                                    *)
(***************************************************************************)
EXTENDS Integers, TLC

CONSTANTS
  Procs,          \* tasks sharing the key
  Cap,            \* profile.capacity
  Rate,           \* profile.refillRatePerSecond, tokens per tick
  Cost,           \* tokens per request
  TtlSec,         \* profile.ttlSeconds
  MaxTime,        \* last tick explored
  MaxSkew,        \* largest clock offset between tasks
  MaxSteps,       \* clock steps allowed in one behaviour
  MaxOcc,         \* RetryPolicy.occRetry retries (10 in the code)
  MaxWrapRetries, \* config.retry.dynamodb.maxRetries
  Requests,       \* requests per task
  AllowAmbiguous, \* may a committed PutItem time out?
  AllowTtlDelete  \* may DynamoDB TTL delete the item?

Min(a, b) == IF a < b THEN a ELSE b
Max(a, b) == IF a > b THEN a ELSE b

NoItem == [exists |-> FALSE, tokens |-> 0, last |-> 0, version |-> 0, ttl |-> 0]

VARIABLES
  now, skew, steps,
  item,        \* the DynamoDB item
  pc,          \* "idle" | "read" | "write" | "done"
  snap,        \* what a task read and wants to write
  occ, wrap,   \* retries used by the current request
  served,      \* requests answered per task
  grants,      \* committed charges, by the real time of the read that decided them
  charges,     \* committed charges for the current request
  lostAcks,    \* committed writes whose answer the task never saw
  effBad       \* a refusal the reference bucket would not have made

vars == <<now, skew, steps, item, pc, snap, occ, wrap, served, grants,
          charges, lostAcks, effBad>>

Wall(p) == now + skew[p]

Times == 0..MaxTime

RECURSIVE SumRange(_, _)
SumRange(a, b) == IF a > b THEN 0 ELSE grants[a] + SumRange(a + 1, b)

\* Could the reference bucket pay `c` more at time t, short by `slack`?
RefAdmits(t, c, slack) ==
  \A t1 \in 0..t : SumRange(t1, t) + c + slack <= Cap + Rate * (t - t1)

Init ==
  /\ now = 0
  /\ skew \in [Procs -> 0..MaxSkew]
  /\ steps = 0
  /\ item = NoItem
  /\ pc = [p \in Procs |-> "idle"]
  /\ snap = [p \in Procs |-> [expect |-> 0, at |-> 0, new |-> NoItem]]
  /\ occ = [p \in Procs |-> 0]
  /\ wrap = [p \in Procs |-> 0]
  /\ served = [p \in Procs |-> 0]
  /\ grants = [t \in Times |-> 0]
  /\ charges = [p \in Procs |-> 0]
  /\ lostAcks = [p \in Procs |-> 0]
  /\ effBad = FALSE

Answer(p) ==
  /\ served' = [served EXCEPT ![p] = @ + 1]
  /\ pc' = [pc EXCEPT ![p] = IF served[p] + 1 < Requests THEN "idle" ELSE "done"]

\* A new request: fresh retry budgets.
Start(p) ==
  /\ pc[p] = "idle"
  /\ occ' = [occ EXCEPT ![p] = 0]
  /\ wrap' = [wrap EXCEPT ![p] = 0]
  /\ charges' = [charges EXCEPT ![p] = 0]
  /\ lostAcks' = [lostAcks EXCEPT ![p] = 0]
  /\ pc' = [pc EXCEPT ![p] = "read"]
  /\ UNCHANGED <<now, skew, steps, item, snap, served, grants, effBad>>

\* getOrInitState, refill and consume, in one step: they run on one read.
Read(p) ==
  /\ pc[p] = "read"
  /\ LET cur == IF item.exists THEN item
                ELSE [exists |-> FALSE, tokens |-> Cap, last |-> Wall(p),
                      version |-> 0, ttl |-> 0]
         refilled == Min(Cap, cur.tokens + Rate * Max(0, Wall(p) - cur.last))
     IN IF refilled >= Cost
        THEN /\ snap' = [snap EXCEPT ![p] =
                   [expect |-> cur.version, at |-> now,
                    new |-> [exists |-> TRUE, tokens |-> refilled - Cost,
                             last |-> Max(Wall(p), cur.last),
                             version |-> cur.version + 1,
                             ttl |-> Wall(p) + TtlSec]]]
             /\ pc' = [pc EXCEPT ![p] = "write"]
             /\ UNCHANGED <<served, effBad>>
        ELSE \* Rejected for lack of tokens.
             /\ effBad' = (effBad \/ RefAdmits(now, Cost, Rate * MaxSkew))
             /\ Answer(p)
             /\ UNCHANGED snap
  /\ UNCHANGED <<now, skew, steps, item, occ, wrap, grants, charges, lostAcks>>

CondHolds(p) ==
  IF snap[p].expect = 0 THEN ~item.exists
  ELSE item.exists /\ item.version = snap[p].expect

Commit(p) ==
  /\ item' = snap[p].new
  /\ grants' = [grants EXCEPT ![snap[p].at] = @ + Cost]
  /\ charges' = [charges EXCEPT ![p] = @ + 1]

\* The PutItem commits and its answer arrives: Allowed.
WriteOk(p) ==
  /\ pc[p] = "write" /\ CondHolds(p)
  /\ Commit(p)
  /\ Answer(p)
  /\ UNCHANGED <<now, skew, steps, snap, occ, wrap, lostAcks, effBad>>

\* The PutItem commits but the attempt times out. The wrapper retries the
\* whole check, or degrades (reject-all) when its retries are spent.
WriteLost(p) ==
  /\ AllowAmbiguous
  /\ pc[p] = "write" /\ CondHolds(p)
  /\ Commit(p)
  /\ lostAcks' = [lostAcks EXCEPT ![p] = @ + 1]
  /\ IF wrap[p] < MaxWrapRetries
     THEN /\ wrap' = [wrap EXCEPT ![p] = @ + 1]
          /\ occ' = [occ EXCEPT ![p] = 0]
          /\ pc' = [pc EXCEPT ![p] = "read"]
          /\ UNCHANGED served
     ELSE /\ Answer(p) /\ UNCHANGED <<wrap, occ>>
  /\ UNCHANGED <<now, skew, steps, snap, effBad>>

\* ConditionalCheckFailed: re-read, or refuse once the retries are spent.
WriteConflict(p) ==
  /\ pc[p] = "write" /\ ~CondHolds(p)
  /\ IF occ[p] < MaxOcc
     THEN /\ occ' = [occ EXCEPT ![p] = @ + 1]
          /\ pc' = [pc EXCEPT ![p] = "read"]
          /\ UNCHANGED served
     ELSE /\ Answer(p) /\ UNCHANGED occ
  /\ UNCHANGED <<now, skew, steps, item, snap, wrap, grants,
                 charges, lostAcks, effBad>>

Tick ==
  /\ now < MaxTime
  /\ now' = now + 1
  /\ UNCHANGED <<skew, steps, item, pc, snap, occ, wrap, served, grants,
                 charges, lostAcks, effBad>>

ClockStep(p) ==
  /\ steps < MaxSteps
  /\ \E s \in 0..MaxSkew : s # skew[p] /\ skew' = [skew EXCEPT ![p] = s]
  /\ steps' = steps + 1
  /\ UNCHANGED <<now, item, pc, snap, occ, wrap, served, grants,
                 charges, lostAcks, effBad>>

TtlDelete ==
  /\ AllowTtlDelete
  /\ item.exists /\ item.ttl < now
  /\ item' = NoItem
  /\ UNCHANGED <<now, skew, steps, pc, snap, occ, wrap, served, grants,
                 charges, lostAcks, effBad>>

Next ==
  \/ Tick \/ TtlDelete
  \/ \E p \in Procs : Start(p) \/ Read(p) \/ WriteOk(p) \/ WriteLost(p)
                      \/ WriteConflict(p) \/ ClockStep(p)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Safety: the committed charges conform to an RFC 2697 bucket of (Cap,    *)
(* Rate), over by at most the tokens a clock offset can mint.              *)
NeverOverIssue ==
  \A t1, t2 \in Times : t1 <= t2 =>
     SumRange(t1, t2) <= Cap + Rate * (t2 - t1) + Rate * MaxSkew

(* Efficiency (under-issue): a refusal for lack of tokens happens only when *)
(* the reference bucket, at the same instant and short by the skew         *)
(* allowance, could not pay Cost either.                                   *)
NoNeedlessRefusal == ~effBad

(* Efficiency under faults: a request is charged once, plus once for each  *)
(* committed write whose answer it lost.                                    *)
ChargedOncePerAnswer == \A p \in Procs : charges[p] <= 1 + lostAcks[p]

TypeOK ==
  /\ item.tokens \in 0..Cap
  /\ \A p \in Procs : pc[p] \in {"idle", "read", "write", "done"}
=============================================================================
