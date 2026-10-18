---------------------------- MODULE Idempotency -----------------------------
(***************************************************************************)
(* One idempotency key of one client, as storage.DynamoDBIdempotencyStore   *)
(* and api.IdempotencyApi run it. Every request belongs to the same client, *)
(* which is the hard case: the client check cannot tell them apart, only    *)
(* the claim token can (ADR-006).                                           *)
(*                                                                          *)
(*  - check: PutItem Pending with a fresh claimId, conditioned on           *)
(*    attribute_not_exists(pk) OR status = Failed OR ttl < now. If the      *)
(*    condition fails, a separate consistent GetItem decides: Pending ->    *)
(*    in_progress, Completed -> duplicate, Failed or absent/expired ->      *)
(*    claim again (at most MaxReclaims times, 3 in the code).               *)
(*  - complete / fail: UpdateItem conditioned on status = Pending, the same *)
(*    client, ttl >= now, and claimId = :claimId when the caller sends one. *)
(*                                                                          *)
(* Clients are well behaved: a request runs the operation only after `new`, *)
(* then calls complete (it took effect) or fail (it had no effect), and     *)
(* the call lands within MaxOpTime ticks of the claim: that is the lease. A request that gets no answer,       *)
(* because its call timed out, retries that same call, up to ClientRetries  *)
(* times in all; a timed-out write may have committed. A request answered   *)
(* in_progress tries its check again later.                                  *)
(*                                                                          *)
(* Time is whole ticks of a global clock, also DynamoDB's. Each task reads  *)
(* now + skew[r], skew in 0..MaxSkew.                                       *)
(***************************************************************************)
EXTENDS Integers, TLC

CONSTANTS
  Reqs,           \* concurrent requests with the same key and client
  Ttl,            \* record TTL in ticks
  MaxTime,
  MaxSkew,
  MaxOpTime,      \* longest from a claim to its complete or fail, in ticks
  MaxReclaims,    \* checkWithRetry's retries
  ClientRetries,  \* retries of a call that got no answer
  UseClaimId,     \* do complete and fail send the claimId?
  AllowAmbiguous  \* may a committed write time out?

NoRec == [exists |-> FALSE, status |-> "none", ttl |-> 0, claim |-> 0]

VARIABLES
  now, skew,
  rec,         \* the DynamoDB item
  nextClaim,   \* claimIds are fresh
  pc,          \* see Next
  mine,        \* claimId a request was answered `new` with (0 if none)
  started,     \* tick its operation started
  reclaims, retries,
  effects,     \* operations that took effect in this window
  running,     \* requests running the operation now
  dishonest    \* a complete answered 409 though its own claim was completed

vars == <<now, skew, rec, nextClaim, pc, mine, started, reclaims, retries,
          effects, running, dishonest>>

Wall(r) == now + skew[r]
Live(r) == rec.exists /\ ~(rec.ttl < Wall(r))

Init ==
  /\ now = 0
  /\ skew \in [Reqs -> 0..MaxSkew]
  /\ rec = NoRec
  /\ nextClaim = 1
  /\ pc = [r \in Reqs |-> "check"]
  /\ mine = [r \in Reqs |-> 0]
  /\ started = [r \in Reqs |-> 0]
  /\ reclaims = [r \in Reqs |-> 0]
  /\ retries = [r \in Reqs |-> 0]
  /\ effects = 0
  /\ running = {}
  /\ dishonest = FALSE

ClaimHolds(r) ==
  ~rec.exists \/ rec.status = "Failed" \/ rec.ttl < Wall(r)

\* A timed-out call: retry it, or give up.
Retry(r, callPc) ==
  IF retries[r] < ClientRetries
  THEN /\ retries' = [retries EXCEPT ![r] = @ + 1]
       /\ pc' = [pc EXCEPT ![r] = callPc]
  ELSE /\ pc' = [pc EXCEPT ![r] = "gaveup"]
       /\ UNCHANGED retries

\* tryCreatePending wins; the answer `new` arrives, or is lost.
ClaimWins(r) ==
  /\ pc[r] = "check" /\ ClaimHolds(r)
  /\ rec' = [exists |-> TRUE, status |-> "Pending", ttl |-> Wall(r) + Ttl,
             claim |-> nextClaim]
  /\ nextClaim' = nextClaim + 1
  /\ \/ /\ mine' = [mine EXCEPT ![r] = nextClaim]
        /\ started' = [started EXCEPT ![r] = now]
        /\ running' = running \cup {r}
        /\ pc' = [pc EXCEPT ![r] = "run"]
        /\ UNCHANGED retries
     \/ /\ AllowAmbiguous
        /\ Retry(r, "check")
        /\ UNCHANGED <<mine, started, running>>
  \* Claiming over an expired Completed record opens a new window: the
  \* store promised duplicates only for Ttl, and that promise is kept.
  /\ effects' = IF rec.exists /\ rec.status = "Completed" THEN 0 ELSE effects
  /\ UNCHANGED <<now, skew, reclaims, dishonest>>

ClaimLoses(r) ==
  /\ pc[r] = "check" /\ ~ClaimHolds(r)
  /\ pc' = [pc EXCEPT ![r] = "get"]
  /\ UNCHANGED <<now, skew, rec, nextClaim, mine, started, reclaims, retries,
                 effects, running, dishonest>>

\* The GetItem after a lost claim.
Get(r) ==
  /\ pc[r] = "get"
  /\ IF Live(r) /\ rec.status = "Pending"
     THEN pc' = [pc EXCEPT ![r] = "check"]      \* in_progress; check later
     ELSE IF Live(r) /\ rec.status = "Completed"
     THEN pc' = [pc EXCEPT ![r] = "done"]       \* duplicate: replay
     ELSE IF reclaims[r] < MaxReclaims           \* Failed, absent, or expired
     THEN pc' = [pc EXCEPT ![r] = "check"]
     ELSE pc' = [pc EXCEPT ![r] = "gaveup"]     \* 503
  /\ reclaims' = [reclaims EXCEPT ![r] =
                    IF Live(r) /\ rec.status \in {"Pending", "Completed"}
                    THEN @ ELSE @ + 1]
  /\ UNCHANGED <<now, skew, rec, nextClaim, mine, started, retries, effects,
                 running, dishonest>>

\* The operation ends, with effect or without.
Finish(r) ==
  /\ pc[r] = "run"
  /\ running' = running \ {r}
  /\ \/ /\ effects' = effects + 1
        /\ pc' = [pc EXCEPT ![r] = "complete"]
     \/ /\ pc' = [pc EXCEPT ![r] = "fail"]
        /\ UNCHANGED effects
  /\ UNCHANGED <<now, skew, rec, nextClaim, mine, started, reclaims, retries,
                 dishonest>>

EndHolds(r) ==
  /\ Live(r) /\ rec.status = "Pending"
  /\ (UseClaimId => rec.claim = mine[r])

\* complete or fail: `to` is the status written.
End(r, callPc, to) ==
  /\ pc[r] = callPc
  /\ IF EndHolds(r)
     THEN /\ rec' = [rec EXCEPT !.status = to]
          /\ \/ /\ pc' = [pc EXCEPT ![r] = "done"] /\ UNCHANGED retries
             \/ /\ AllowAmbiguous /\ Retry(r, callPc)
          /\ UNCHANGED dishonest
     ELSE \* 409 not pending
          /\ UNCHANGED rec
          /\ pc' = [pc EXCEPT ![r] = "done"]
          /\ UNCHANGED retries
          /\ dishonest' = (dishonest \/
                (callPc = "complete" /\ rec.exists /\ rec.status = "Completed"
                 /\ rec.claim = mine[r]))
  /\ UNCHANGED <<now, skew, nextClaim, mine, started, reclaims, effects,
                 running>>

Tick ==
  /\ now < MaxTime
  \* Claims end within MaxOpTime: time cannot pass a claim still open.
  /\ \A r \in Reqs : pc[r] \in {"run", "complete", "fail"} =>
        now + 1 - started[r] <= MaxOpTime
  /\ now' = now + 1
  /\ UNCHANGED <<skew, rec, nextClaim, pc, mine, started, reclaims, retries,
                 effects, running, dishonest>>

Next ==
  \/ Tick
  \/ \E r \in Reqs :
       \/ ClaimWins(r) \/ ClaimLoses(r) \/ Get(r) \/ Finish(r)
       \/ End(r, "complete", "Completed") \/ End(r, "fail", "Failed")

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Safety: within one idempotency window (from a claim until its Completed *)
(* record expires), the operation takes effect at most once, and it never  *)
(* runs in two places at once. Neither is promised past the lease: one     *)
(* that outlives Ttl - MaxSkew can be claimed again (the cfg files keep    *)
(* MaxOpTime under it).                                                     *)
AtMostOnce == effects <= 1
NoOverlap == \A r1, r2 \in running : r1 = r2

(* Honesty: a complete answered "not pending" was not, in fact, stored.    *)
HonestComplete == ~dishonest
=============================================================================
