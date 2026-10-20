------------------------------- MODULE Quota --------------------------------
(***************************************************************************)
(* One fixed-window quota counter, as core.TokenQuotaService and           *)
(* storage.DynamoDBTokenQuotaStore run it (reconcile trust, PR #25):        *)
(*                                                                          *)
(*  check:  read the counter (consistent), plan with                        *)
(*          TokenQuotaState.next at the nowMs taken once when the check     *)
(*          began, refuse above the limit, else PutItem conditioned on the  *)
(*          version read (attribute_not_exists for none). A lost condition  *)
(*          re-reads, with the same nowMs, up to MaxOcc times. Then the     *)
(*          reservation record gets its own conditional PutItem; if that    *)
(*          fails, the charge is released (a reserve of -estimate with no   *)
(*          limit, again at the check's nowMs) and the check is refused.    *)
(*  reconcile: read the record (NotFound once expired) and the counter,     *)
(*          plan with QuotaReconciliation.plan at a fresh nowMs, and write  *)
(*          both in one transaction conditioned on the counter's version    *)
(*          and on the record still being pending.                          *)
(*                                                                          *)
(* One level (the user counter) is modelled: every level is the same       *)
(* counter protocol, and a TransactWriteItems over several is atomic.      *)
(*                                                                          *)
(* The reference is a ledger of what each reservation should contribute to *)
(* each window: its estimate in the window it was charged to, replaced by  *)
(* its actual usage if it is reconciled while that window is current, plus *)
(* any overage in the window current at reconcile otherwise; nothing once  *)
(* released. The counter may hold more than the ledger (a release that      *)
(* failed only under-issues), never less.                                  *)
(***************************************************************************)
EXTENDS Integers, TLC

CONSTANTS
  Procs, Limit, Win, Ests, Actuals,
  MaxTime, MaxSkew, MaxOcc, ResTtl, RecTries,
  AllowRecordFail,  \* may the reservation PutItem fail after the charge?
  AllowCounterTtl,  \* may DynamoDB TTL delete the counter?
  Grace             \* counter ttl = nowMs + Win + Grace (60 s in the code)

Max(a, b) == IF a > b THEN a ELSE b

NoCtr == [exists |-> FALSE, used |-> 0, ws |-> 0, version |-> 0, ttl |-> 0]
NoPart == [ws |-> -1, amt |-> 0]

VARIABLES
  now, skew, ctr, res, pc, t0, est, act, snap, occ, tries,
  contrib, extra,   \* the ledger, per reservation
  overBad,          \* a check admitted past the limit of the ledger
  phantom           \* a check refused though the ledger had room

vars == <<now, skew, ctr, res, pc, t0, est, act, snap, occ, tries, contrib,
          extra, overBad, phantom>>

Wall(p) == now + skew[p]

RECURSIVE SumParts(_, _)
SumParts(S, w) ==
  IF S = {} THEN 0
  ELSE LET p == CHOOSE x \in S : TRUE
       IN (IF contrib[p].ws = w THEN contrib[p].amt ELSE 0)
          + (IF extra[p].ws = w THEN extra[p].amt ELSE 0)
          + SumParts(S \ {p}, w)

Ledger(w) == SumParts(Procs, w)

\* TokenQuotaState.next, with clamp.
InWin(c, t) == c.exists /\ t - c.ws < Win
Next(c, delta, t) ==
  [exists |-> TRUE,
   used |-> Max(0, (IF InWin(c, t) THEN c.used ELSE 0) + delta),
   ws |-> IF InWin(c, t) THEN c.ws ELSE t,
   version |-> IF c.exists THEN c.version + 1 ELSE 1,
   ttl |-> t + Win + Grace]

CondHolds(p) ==
  IF snap[p].expect = 0 THEN ~ctr.exists
  ELSE ctr.exists /\ ctr.version = snap[p].expect

Init ==
  /\ now = 0
  /\ skew \in [Procs -> 0..MaxSkew]
  /\ ctr = NoCtr
  /\ res = [p \in Procs |-> [state |-> "none", est |-> 0, ws |-> 0, exp |-> 0]]
  /\ pc = [p \in Procs |-> "idle"]
  /\ t0 = [p \in Procs |-> 0]
  /\ est = [p \in Procs |-> 0]
  /\ act = [p \in Procs |-> 0]
  /\ snap = [p \in Procs |-> [expect |-> 0, after |-> NoCtr]]
  /\ occ = [p \in Procs |-> 0]
  /\ tries = [p \in Procs |-> 0]
  /\ contrib = [p \in Procs |-> NoPart]
  /\ extra = [p \in Procs |-> NoPart]
  /\ overBad = FALSE
  /\ phantom = FALSE

Set(f, p, v) == [f EXCEPT ![p] = v]

Start(p) ==
  /\ pc[p] = "idle"
  /\ \E e \in Ests : est' = Set(est, p, e)
  /\ t0' = Set(t0, p, Wall(p))
  /\ occ' = Set(occ, p, 0)
  /\ pc' = Set(pc, p, "read")
  /\ UNCHANGED <<now, skew, ctr, res, act, snap, tries, contrib, extra, overBad, phantom>>

\* readCurrent and QuotaReservation.plan.
Read(p) ==
  /\ pc[p] = "read"
  /\ LET after == Next(ctr, est[p], t0[p])
     IN IF after.used > Limit
        THEN \* Exceeded
             /\ phantom' = (phantom \/ Ledger(after.ws) + est[p] <= Limit)
             /\ pc' = Set(pc, p, "done")
             /\ UNCHANGED snap
        ELSE /\ snap' = Set(snap, p, [expect |-> IF ctr.exists THEN ctr.version ELSE 0,
                                      after |-> after])
             /\ pc' = Set(pc, p, "write")
             /\ UNCHANGED phantom
  /\ UNCHANGED <<now, skew, ctr, res, t0, est, act, occ, tries, contrib, extra, overBad>>

Write(p) ==
  /\ pc[p] = "write"
  /\ IF CondHolds(p)
     THEN /\ ctr' = snap[p].after
          /\ overBad' = (overBad \/ Ledger(snap[p].after.ws) + est[p] > Limit)
          /\ contrib' = Set(contrib, p, [ws |-> snap[p].after.ws, amt |-> est[p]])
          /\ pc' = Set(pc, p, "record")
          /\ UNCHANGED occ
     ELSE /\ IF occ[p] < MaxOcc
             THEN /\ occ' = Set(occ, p, occ[p] + 1) /\ pc' = Set(pc, p, "read")
             ELSE /\ pc' = Set(pc, p, "done") /\ UNCHANGED occ   \* Contended
          /\ UNCHANGED <<ctr, overBad, contrib>>
  /\ UNCHANGED <<now, skew, res, t0, est, act, snap, tries, extra, phantom>>

\* The reservation record's PutItem: written, or failed after the charge.
Record(p) ==
  /\ pc[p] = "record"
  /\ \/ /\ res' = Set(res, p, [state |-> "pending", est |-> est[p],
                               ws |-> snap[p].after.ws, exp |-> t0[p] + ResTtl])
        /\ pc' = Set(pc, p, "admitted")
        /\ UNCHANGED occ
     \/ /\ AllowRecordFail
        /\ pc' = Set(pc, p, "relread")
        /\ occ' = Set(occ, p, 0)
        /\ UNCHANGED res
  /\ UNCHANGED <<now, skew, ctr, t0, est, act, snap, tries, contrib, extra, overBad, phantom>>

\* release: attemptReserve of -estimate, no limit, at the check's nowMs.
RelRead(p) ==
  /\ pc[p] = "relread"
  /\ snap' = Set(snap, p, [expect |-> IF ctr.exists THEN ctr.version ELSE 0,
                           after |-> Next(ctr, -est[p], t0[p])])
  /\ pc' = Set(pc, p, "relwrite")
  /\ UNCHANGED <<now, skew, ctr, res, t0, est, act, occ, tries, contrib, extra, overBad, phantom>>

RelWrite(p) ==
  /\ pc[p] = "relwrite"
  /\ IF CondHolds(p)
     THEN /\ ctr' = snap[p].after
          /\ contrib' = Set(contrib, p, NoPart)   \* the charge is given back
          /\ pc' = Set(pc, p, "done")
          /\ UNCHANGED occ
     ELSE /\ IF occ[p] < MaxOcc
             THEN /\ occ' = Set(occ, p, occ[p] + 1) /\ pc' = Set(pc, p, "relread")
             ELSE /\ pc' = Set(pc, p, "done") /\ UNCHANGED occ  \* stays charged
          /\ UNCHANGED <<ctr, contrib>>
  /\ UNCHANGED <<now, skew, res, t0, est, act, snap, tries, extra, overBad, phantom>>

\* reconcile: the record, then the counter, read at a fresh nowMs.
RecRead(p) ==
  /\ pc[p] = "admitted"
  /\ tries[p] < RecTries
  /\ tries' = Set(tries, p, tries[p] + 1)
  /\ \E a \in Actuals : act' = Set(act, p, a)
  /\ LET tr == Wall(p) IN
     IF res[p].state # "pending" \/ tr >= res[p].exp
     THEN /\ pc' = Set(pc, p, "done")       \* NotFound or AlreadyReconciled
          /\ UNCHANGED <<snap, t0>>
     ELSE /\ t0' = Set(t0, p, tr)
          /\ pc' = Set(pc, p, "recwrite")
          /\ snap' = Set(snap, p, [expect |-> IF ctr.exists THEN ctr.version ELSE 0,
                                   after |-> ctr])
  /\ UNCHANGED <<now, skew, ctr, res, est, occ, contrib, extra, overBad, phantom>>

\* QuotaReconciliation.plan on the counter read, then the transaction.
RecWrite(p) ==
  /\ pc[p] = "recwrite"
  /\ LET c == snap[p].after      \* the counter as read
         tr == t0[p]
         charged == c.exists /\ c.ws = res[p].ws /\ tr - c.ws < Win
         d == act[p] - est[p]
         dd == IF charged THEN d ELSE Max(0, d)
         after == Next(c, dd, tr)
     IN IF CondHolds(p) /\ res[p].state = "pending"
        THEN /\ ctr' = IF dd # 0 THEN after ELSE ctr
             /\ res' = Set(res, p, [res[p] EXCEPT !.state = "reconciled"])
             /\ IF charged
                THEN /\ contrib' = Set(contrib, p, [contrib[p] EXCEPT !.amt = act[p]])
                     /\ UNCHANGED extra
                ELSE /\ extra' = IF dd > 0 THEN Set(extra, p, [ws |-> after.ws, amt |-> dd])
                                 ELSE extra
                     /\ UNCHANGED contrib
             /\ pc' = Set(pc, p, "done")
        ELSE \* a counter moved: retry the whole reconcile
             /\ pc' = Set(pc, p, "admitted")
             /\ UNCHANGED <<ctr, res, contrib, extra>>
  /\ UNCHANGED <<now, skew, t0, est, act, snap, occ, tries, overBad, phantom>>

Tick ==
  /\ now < MaxTime
  /\ now' = now + 1
  /\ UNCHANGED <<skew, ctr, res, pc, t0, est, act, snap, occ, tries, contrib,
                 extra, overBad, phantom>>

CounterTtl ==
  /\ AllowCounterTtl
  /\ ctr.exists /\ ctr.ttl < now
  /\ ctr' = NoCtr
  /\ UNCHANGED <<now, skew, res, pc, t0, est, act, snap, occ, tries, contrib,
                 extra, overBad, phantom>>

NextStep ==
  \/ Tick \/ CounterTtl
  \/ \E p \in Procs :
       \/ Start(p) \/ Read(p) \/ Write(p) \/ Record(p) \/ RelRead(p)
       \/ RelWrite(p) \/ RecRead(p) \/ RecWrite(p)

Spec == Init /\ [][NextStep]_vars

-----------------------------------------------------------------------------
(* Safety: the counter never holds less than the ledger of its window, so  *)
(* no check is admitted past the limit.                                     *)
CounterCoversLedger == ctr.exists => ctr.used >= Ledger(ctr.ws)
NoOverAdmit == ~overBad

(* Efficiency: a check is refused only when the ledger itself is full.     *)
NoPhantomRefusal == ~phantom
=============================================================================
