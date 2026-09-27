package core

import org.typelevel.log4cats.Logger

import cats.effect.*
import cats.syntax.all.*
import observability.MetricsPublisher
import config.TokenQuotaConfig

sealed trait QuotaLevel:
  def prefix: String

object QuotaLevel:
  case object User extends QuotaLevel:
    val prefix = "user"
  case object Agent extends QuotaLevel:
    val prefix = "agent"
  case object Org extends QuotaLevel:
    val prefix = "org"

  val all: List[QuotaLevel] = List(User, Agent, Org)

case class QuotaIdentifier(
    userId: String,
    agentId: Option[String] = None,
    orgId: Option[String] = None,
)

sealed trait QuotaDecision
object QuotaDecision:
  /** Admitted. `reservationId` names what was reserved; reconcile takes it. */
  case class Available(
      remainingByLevel: Map[QuotaLevel, Long],
      reservationId: String,
  ) extends QuotaDecision

  case class Exceeded(
      level: QuotaLevel,
      limit: Long,
      used: Long,
      retryAfterSeconds: Int,
  ) extends QuotaDecision

  /** The store lost every conditional write in its retry budget. Nothing was
    * reserved, so the caller should fail closed and retry shortly.
    */
  case class Contended(attempts: Int) extends QuotaDecision

sealed trait ReconcileResult
object ReconcileResult:
  /** Applied, or already applied with the same actual usage. The deltas are
    * actual minus the stored estimate.
    */
  case class Reconciled(inputDelta: Long, outputDelta: Long)
      extends ReconcileResult

  /** The adjustment was not recorded; the caller should retry it. */
  case class Contended(attempts: Int) extends ReconcileResult

  /** No such reservation for this client: unknown, expired, or another
    * client's. The three are indistinguishable on purpose.
    */
  case object NotFound extends ReconcileResult

  /** Already reconciled with different actual usage. A reservation reconciles
    * once; a retry with the same usage gets `Reconciled` again.
    */
  case class Conflict(recorded: ReconciledUsage) extends ReconcileResult

case class TokenQuotaState(
    inputTokens: Long,
    outputTokens: Long,
    windowStart: Long,
    version: Long,
):
  def totalTokens: Long = inputTokens + outputTokens

object TokenQuotaState:
  def inWindow(s: TokenQuotaState, windowSeconds: Long, nowMs: Long): Boolean =
    nowMs - s.windowStart < windowSeconds * 1000

  /** Usage that still counts at `nowMs`; a lapsed window contributes nothing.
    */
  def usedWithin(
      current: Option[TokenQuotaState],
      windowSeconds: Long,
      nowMs: Long,
  ): Long = current.filter(inWindow(_, windowSeconds, nowMs)).map(_.totalTokens)
    .getOrElse(0L)

  /** State after applying the deltas at `nowMs`. A lapsed window starts over
    * holding only this delta. I keep the version monotonic across rollovers so
    * a stale writer can never match a freshly reset counter.
    */
  def next(
      current: Option[TokenQuotaState],
      inputDelta: Long,
      outputDelta: Long,
      windowSeconds: Long,
      nowMs: Long,
  ): TokenQuotaState =
    val version = current.map(_.version + 1).getOrElse(1L)
    current.filter(inWindow(_, windowSeconds, nowMs)) match
      case Some(s) => TokenQuotaState(
          clamp(s.inputTokens + inputDelta),
          clamp(s.outputTokens + outputDelta),
          s.windowStart,
          version,
        )
      case None =>
        TokenQuotaState(clamp(inputDelta), clamp(outputDelta), nowMs, version)

  private def clamp(n: Long): Long = math.max(0L, n)

/** One counter a reservation applies to. `limit = None` records the delta
  * unconditionally, which is what reconciliation needs.
  */
case class QuotaTarget(pk: String, windowSeconds: Long, limit: Option[Long])

/** A counter a reservation was charged to, and the window it was charged in.
  */
case class ChargedTarget(pk: String, windowSeconds: Long, windowStart: Long)

/** Actual usage a reservation was reconciled with. */
case class ReconciledUsage(actualInput: Long, actualOutput: Long)

/** What a quota check reserved, kept server-side so reconcile never trusts the
  * caller's estimate. Reconcile used to take the estimate from the request, so
  * a call with actual = 0 and a huge estimate zeroed the user, agent, and org
  * counters. It now applies `actual - estimatedInput/Output` from this record,
  * once.
  */
case class ReservationRecord(
    pk: String,
    targets: List[ChargedTarget],
    estimatedInput: Long,
    estimatedOutput: Long,
    expiresAtSeconds: Long,
    reconciled: Option[ReconciledUsage],
):
  def expired(nowMs: Long): Boolean = nowMs >= expiresAtSeconds * 1000

/** Asks `reserve` to record a reservation at `pk` with the counters it writes.
  */
case class NewReservation(pk: String, expiresAtSeconds: Long):
  def record(
      planned: List[PlannedWrite],
      estimatedInput: Long,
      estimatedOutput: Long,
  ): ReservationRecord = ReservationRecord(
    pk,
    planned.map(p =>
      ChargedTarget(p.target.pk, p.target.windowSeconds, p.after.windowStart),
    ),
    estimatedInput,
    estimatedOutput,
    expiresAtSeconds,
    reconciled = None,
  )

sealed trait ReserveOutcome
object ReserveOutcome:
  /** Every target was written; states are keyed by target pk. */
  case class Reserved(states: Map[String, TokenQuotaState])
      extends ReserveOutcome

  /** The delta would push `pk` past its limit; nothing was written. */
  case class LimitExceeded(pk: String, used: Long, windowStart: Long)
      extends ReserveOutcome

  /** Conditional writes kept losing; nothing was written. */
  case class Contended(attempts: Int) extends ReserveOutcome

sealed trait ReconcileOutcome
object ReconcileOutcome:
  /** The counters were adjusted and `record` marked reconciled, atomically. */
  case class Applied(record: ReservationRecord) extends ReconcileOutcome

  /** Nothing was written; `record` was reconciled before. */
  case class AlreadyReconciled(record: ReservationRecord)
      extends ReconcileOutcome

  /** No live reservation at that key. */
  case object NotFound extends ReconcileOutcome

  /** Conditional writes kept losing; nothing was written. */
  case class Contended(attempts: Int) extends ReconcileOutcome

/** One target's read state and the state a reservation would write. */
case class PlannedWrite(
    target: QuotaTarget,
    before: Option[TokenQuotaState],
    after: TokenQuotaState,
)

/** Pure reservation planning shared by every store implementation. */
object QuotaReservation:
  /** Computes the post-reservation state for every target, or the first target
    * in order whose limit the delta would overflow.
    */
  def plan(
      targets: List[QuotaTarget],
      current: Map[String, TokenQuotaState],
      inputDelta: Long,
      outputDelta: Long,
      nowMs: Long,
  ): Either[ReserveOutcome.LimitExceeded, List[PlannedWrite]] = targets
    .traverse { t =>
      val before = current.get(t.pk)
      val after = TokenQuotaState
        .next(before, inputDelta, outputDelta, t.windowSeconds, nowMs)
      if t.limit.exists(after.totalTokens > _) then
        Left(ReserveOutcome.LimitExceeded(
          t.pk,
          TokenQuotaState.usedWithin(before, t.windowSeconds, nowMs),
          after.windowStart,
        ))
      else Right(PlannedWrite(t, before, after))
    }

/** Pure reconciliation planning shared by every store implementation. */
object QuotaReconciliation:
  /** The counter writes that reconcile `record` with actual usage at `nowMs`.
    *
    * A counter still in the window the estimate was charged to gets
    * `actual - estimate`, which may be negative. That can only give back this
    * reservation's own charge, so the counter never drops below the usage
    * recorded by everyone else. A counter whose window has rolled over never
    * held this estimate, so it gets only the overage, never a refund. A refund
    * there would erase other reservations' usage.
    *
    * Counters with nothing to apply are left out; the record is marked
    * reconciled regardless.
    */
  def plan(
      record: ReservationRecord,
      current: Map[String, TokenQuotaState],
      actualInput: Long,
      actualOutput: Long,
      nowMs: Long,
  ): List[PlannedWrite] =
    val inputDelta = actualInput - record.estimatedInput
    val outputDelta = actualOutput - record.estimatedOutput
    record.targets.flatMap { t =>
      val before = current.get(t.pk)
      val charged = before.exists(s =>
        s.windowStart == t.windowStart &&
          TokenQuotaState.inWindow(s, t.windowSeconds, nowMs),
      )
      val (in, out) =
        if charged then (inputDelta, outputDelta)
        else (math.max(0L, inputDelta), math.max(0L, outputDelta))
      Option.when(in != 0 || out != 0)(PlannedWrite(
        QuotaTarget(t.pk, t.windowSeconds, limit = None),
        before,
        TokenQuotaState.next(before, in, out, t.windowSeconds, nowMs),
      ))
    }

trait TokenQuotaStore[F[_]]:
  def getQuota(pk: String): F[Option[TokenQuotaState]]

  /** Applies the deltas to every target or to none of them. With a
    * `reservation`, `Reserved` also means its record was written; if the record
    * cannot be written, the counters are released and the outcome is
    * `Contended`. So an admitted check always leaves something to reconcile.
    */
  def reserve(
      targets: List[QuotaTarget],
      inputDelta: Long,
      outputDelta: Long,
      nowMs: Long,
      reservation: Option[NewReservation] = None,
  ): F[ReserveOutcome]

  /** Applies `QuotaReconciliation.plan` to the reservation at `reservationPk`
    * and marks it reconciled, atomically and at most once.
    */
  def reconcile(
      reservationPk: String,
      actualInput: Long,
      actualOutput: Long,
      nowMs: Long,
  ): F[ReconcileOutcome]

  def healthCheck: F[Either[String, Unit]]

object TokenQuotaStore:
  private case class Memory(
      counters: Map[String, TokenQuotaState],
      reservations: Map[String, ReservationRecord],
  )

  def inMemory[F[_]: Async]: F[TokenQuotaStore[F]] = Ref
    .of[F, Memory](Memory(Map.empty, Map.empty)).map { ref =>
      new TokenQuotaStore[F]:
        override def getQuota(pk: String): F[Option[TokenQuotaState]] = ref.get
          .map(_.counters.get(pk))

        override def reserve(
            targets: List[QuotaTarget],
            inputDelta: Long,
            outputDelta: Long,
            nowMs: Long,
            reservation: Option[NewReservation],
        ): F[ReserveOutcome] = ref.modify { m =>
          QuotaReservation
            .plan(targets, m.counters, inputDelta, outputDelta, nowMs) match
            case Left(exceeded) => (m, exceeded)
            case Right(_)
                if reservation.exists(r => m.reservations.contains(r.pk)) =>
              (m, ReserveOutcome.Contended(1))
            case Right(planned) =>
              val written = planned.map(p => p.target.pk -> p.after).toMap
              val records = reservation
                .map(r => r.pk -> r.record(planned, inputDelta, outputDelta))
              (
                Memory(m.counters ++ written, m.reservations ++ records),
                ReserveOutcome.Reserved(written),
              )
        }

        override def reconcile(
            reservationPk: String,
            actualInput: Long,
            actualOutput: Long,
            nowMs: Long,
        ): F[ReconcileOutcome] = ref.modify { m =>
          m.reservations.get(reservationPk).filterNot(_.expired(nowMs)) match
            case None => (m, ReconcileOutcome.NotFound)
            case Some(record) if record.reconciled.isDefined =>
              (m, ReconcileOutcome.AlreadyReconciled(record))
            case Some(record) =>
              val planned = QuotaReconciliation
                .plan(record, m.counters, actualInput, actualOutput, nowMs)
              val done = record.copy(reconciled =
                Some(ReconciledUsage(actualInput, actualOutput)),
              )
              (
                Memory(
                  m.counters ++ planned.map(p => p.target.pk -> p.after),
                  m.reservations + (reservationPk -> done),
                ),
                ReconcileOutcome.Applied(done),
              )
        }

        override def healthCheck: F[Either[String, Unit]] = Async[F].pure(Right(()))
    }

/** Quota checks and reconciliation. `clientId` scopes every counter (ADR-005):
  * two clients naming the same user, agent, or org meter separate counters.
  */
trait TokenQuotaService[F[_]]:
  def checkQuota(
      clientId: String,
      identifier: QuotaIdentifier,
      estimatedInputTokens: Long,
      estimatedOutputTokens: Long,
  ): F[QuotaDecision]

  /** Reconciles the reservation `reservationId` made for `clientId` with actual
    * usage. The estimate comes from the stored reservation, never from the
    * caller.
    */
  def reconcile(
      clientId: String,
      reservationId: String,
      actualInputTokens: Long,
      actualOutputTokens: Long,
  ): F[ReconcileResult]

object TokenQuotaService:
  private case class Level(level: QuotaLevel, limit: Long, target: QuotaTarget)

  def apply[F[_]: Async](
      store: TokenQuotaStore[F],
      config: TokenQuotaConfig,
      metrics: MetricsPublisher[F],
      logger: Logger[F],
  ): TokenQuotaService[F] = new TokenQuotaService[F]:

    override def checkQuota(
        clientId: String,
        identifier: QuotaIdentifier,
        estimatedInputTokens: Long,
        estimatedOutputTokens: Long,
    ): F[QuotaDecision] =
      val levels = levelsFor(clientId, identifier)
      for
        nowMs <- Clock[F].realTime.map(_.toMillis)
        reservationId <- Async[F].delay(java.util.UUID.randomUUID().toString)
        reservation = NewReservation(
          reservationPk(clientId, reservationId),
          nowMs / 1000 + config.reservationTtlSeconds,
        )
        outcome <- metrics
          .timed("TokenQuotaStoreLatency", Map("operation" -> "quota_reserve"))(
            store.reserve(
              levels.map(_.target),
              estimatedInputTokens,
              estimatedOutputTokens,
              nowMs,
              Some(reservation),
            ),
          )
        _ <- outcome match
          case ReserveOutcome.Reserved(_) =>
            recordAdmitted(levels, estimatedInputTokens + estimatedOutputTokens)
          case _ => Async[F].unit
        decision <- toDecision(levels, outcome, nowMs, reservationId)
      yield decision

    private def recordAdmitted(levels: List[Level], tokens: Long): F[Unit] =
      levels.traverse_(l =>
        metrics.count(
          "QuotaTokensAdmitted",
          tokens.toDouble,
          Map("level" -> l.level.prefix),
        ),
      )

    // Actual usage already happened, so the store records it even past the
    // limit.
    override def reconcile(
        clientId: String,
        reservationId: String,
        actualInputTokens: Long,
        actualOutputTokens: Long,
    ): F[ReconcileResult] =
      val usage = ReconciledUsage(actualInputTokens, actualOutputTokens)
      def deltas(record: ReservationRecord) = ReconcileResult.Reconciled(
        actualInputTokens - record.estimatedInput,
        actualOutputTokens - record.estimatedOutput,
      )
      for
        nowMs <- Clock[F].realTime.map(_.toMillis)
        outcome <- store.reconcile(
          reservationPk(clientId, reservationId),
          actualInputTokens,
          actualOutputTokens,
          nowMs,
        )
        result <- outcome match
          case ReconcileOutcome.Applied(record) => Async[F].pure(deltas(record))
          // A retry after a lost response: same usage, same answer, nothing
          // applied twice.
          case ReconcileOutcome.AlreadyReconciled(record)
              if record.reconciled.contains(usage) =>
            Async[F].pure(deltas(record))
          case ReconcileOutcome.AlreadyReconciled(record) => metrics
              .increment("TokenQuotaReconcileConflict") *> Async[F].pure(
              ReconcileResult.Conflict(record.reconciled.getOrElse(usage)),
            )
          case ReconcileOutcome.NotFound => Async[F]
              .pure(ReconcileResult.NotFound)
          case ReconcileOutcome.Contended(attempts) => metrics
              .increment("TokenQuotaReconcileFailed") *>
              logger.warn(s"Quota reconciliation of $reservationId lost $attempts conditional writes; usage not recorded")
                .as(ReconcileResult.Contended(attempts))
      yield result

    private def toDecision(
        levels: List[Level],
        outcome: ReserveOutcome,
        nowMs: Long,
        reservationId: String,
    ): F[QuotaDecision] = outcome match
      case ReserveOutcome.Reserved(states) =>
        val remaining = levels
          .map(l => l.level -> (l.limit - states(l.target.pk).totalTokens))
        Async[F].pure(QuotaDecision.Available(remaining.toMap, reservationId))

      case ReserveOutcome.LimitExceeded(pk, used, windowStart) =>
        levelFor(levels, pk).flatMap { l =>
          val retryAfter =
            secondsUntilReset(windowStart, l.target.windowSeconds, nowMs)
          metrics
            .increment("TokenQuotaExceeded", Map("level" -> l.level.prefix)) *>
            logger.info(s"Quota exceeded at ${l.level
                .prefix} level: used=$used, limit=${l.limit}")
              .as(QuotaDecision.Exceeded(l.level, l.limit, used, retryAfter))
        }

      case ReserveOutcome.Contended(attempts) => metrics
          .increment("TokenQuotaContended") *>
          logger.warn(s"Quota reservation lost $attempts conditional writes; nothing reserved")
            .as(QuotaDecision.Contended(attempts))

    private def levelFor(levels: List[Level], pk: String): F[Level] =
      levels.find(_.target.pk == pk) match
        case Some(l) => Async[F].pure(l)
        case None => Async[F].raiseError(new IllegalStateException(
            s"Store reported unknown quota target $pk",
          ))

    // Scoped like every other key (ADR-005): another client's reservation ID
    // finds nothing.
    private def reservationPk(clientId: String, reservationId: String): String =
      s"reservation:${TenantKey(clientId, reservationId)}"

    private def levelsFor(clientId: String, id: QuotaIdentifier): List[Level] =
      val user = mkLevel(
        clientId,
        QuotaLevel.User,
        id.userId,
        config.userLimit,
        config.userWindowSeconds,
      )
      val agent = id.agentId.map { aid =>
        val limit = math.min(config.agentLimit, (config.userLimit * 0.8).toLong)
        mkLevel(
          clientId,
          QuotaLevel.Agent,
          aid,
          limit,
          config.agentWindowSeconds,
        )
      }
      val org = id.orgId.map(oid =>
        mkLevel(
          clientId,
          QuotaLevel.Org,
          oid,
          config.orgLimit,
          config.orgWindowSeconds,
        ),
      )
      user :: agent.toList ::: org.toList

    private def mkLevel(
        clientId: String,
        l: QuotaLevel,
        id: String,
        limit: Long,
        windowSec: Long,
    ): Level = Level(
      l,
      limit,
      QuotaTarget(
        s"${l.prefix}:${TenantKey(clientId, id)}:${windowSec}s",
        windowSec,
        Some(limit),
      ),
    )

    private def secondsUntilReset(
        windowStart: Long,
        windowSeconds: Long,
        nowMs: Long,
    ): Int =
      val remainingMs = windowStart + windowSeconds * 1000 - nowMs
      math.max(1L, (remainingMs + 999) / 1000).toInt
