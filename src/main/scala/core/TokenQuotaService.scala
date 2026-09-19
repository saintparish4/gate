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
  case class Available(remainingByLevel: Map[QuotaLevel, Long])
      extends QuotaDecision

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
  case class Reconciled(inputDelta: Long, outputDelta: Long)
      extends ReconcileResult

  /** The adjustment was not recorded; the caller should retry it. */
  case class Contended(attempts: Int) extends ReconcileResult

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

trait TokenQuotaStore[F[_]]:
  def getQuota(pk: String): F[Option[TokenQuotaState]]

  /** Applies the deltas to every target or to none of them. */
  def reserve(
      targets: List[QuotaTarget],
      inputDelta: Long,
      outputDelta: Long,
      nowMs: Long,
  ): F[ReserveOutcome]

  def healthCheck: F[Either[String, Unit]]

object TokenQuotaStore:
  def inMemory[F[_]: Async]: F[TokenQuotaStore[F]] = Ref
    .of[F, Map[String, TokenQuotaState]](Map.empty).map { ref =>
      new TokenQuotaStore[F]:
        override def getQuota(pk: String): F[Option[TokenQuotaState]] = ref.get
          .map(_.get(pk))

        override def reserve(
            targets: List[QuotaTarget],
            inputDelta: Long,
            outputDelta: Long,
            nowMs: Long,
        ): F[ReserveOutcome] = ref.modify(m =>
          QuotaReservation
            .plan(targets, m, inputDelta, outputDelta, nowMs) match
            case Left(exceeded) => (m, exceeded)
            case Right(planned) =>
              val written = planned.map(p => p.target.pk -> p.after).toMap
              (m ++ written, ReserveOutcome.Reserved(written)),
        )

        override def healthCheck: F[Either[String, Unit]] = Async[F].pure(Right(()))
    }

trait TokenQuotaService[F[_]]:
  def checkQuota(
      identifier: QuotaIdentifier,
      estimatedInputTokens: Long,
      estimatedOutputTokens: Long,
  ): F[QuotaDecision]

  def reconcile(
      identifier: QuotaIdentifier,
      actualInputTokens: Long,
      actualOutputTokens: Long,
      estimatedInputTokens: Long,
      estimatedOutputTokens: Long,
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
        identifier: QuotaIdentifier,
        estimatedInputTokens: Long,
        estimatedOutputTokens: Long,
    ): F[QuotaDecision] =
      val levels = levelsFor(identifier)
      for
        nowMs <- Clock[F].realTime.map(_.toMillis)
        outcome <- metrics
          .timed("TokenQuotaStoreLatency", Map("operation" -> "quota_reserve"))(
            store.reserve(
              levels.map(_.target),
              estimatedInputTokens,
              estimatedOutputTokens,
              nowMs,
            ),
          )
        _ <- outcome match
          case ReserveOutcome.Reserved(_) =>
            recordAdmitted(levels, estimatedInputTokens + estimatedOutputTokens)
          case _ => Async[F].unit
        decision <- toDecision(levels, outcome, nowMs)
      yield decision

    private def recordAdmitted(levels: List[Level], tokens: Long): F[Unit] =
      levels.traverse_(l =>
        metrics.count(
          "QuotaTokensAdmitted",
          tokens.toDouble,
          Map("level" -> l.level.prefix),
        ),
      )

    override def reconcile(
        identifier: QuotaIdentifier,
        actualInputTokens: Long,
        actualOutputTokens: Long,
        estimatedInputTokens: Long,
        estimatedOutputTokens: Long,
    ): F[ReconcileResult] =
      val inputDelta = actualInputTokens - estimatedInputTokens
      val outputDelta = actualOutputTokens - estimatedOutputTokens
      if inputDelta == 0 && outputDelta == 0 then
        Async[F].pure(ReconcileResult.Reconciled(0, 0))
      else
        val levels = levelsFor(identifier)
        // Actual usage already happened, so I record it even past the limit.
        val unlimited = levels.map(_.target.copy(limit = None))
        for
          nowMs <- Clock[F].realTime.map(_.toMillis)
          outcome <- store.reserve(unlimited, inputDelta, outputDelta, nowMs)
          result <-
            toReconcileResult(identifier, outcome, inputDelta, outputDelta)
        yield result

    private def toDecision(
        levels: List[Level],
        outcome: ReserveOutcome,
        nowMs: Long,
    ): F[QuotaDecision] = outcome match
      case ReserveOutcome.Reserved(states) =>
        val remaining = levels
          .map(l => l.level -> (l.limit - states(l.target.pk).totalTokens))
        Async[F].pure(QuotaDecision.Available(remaining.toMap))

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

    private def toReconcileResult(
        identifier: QuotaIdentifier,
        outcome: ReserveOutcome,
        inputDelta: Long,
        outputDelta: Long,
    ): F[ReconcileResult] = outcome match
      case ReserveOutcome.Reserved(_) => Async[F]
          .pure(ReconcileResult.Reconciled(inputDelta, outputDelta))

      case ReserveOutcome.Contended(attempts) => metrics
          .increment("TokenQuotaReconcileFailed") *>
          logger.warn(s"Quota reconciliation for ${identifier
              .userId} lost $attempts conditional writes; usage not recorded")
            .as(ReconcileResult.Contended(attempts))

      case ReserveOutcome.LimitExceeded(pk, _, _) => Async[F]
          .raiseError(new IllegalStateException(
            s"Unlimited reservation on $pk reported a limit",
          ))

    private def levelFor(levels: List[Level], pk: String): F[Level] =
      levels.find(_.target.pk == pk) match
        case Some(l) => Async[F].pure(l)
        case None => Async[F].raiseError(new IllegalStateException(
            s"Store reported unknown quota target $pk",
          ))

    private def levelsFor(id: QuotaIdentifier): List[Level] =
      val user = mkLevel(
        QuotaLevel.User,
        id.userId,
        config.userLimit,
        config.userWindowSeconds,
      )
      val agent = id.agentId.map { aid =>
        val limit = math.min(config.agentLimit, (config.userLimit * 0.8).toLong)
        mkLevel(QuotaLevel.Agent, aid, limit, config.agentWindowSeconds)
      }
      val org = id.orgId.map(oid =>
        mkLevel(QuotaLevel.Org, oid, config.orgLimit, config.orgWindowSeconds),
      )
      user :: agent.toList ::: org.toList

    private def mkLevel(
        l: QuotaLevel,
        id: String,
        limit: Long,
        windowSec: Long,
    ): Level = Level(
      l,
      limit,
      QuotaTarget(s"${l.prefix}:$id:${windowSec}s", windowSec, Some(limit)),
    )

    private def secondsUntilReset(
        windowStart: Long,
        windowSeconds: Long,
        nowMs: Long,
    ): Int =
      val remainingMs = windowStart + windowSeconds * 1000 - nowMs
      math.max(1L, (remainingMs + 999) / 1000).toInt
