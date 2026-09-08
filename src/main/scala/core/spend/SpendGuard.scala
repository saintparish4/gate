package core.spend

import org.typelevel.log4cats.Logger

import cats.effect.*
import cats.syntax.all.*
import core.{QuotaTarget, ReserveOutcome, TokenQuotaStore}
import observability.MetricsPublisher

/** Where an agent sits in the tree that spawned it.
  *
  * A multi-agent run is a tree: a planner spawns researchers, each researcher
  * spawns scrapers. Spend has to be charged to the whole ancestry, not just the
  * leaf, or a parent's budget means nothing as soon as it delegates.
  */
case class AgentPath(segments: List[String]):
  def isEmpty: Boolean = segments.isEmpty

  def render: String = segments.mkString("/")

  /** Every ancestor including self, outermost first: a/b/c yields a, a/b, a/b/c.
    */
  def lineage: List[AgentPath] = segments.inits.toList.reverse.collect {
    case s if s.nonEmpty => AgentPath(s)
  }

object AgentPath:
  val root: AgentPath = AgentPath(Nil)

  /** Accepts "a/b/c". Empty segments are dropped so "a//b" and "/a/b" behave. */
  def parse(s: String): AgentPath =
    AgentPath(s.split('/').toList.map(_.trim).filter(_.nonEmpty))

case class SpendIdentifier(
    userId: String,
    orgId: Option[String] = None,
    agent: AgentPath = AgentPath.root,
)

/** A budget that applies to one scope. */
case class Budget(scope: String, limit: MicroUsd, windowSeconds: Long)

sealed trait SpendDecision
object SpendDecision:
  /** The call is allowed to proceed using `model`, and `cost` has already been
    * reserved against every scope in the lineage.
    */
  case class Admitted(
      model: String,
      requestedModel: String,
      cost: MicroUsd,
      remaining: Map[String, MicroUsd],
  ) extends SpendDecision:
    def wasDowngraded: Boolean = model != requestedModel

  /** Not even the cheapest permitted substitute fits. Nothing was reserved. */
  case class Denied(
      scope: String,
      limit: MicroUsd,
      used: MicroUsd,
      retryAfterSeconds: Int,
      triedModels: List[String],
  ) extends SpendDecision

  /** The store lost its whole retry budget. Nothing was reserved; fail closed. */
  case class Contended(attempts: Int) extends SpendDecision

  /** The caller named a model with no price. Guessing a price would either
    * over-charge a customer or let an unpriced model spend without limit.
    */
  case class UnknownModel(model: String) extends SpendDecision

trait SpendGuard[F[_]]:
  /** Reserves the estimated cost of the call, downgrading the model rather than
    * denying outright when the requested one does not fit.
    */
  def authorize(
      id: SpendIdentifier,
      model: String,
      estimatedInputTokens: Long,
      estimatedOutputTokens: Long,
  ): F[SpendDecision]

  /** Replaces the estimate with what the call actually cost. Recorded even when
    * it lands over budget -- refusing to record real spend is how ledgers lie.
    */
  def settle(
      id: SpendIdentifier,
      model: String,
      actualInputTokens: Long,
      actualOutputTokens: Long,
      estimatedCost: MicroUsd,
  ): F[SpendSettlement]

sealed trait SpendSettlement
object SpendSettlement:
  case class Settled(delta: MicroUsd, actualCost: MicroUsd)
      extends SpendSettlement
  case class Contended(attempts: Int) extends SpendSettlement
  case class UnknownModel(model: String) extends SpendSettlement

object SpendGuard:

  /** Scopes share the token-quota table but live under their own key prefix, so
    * dollar counters can never collide with token counters.
    */
  private val KeyPrefix = "spend"

  def apply[F[_]: Async](
      store: TokenQuotaStore[F],
      budgets: BudgetBook,
      prices: PriceTable,
      metrics: MetricsPublisher[F],
      logger: Logger[F],
  ): SpendGuard[F] = new SpendGuard[F]:

    override def authorize(
        id: SpendIdentifier,
        model: String,
        estimatedInputTokens: Long,
        estimatedOutputTokens: Long,
    ): F[SpendDecision] =
      if !prices.knows(model) then
        logger.warn(s"Spend authorize for unpriced model '$model'")
          .as(SpendDecision.UnknownModel(model))
      else
        val scopes = budgets.scopesFor(id)
        Clock[F].realTime.map(_.toMillis).flatMap(now =>
          attempt(
            prices.candidates(model),
            model,
            estimatedInputTokens,
            estimatedOutputTokens,
            scopes,
            now,
            firstRefusal = None,
          ),
        )

    /** Tries each permitted model in turn. Every attempt is one atomic
      * all-or-nothing reservation, so a rejected attempt leaves nothing behind
      * and the next candidate starts from a clean slate.
      */
    private def attempt(
        remainingCandidates: List[String],
        requested: String,
        inTokens: Long,
        outTokens: Long,
        scopes: List[Budget],
        now: Long,
        firstRefusal: Option[ReserveOutcome.LimitExceeded],
    ): F[SpendDecision] = remainingCandidates match
      case Nil => Async[F].pure(deny(firstRefusal, scopes, requested, now))

      case candidate :: rest =>
        val cost = prices.costOf(candidate, inTokens, outTokens)
          .getOrElse(MicroUsd.zero)
        store.reserve(targets(scopes), cost.micros, 0L, now).flatMap {
          case ReserveOutcome.Reserved(states) =>
            val remaining = scopes.map(b =>
              b.scope -> (b.limit - MicroUsd(
                states.get(pk(b)).map(_.totalTokens).getOrElse(0L),
              )).clampedAtZero,
            ).toMap
            recordAdmission(candidate, requested)
              .as(SpendDecision.Admitted(candidate, requested, cost, remaining))

          case e: ReserveOutcome.LimitExceeded => attempt(
              rest,
              requested,
              inTokens,
              outTokens,
              scopes,
              now,
              firstRefusal.orElse(Some(e)),
            )

          case ReserveOutcome.Contended(attempts) =>
            metrics.increment("SpendContended") *>
              logger.warn(s"Spend reservation lost $attempts writes for ${id(
                  scopes,
                )}").as(SpendDecision.Contended(attempts))
        }

    override def settle(
        id: SpendIdentifier,
        model: String,
        actualInputTokens: Long,
        actualOutputTokens: Long,
        estimatedCost: MicroUsd,
    ): F[SpendSettlement] = prices
      .costOf(model, actualInputTokens, actualOutputTokens) match
      case None => Async[F].pure(SpendSettlement.UnknownModel(model))
      case Some(actual) =>
        val delta = actual - estimatedCost
        // Unlimited targets so a true overrun is still recorded rather than
        // rejected; the money was already spent upstream.
        val unlimited = budgets.scopesFor(id)
          .map(b => QuotaTarget(pk(b), b.windowSeconds, None))
        Clock[F].realTime.map(_.toMillis).flatMap(now =>
          store.reserve(unlimited, delta.micros, 0L, now).flatMap {
            case ReserveOutcome.Reserved(_) => metrics
                .gauge("gate_spend_micro_usd", actual.micros.toDouble)
                .as(SpendSettlement.Settled(delta, actual))
            case ReserveOutcome.Contended(n) => metrics
                .increment("SpendSettleFailed")
                .as(SpendSettlement.Contended(n))
            case _: ReserveOutcome.LimitExceeded => Async[F].pure(
                SpendSettlement.Settled(delta, actual),
              )
          },
        )

    private def recordAdmission(chosen: String, requested: String): F[Unit] =
      if chosen == requested then metrics.increment("SpendAdmitted")
      else metrics.increment("SpendDowngraded") *>
        logger.info(s"Downgraded $requested -> $chosen to stay within budget")

    private def id(scopes: List[Budget]): String = scopes.map(_.scope)
      .mkString(",")

  private def pk(b: Budget): String = s"$KeyPrefix:${b.scope}"

  private def targets(scopes: List[Budget]): List[QuotaTarget] = scopes
    .map(b => QuotaTarget(pk(b), b.windowSeconds, Some(b.limit.micros)))

  private def deny(
      refusal: Option[ReserveOutcome.LimitExceeded],
      scopes: List[Budget],
      requested: String,
      now: Long,
  ): SpendDecision = refusal match
    case None => SpendDecision
        .Denied("unknown", MicroUsd.zero, MicroUsd.zero, 1, List(requested))
    case Some(e) =>
      val budget = scopes.find(b => pk(b) == e.pk)
      val window = budget.map(_.windowSeconds).getOrElse(0L)
      val elapsed = math.max(0L, (now - e.windowStart) / 1000L)
      SpendDecision.Denied(
        scope = budget.map(_.scope).getOrElse(e.pk),
        limit = budget.map(_.limit).getOrElse(MicroUsd.zero),
        used = MicroUsd(e.used),
        retryAfterSeconds = math.max(1L, window - elapsed).toInt,
        triedModels = List(requested),
      )
