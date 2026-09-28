package resilience

import scala.concurrent.duration.*

import org.typelevel.log4cats.Logger

import cats.effect.*
import cats.effect.syntax.monadCancel.*
import cats.syntax.all.*

/** What a rate-limit check answers when the store cannot: the circuit breaker
  * is open, the bulkhead is full, or the call failed.
  *
  * Only the two modes config can select exist. `UseCached` and `ReducedLimit`
  * used to be here too, with a cache and a per-key counter behind them, but
  * config only ever mapped to allow-all or reject-all, so they were
  * unreachable. A configured `use-cached` now stops startup
  * (AppConfig.validateDegradationMode).
  */
object GracefulDegradation:

  sealed trait DegradationMode
  object DegradationMode:
    /** Fail open: admit everything, so downstream spend is unbounded while
      * degraded.
      */
    case object AllowAll extends DegradationMode

    /** Fail closed: refuse everything. The default. */
    case object RejectAll extends DegradationMode

  def degradedDecision[F[_]: Temporal](
      mode: DegradationMode,
  ): F[core.RateLimitDecision] = Clock[F].realTime.map { now =>
    val resetAt = java.time.Instant.ofEpochMilli(now.toMillis + 60000)
    mode match
      case DegradationMode.AllowAll => core.RateLimitDecision
          .Allowed(tokensRemaining = 100, resetAt = resetAt)
      case DegradationMode.RejectAll => core.RateLimitDecision
          .Rejected(retryAfterSeconds = 60, resetAt = resetAt)
  }

/** Bulkhead pattern implementation for isolating failures.
  *
  * Limits concurrent access to a resource to prevent overwhelming it and
  * isolate failures from affecting other parts of the system.
  */
trait Bulkhead[F[_]]:
  /** Execute an effect within the bulkhead constraints.
    *
    * @param fa
    *   The effect to execute
    * @return
    *   The result, or fails with BulkheadRejected if at capacity
    */
  def execute[A](fa: F[A]): F[A]

  /** Get current number of in-flight requests */
  def inFlight: F[Int]

  /** Get number of queued requests */
  def queued: F[Int]

case class BulkheadConfig(
    maxConcurrent: Int = 25,
    maxWait: FiniteDuration = 100.millis,
)

object BulkheadConfig:
  val default: BulkheadConfig = BulkheadConfig()

  /** High-throughput configuration */
  val highThroughput: BulkheadConfig =
    BulkheadConfig(maxConcurrent = 100, maxWait = 500.millis)

/** Kept for binary/source compatibility. Use GateError.BulkheadFull directly.
  */
type BulkheadRejected = core.GateError.BulkheadFull

object Bulkhead:
  import cats.effect.std.Semaphore

  def apply[F[_]: Temporal: Logger](
      name: String,
      config: BulkheadConfig = BulkheadConfig.default,
  ): F[Bulkhead[F]] =
    for
      semaphore <- Semaphore[F](config.maxConcurrent)
      inFlightRef <- Ref.of[F, Int](0)
      queuedRef <- Ref.of[F, Int](0)
    yield new Bulkhead[F]:
      private val logger = Logger[F]

      // Only the wait for a permit is bounded by maxWait. The operation itself
      // runs to completion once admitted, otherwise a slow backend turns the
      // bulkhead into a hard timeout that cancels writes already in flight.
      override def execute[A](fa: F[A]): F[A] =
        val acquire = Temporal[F].timeout(semaphore.acquire, config.maxWait)
          .adaptError { case _: java.util.concurrent.TimeoutException =>
            core.GateError.BulkheadFull(name)
          }
        queuedRef.update(_ + 1) *>
          Temporal[F].bracketFull(poll => poll(acquire))(_ =>
            queuedRef.update(_ - 1) *> inFlightRef.update(_ + 1) *>
              fa.guarantee(inFlightRef.update(_ - 1)),
          )((_, _) => semaphore.release).onError {
            case _: core.GateError.BulkheadFull => queuedRef.update(_ - 1) *>
                logger.warn(
                  s"Bulkhead '$name' rejected request - wait timeout exceeded",
                )
          }

      override def inFlight: F[Int] = inFlightRef.get
      override def queued: F[Int] = queuedRef.get

/** Health-aware wrapper that degrades gracefully based on dependency health.
  */
trait HealthAwareService[F[_]]:
  /** Check if the service is healthy */
  def isHealthy: F[Boolean]

  /** Get health details */
  def healthDetails: F[HealthDetails]

case class HealthDetails(
    healthy: Boolean,
    lastSuccessTime: Option[Long],
    lastFailureTime: Option[Long],
    consecutiveFailures: Int,
    lastError: Option[String],
)

object HealthAwareService:

  /** Create a health tracker that monitors operation success/failure.
    */
  def tracker[F[_]: Temporal](
      unhealthyThreshold: Int = 3,
      recoveryThreshold: Int = 2,
  ): F[HealthTracker[F]] = Ref.of[F, HealthState](HealthState.initial)
    .map { stateRef =>
      new HealthTracker[F]:
        override def recordSuccess: F[Unit] = Clock[F].realTime.map(_.toMillis)
          .flatMap(now => stateRef.update(_.success(now, recoveryThreshold)))

        override def recordFailure(error: Throwable): F[Unit] = Clock[F]
          .realTime.map(_.toMillis).flatMap(now =>
            stateRef.update(_.failure(now, error, unhealthyThreshold)),
          )

        override def isHealthy: F[Boolean] = stateRef.get.map(_.healthy)

        override def healthDetails: F[HealthDetails] = stateRef.get.map(s =>
          HealthDetails(
            healthy = s.healthy,
            lastSuccessTime = s.lastSuccessTime,
            lastFailureTime = s.lastFailureTime,
            consecutiveFailures = s.consecutiveFailures,
            lastError = s.lastError,
          ),
        )
    }

  private case class HealthState(
      healthy: Boolean,
      consecutiveSuccesses: Int,
      consecutiveFailures: Int,
      lastSuccessTime: Option[Long],
      lastFailureTime: Option[Long],
      lastError: Option[String],
  ):
    def success(now: Long, recoveryThreshold: Int): HealthState =
      val newConsecutive = consecutiveSuccesses + 1
      copy(
        healthy = healthy || newConsecutive >= recoveryThreshold,
        consecutiveSuccesses = newConsecutive,
        consecutiveFailures = 0,
        lastSuccessTime = Some(now),
      )

    def failure(
        now: Long,
        error: Throwable,
        unhealthyThreshold: Int,
    ): HealthState =
      val newConsecutive = consecutiveFailures + 1
      copy(
        healthy = healthy && newConsecutive < unhealthyThreshold,
        consecutiveSuccesses = 0,
        consecutiveFailures = newConsecutive,
        lastFailureTime = Some(now),
        lastError = Some(error.getMessage),
      )

  private object HealthState:
    val initial: HealthState = HealthState(
      healthy = true,
      consecutiveSuccesses = 0,
      consecutiveFailures = 0,
      lastSuccessTime = None,
      lastFailureTime = None,
      lastError = None,
    )

trait HealthTracker[F[_]] extends HealthAwareService[F]:
  def recordSuccess: F[Unit]
  def recordFailure(error: Throwable): F[Unit]
