package resilience

import scala.concurrent.duration.*

import org.typelevel.log4cats.Logger

import cats.effect.*
import cats.effect.syntax.monadCancel.*
import cats.syntax.all.*

/** Graceful degradation strategies for when dependencies fail.
  *
  * These utilities help the rate limiter remain functional even when DynamoDB
  * or Kinesis are unavailable, trading off some accuracy for availability.
  */
object GracefulDegradation:

  /** Degradation mode when primary storage is unavailable.
    */
  sealed trait DegradationMode
  object DegradationMode:
    /** Fail open - allow all requests (use when availability > accuracy) */
    case object AllowAll extends DegradationMode

    /** Fail closed - reject all requests (use when accuracy > availability) */
    case object RejectAll extends DegradationMode

    /** Use cached/stale data if available */
    case object UseCached extends DegradationMode

    /** Apply a reduced rate limit */
    case class ReducedLimit(tokensPerSecond: Int) extends DegradationMode

  /** Degradation configuration.
    */
  case class DegradationConfig(
      mode: DegradationMode = DegradationMode.AllowAll,
      degradationWindow: FiniteDuration = 30.seconds,
      logSampling: Int = 100, // Log 1 in N degraded requests
  )

  /** Policy when a cache lookup misses (UseCached mode with no cached
    * decision).
    */
  sealed trait CacheMissPolicy
  object CacheMissPolicy:
    case object AllowAll extends CacheMissPolicy
    case object RejectAll extends CacheMissPolicy

  /** Execute with fallback behavior when the primary operation fails.
    *
    * @param primary
    *   The primary operation to attempt
    * @param fallback
    *   The fallback operation if primary fails
    * @param operationName
    *   Name for logging
    */
  def withFallback[F[_]: Temporal: Logger, A](
      primary: F[A],
      fallback: F[A],
      operationName: String,
  ): F[A] =
    val logger = Logger[F]
    primary.handleErrorWith(error =>
      logger
        .warn(s"$operationName failed, using fallback: ${error.getMessage}") *>
        fallback,
    )

  /** Execute with a timeout and fallback.
    */
  def withTimeoutAndFallback[F[_]: Temporal: Logger, A](
      primary: F[A],
      fallback: F[A],
      timeout: FiniteDuration,
      operationName: String,
  ): F[A] =
    val logger = Logger[F]
    Temporal[F].timeout(primary, timeout).handleErrorWith(error =>
      logger.warn(s"$operationName timed out or failed after $timeout: ${error
          .getMessage}") *> fallback,
    )

  /** Create a rate limit decision for degraded mode.
    *
    * UseCached: if `cache` is provided, looks up the key and returns the cached
    * decision on hit; on miss (or no cache) uses `cacheMissPolicy`.
    * ReducedLimit: if `reducedLimitState` is provided, tracks per-key
    * per-window usage and allows up to tps per second; otherwise returns
    * Allowed(tps, resetAt).
    */
  def degradedDecision[F[_]: Temporal](
      mode: DegradationMode,
      key: String,
      cache: Option[LocalCache[F, String, core.RateLimitDecision]] = None,
      cacheMissPolicy: CacheMissPolicy = CacheMissPolicy.AllowAll,
      reducedLimitState: Option[Ref[F, Map[String, (Long, Int)]]] = None,
  ): F[core.RateLimitDecision] =
    import core.RateLimitDecision.*

    Clock[F].realTime.flatMap { now =>
      val nowMs = now.toMillis
      val resetAt = java.time.Instant.ofEpochMilli(nowMs + 60000)

      mode match
        case DegradationMode.AllowAll => Temporal[F]
            .pure(Allowed(tokensRemaining = 100, resetAt = resetAt))

        case DegradationMode.RejectAll => Temporal[F]
            .pure(Rejected(retryAfterSeconds = 60, resetAt = resetAt))

        case DegradationMode.UseCached => cache match
            case Some(c) => c.get(key).map {
                case Some(decision) => decision
                case None => cacheMissPolicy match
                    case CacheMissPolicy.AllowAll =>
                      Allowed(tokensRemaining = 100, resetAt = resetAt)
                    case CacheMissPolicy.RejectAll =>
                      Rejected(retryAfterSeconds = 60, resetAt = resetAt)
              }
            case None => Temporal[F].pure(
                cacheMissPolicy match
                  case CacheMissPolicy.AllowAll =>
                    Allowed(tokensRemaining = 100, resetAt = resetAt)
                  case CacheMissPolicy.RejectAll =>
                    Rejected(retryAfterSeconds = 60, resetAt = resetAt),
              )

        case DegradationMode.ReducedLimit(tps) => reducedLimitState match
            case Some(stateRef) =>
              val windowStart = nowMs / 1000 * 1000 // floor to current second
              stateRef.modify { map =>
                val (ws, count) = map.getOrElse(key, (windowStart, 0))
                if ws == windowStart then
                  if count < tps then
                    (
                      map.updated(key, (windowStart, count + 1)),
                      Allowed(
                        tokensRemaining = tps - count - 1,
                        resetAt = resetAt,
                      ),
                    )
                  else (map, Rejected(retryAfterSeconds = 1, resetAt = resetAt))
                else
                  (
                    map.updated(key, (windowStart, 1)),
                    Allowed(tokensRemaining = tps - 1, resetAt = resetAt),
                  )
              }
            case None => Temporal[F]
                .pure(Allowed(tokensRemaining = tps, resetAt = resetAt))
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

      override def execute[A](fa: F[A]): F[A] = queuedRef.update(_ + 1) *>
        Temporal[F].timeout(
          semaphore.permit.use(_ =>
            queuedRef.update(_ - 1) *> inFlightRef.update(_ + 1) *>
              fa.guarantee(inFlightRef.update(_ - 1)),
          ),
          config.maxWait,
        ).handleErrorWith {
          case _: java.util.concurrent.TimeoutException => queuedRef
              .update(_ - 1) *> logger.warn(
              s"Bulkhead '$name' rejected request - wait timeout exceeded",
            ) *> Temporal[F].raiseError(core.GateError.BulkheadFull(name))
          case e => Temporal[F].raiseError(e)
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
