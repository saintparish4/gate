package resilience

import scala.concurrent.duration.*

import org.typelevel.log4cats.Logger

import cats.effect.*
import cats.syntax.all.*
import config.{
  BulkheadSettings, CircuitBreakerSettings, ResilienceConfig, RetryConfig,
  RetrySettings, TimeoutSettings,
}
import resilience.CircuitBreakerConfig
import core.{RateLimitDecision, RateLimitProfile, RateLimitStore}
import events.{EventPublisher, RateLimitEvent}
import observability.MetricsPublisher

/** Resilient wrapper that applies production hardening patterns to the rate
  * limit store.
  *
  * Provides:
  *   - Circuit breaker protection against cascading failures
  *   - Retry with exponential backoff for transient failures
  *   - Local caching for reduced latency and backend protection
  *   - Bulkhead isolation to prevent resource exhaustion
  *   - Graceful degradation when dependencies fail
  *   - Comprehensive metrics and logging
  */
object ResilientRateLimitStore:

  /** Whether an error is evidence that DynamoDB itself is unhealthy, and so
    * should count toward opening the shared circuit breaker.
    *
    * This breaker is process-wide: one instance guards every key. Counting
    * caller-scoped failures therefore lets a single contended key open it for
    * all tenants, and with degradation-mode=reject-all that is a full outage
    * caused by one noisy neighbour. An OCC conflict or a corrupt item is also
    * positive evidence the dependency is alive -- DynamoDB answered us, this
    * key just lost a race -- so it must not be counted against it.
    *
    * Timeouts and transport faults stay counted. They mean we got no answer at
    * all, which is exactly what the breaker exists to detect.
    */
  private[resilience] def dependencyFailure(error: Throwable): Boolean =
    error match
      // Caller- or key-scoped: the dependency responded.
      case _: core.GateError.OCCConflict => false
      case _: core.GateError.CorruptState => false
      case _: software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException =>
        false
      // Load we already shed. Counting these would let the breaker feed itself.
      case _: core.GateError.CircuitOpen => false
      case _: core.GateError.BulkheadFull => false
      // A misconfiguration is not something a breaker can ride out.
      case _: core.GateError.ConfigError => false
      case _ => true

  /** Wrap a rate limit store with production resilience patterns.
    */
  def apply[F[_]: Temporal: Logger](
      underlying: RateLimitStore[F],
      config: ResilienceConfig,
      metrics: MetricsPublisher[F],
      eventPublisher: EventPublisher[F],
      degradationMode: GracefulDegradation.DegradationMode =
        GracefulDegradation.DegradationMode.AllowAll,
      degradationCache: Option[LocalCache[F, String, RateLimitDecision]] = None,
  ): Resource[F, RateLimitStore[F]] =
    for
      // Create circuit breaker
      circuitBreaker <- Resource.eval {
        if config.circuitBreaker.enabled then
          CircuitBreaker[F](
            "dynamodb-ratelimit",
            resilience.CircuitBreakerConfig(
              maxFailures = config.circuitBreaker.dynamodb.maxFailures,
              resetTimeout = config.circuitBreaker.dynamodb.resetTimeout,
              halfOpenMaxCalls = config.circuitBreaker.dynamodb.halfOpenMaxCalls,
            ),
            countsAsFailure = dependencyFailure,
          ).map(Some(_))
        else Temporal[F].pure(None)
      }

      // Create bulkhead
      bulkhead <- Resource.eval(
        if config.bulkhead.enabled then
          Bulkhead[F](
            "ratelimit-operations",
            BulkheadConfig(
              maxConcurrent = config.bulkhead.maxConcurrent,
              maxWait = config.bulkhead.maxWait,
            ),
          ).map(Some(_))
        else Temporal[F].pure(None),
      )

      // Create health tracker
      healthTracker <- Resource.eval(HealthAwareService.tracker[F]())
      reducedLimitRef <- Resource
        .eval(Ref.of[F, Map[String, (Long, Int)]](Map.empty))
    yield new RateLimitStore[F]:
      private val logger = Logger[F]

      private val retryPolicy = RetryPolicy(
        maxRetries = config.retry.dynamodb.maxRetries,
        baseDelay = config.retry.dynamodb.baseDelay,
        maxDelay = config.retry.dynamodb.maxDelay,
        multiplier = config.retry.dynamodb.multiplier,
        retryOn = isRetryable,
      )

      override def checkAndConsume(
          key: String,
          cost: Int,
          profile: RateLimitProfile,
      ): F[RateLimitDecision] =
        val operation = metrics
          .timed("RateLimitCheckLatency", Map("operation" -> "checkAndConsume"))(
            underlying.checkAndConsume(key, cost, profile),
          )

        // Apply patterns in order: bulkhead -> circuit breaker -> retry -> timeout
        val wrappedOp = applyPatterns(operation, "checkAndConsume")

        // Handle failures with graceful degradation
        wrappedOp.flatTap(decision => recordDecision(key, decision))
          .flatTap(_ => healthTracker.recordSuccess).handleErrorWith(error =>
            healthTracker.recordFailure(error) *> handleDegradation(key, error),
          )

      override def getStatus(
          key: String,
          profile: RateLimitProfile,
      ): F[Option[RateLimitDecision.Allowed]] =
        val operation = metrics
          .timed("RateLimitStatusLatency", Map("operation" -> "getStatus"))(
            underlying.getStatus(key, profile),
          )

        applyPatterns(operation, "getStatus")
          .flatTap(_ => healthTracker.recordSuccess).handleErrorWith(error =>
            healthTracker.recordFailure(error) *>
              logger
                .warn(s"Failed to get status for $key, returning None: ${error
                    .getMessage}") *> Temporal[F].pure(None),
          )

      override def healthCheck: F[Either[String, Unit]] = healthTracker
        .isHealthy.flatMap(healthy =>
          if healthy then
            Temporal[F]
              .timeout(underlying.healthCheck, config.timeout.healthCheck)
              .handleError(e => Left(e.getMessage))
          else Temporal[F].pure(Left("circuit breaker open")),
        )

      private def applyPatterns[A](operation: F[A], name: String): F[A] =
        val withTimeout = Temporal[F]
          .timeout(operation, config.timeout.rateLimitCheck)
          .adaptError { case _: java.util.concurrent.TimeoutException =>
            core.GateError.StoreTimeout(name, config.timeout.rateLimitCheck)
          }

        val withRetry = Retry.withPolicy(retryPolicy, name)(withTimeout)

        val withCB = circuitBreaker.fold(withRetry)(cb =>
          cb.protect(withRetry).flatTap(_ =>
            cb.metrics.flatMap(m =>
              metrics.recordCircuitBreakerState(
                "dynamodb-ratelimit",
                m.state.toString,
                m.failureCount,
              ),
            ),
          ),
        )

        bulkhead.fold(withCB)(_.execute(withCB))

      private def handleDegradation(
          key: String,
          error: Throwable,
      ): F[RateLimitDecision] = error match
        case _: CircuitBreakerOpen => logger.warn(
            s"Circuit breaker open for key $key, applying degradation mode",
          ) *> metrics.increment(
            "RateLimitDegraded",
            Map("reason" -> "circuit_breaker"),
          ) *> GracefulDegradation.degradedDecision(
            degradationMode,
            key,
            cache = degradationCache,
            reducedLimitState = Some(reducedLimitRef),
          )

        case _: BulkheadRejected => logger.warn(
            s"Bulkhead rejected for key $key, applying degradation mode",
          ) *>
            metrics
              .increment("RateLimitDegraded", Map("reason" -> "bulkhead")) *>
            GracefulDegradation.degradedDecision(
              degradationMode,
              key,
              cache = degradationCache,
              reducedLimitState = Some(reducedLimitRef),
            )

        case _ => logger.error(error)(
            s"Unexpected error for key $key, applying degradation mode",
          ) *>
            metrics.increment("RateLimitDegraded", Map("reason" -> "error")) *>
            GracefulDegradation.degradedDecision(
              degradationMode,
              key,
              cache = degradationCache,
              reducedLimitState = Some(reducedLimitRef),
            )

      private def recordDecision(
          key: String,
          decision: RateLimitDecision,
      ): F[Unit] = decision match
        case RateLimitDecision.Allowed(tokens, _) => metrics
            .increment("RateLimitAllowed") *> metrics.gauge(
            "TokensRemaining",
            tokens.toDouble,
            Map("key_prefix" -> keyPrefix(key)),
          )
        case RateLimitDecision.Rejected(retryAfter, _) => metrics
            .increment("RateLimitRejected") *> metrics.gauge(
            "RetryAfterSeconds",
            retryAfter.toDouble,
            Map("key_prefix" -> keyPrefix(key)),
          )

      private def keyPrefix(key: String): String = key.split(":").headOption
        .getOrElse("unknown")

      private def isRetryable(error: Throwable): Boolean = error match
        case _: software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputExceededException =>
          true
        case _: software.amazon.awssdk.core.exception.SdkServiceException =>
          true
        case _: java.util.concurrent.TimeoutException => true
        case _: java.io.IOException => true
        case _ => false

/** Immutable builder for creating resilient stores with fluent API.
  */
case class ResilientStoreBuilder[F[_]: Async: Logger](
    underlying: RateLimitStore[F],
    config: ResilienceConfig = ResilientStoreBuilder.defaultConfig,
    metrics: MetricsPublisher[F],
    events: EventPublisher[F],
    degradationMode: GracefulDegradation.DegradationMode =
      GracefulDegradation.DegradationMode.AllowAll,
):
  def withConfig(c: ResilienceConfig): ResilientStoreBuilder[F] =
    copy(config = c)

  def withMetrics(m: MetricsPublisher[F]): ResilientStoreBuilder[F] =
    copy(metrics = m)

  def withEvents(e: EventPublisher[F]): ResilientStoreBuilder[F] =
    copy(events = e)

  def withDegradationMode(
      mode: GracefulDegradation.DegradationMode,
  ): ResilientStoreBuilder[F] = copy(degradationMode = mode)

  def build: Resource[F, RateLimitStore[F]] =
    ResilientRateLimitStore(underlying, config, metrics, events, degradationMode)

object ResilientStoreBuilder:
  def apply[F[_]: Async: Logger](
      store: RateLimitStore[F],
  ): ResilientStoreBuilder[F] = ResilientStoreBuilder[F](
    underlying = store,
    config = defaultConfig,
    metrics = MetricsPublisher.noop[F],
    events = EventPublisher.noop[F],
    degradationMode = GracefulDegradation.DegradationMode.AllowAll,
  )

  private[resilience] def defaultConfig: ResilienceConfig =
    import config.*
    ResilienceConfig(
      circuitBreaker = CircuitBreakerSettings(
        enabled = true,
        dynamodb = config.CircuitBreakerConfig(),
        kinesis = config.CircuitBreakerConfig(),
      ),
      retry = RetrySettings(dynamodb = RetryConfig(), kinesis = RetryConfig()),
      bulkhead = BulkheadSettings(),
      timeout = TimeoutSettings(),
    )
