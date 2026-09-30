package resilience

import scala.concurrent.duration.*

import org.typelevel.log4cats.Logger

import cats.effect.*
import cats.syntax.all.*
import config.ResilienceConfig
import core.{RateLimitDecision, RateLimitProfile, RateLimitStore}
import observability.MetricsPublisher

/** Wraps the rate-limit store in a bulkhead, a circuit breaker, retries with
  * backoff, and a per-check timeout. When the store cannot answer, the decision
  * follows `degradationMode`.
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

  /** Whether a failed check is worth another attempt: only a transient fault.
    *
    * Every `SdkServiceException` used to be retried, including a 400 such as a
    * validation error that fails the same way every time, while an
    * `SdkClientException` -- how the SDK reports a connection reset or refused
    * -- was not. A service error is now retried when it is throttling or a 5xx,
    * and a client error when it is an attempt timeout or an I/O fault.
    */
  private[resilience] def isRetryable(error: Throwable): Boolean = error match
    case e: java.util.concurrent.CompletionException if e.getCause != null =>
      isRetryable(e.getCause)
    case e: software.amazon.awssdk.core.exception.SdkServiceException =>
      e.isThrottlingException || e.statusCode >= 500
    case _: software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException =>
      true
    case e: software.amazon.awssdk.core.exception.SdkClientException =>
      causedByIo(e)
    case _: java.util.concurrent.TimeoutException => true
    case _: java.io.IOException => true
    case _ => false

  private def causedByIo(e: Throwable): Boolean = Iterator
    .iterate(e.getCause)(_.getCause).takeWhile(_ != null).take(10)
    .exists(_.isInstanceOf[java.io.IOException])

  /** Wrap a rate limit store with production resilience patterns.
    */
  def apply[F[_]: Temporal: Logger](
      underlying: RateLimitStore[F],
      config: ResilienceConfig,
      metrics: MetricsPublisher[F],
      degradationMode: GracefulDegradation.DegradationMode =
        GracefulDegradation.DegradationMode.AllowAll,
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
        wrappedOp.handleErrorWith(handleDegradation(key, _))

      override def getStatus(
          key: String,
          profile: RateLimitProfile,
      ): F[Option[RateLimitDecision.Allowed]] =
        val operation = metrics
          .timed("RateLimitStatusLatency", Map("operation" -> "getStatus"))(
            underlying.getStatus(key, profile),
          )

        // A failure propagates. It used to become None, which the API renders
        // as a key never seen, so a store outage reported every bucket full.
        applyPatterns(operation, "getStatus").onError(error =>
          logger.warn(s"Failed to get status for $key: ${error.getMessage}"),
        )

      // /ready probes the raw store (Main), never this. A health tracker used
      // to answer here from the outcome of recent checks; nothing read it.
      override def healthCheck: F[Either[String, Unit]] = underlying.healthCheck

      private def applyPatterns[A](operation: F[A], name: String): F[A] =
        val withTimeout = Temporal[F]
          .timeout(operation, config.timeout.rateLimitCheck)
          .adaptError { case _: java.util.concurrent.TimeoutException =>
            core.GateError.StoreTimeout(name, config.timeout.rateLimitCheck)
          }

        val withRetry = Retry.withPolicy(retryPolicy, name)(withTimeout)

        // The state is recorded after every call, whatever its outcome. It
        // used to follow successful calls only, and while the breaker is open
        // every call fails, so the gauge and the CloudWatch alarm read closed
        // for the whole outage. A failure to record never replaces the call's
        // own result.
        val withCB = circuitBreaker.fold(withRetry)(cb =>
          Temporal[F].guarantee(
            cb.protect(withRetry),
            cb.metrics.flatMap(m =>
              metrics.recordCircuitBreakerState(
                "dynamodb-ratelimit",
                m.state.toString,
                m.failureCount,
              ),
            ).handleError(_ => ()),
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
          ) *> GracefulDegradation.degradedDecision(degradationMode)

        case _: BulkheadRejected => logger.warn(
            s"Bulkhead rejected for key $key, applying degradation mode",
          ) *>
            metrics
              .increment("RateLimitDegraded", Map("reason" -> "bulkhead")) *>
            GracefulDegradation.degradedDecision(degradationMode)

        case _ => logger.error(error)(
            s"Unexpected error for key $key, applying degradation mode",
          ) *>
            metrics.increment("RateLimitDegraded", Map("reason" -> "error")) *>
            GracefulDegradation.degradedDecision(degradationMode)
