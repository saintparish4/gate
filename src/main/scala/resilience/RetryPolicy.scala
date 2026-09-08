package resilience

import scala.concurrent.duration.*
import scala.util.Random

import org.typelevel.log4cats.Logger

import cats.effect.*
import cats.syntax.all.*

/** Raised when an optimistic concurrency control (OCC) write fails after
  * exhausting retries (e.g. DynamoDB conditional write conflict). Callers
  * typically map this to HTTP 409 Conflict.
  */
case class OCCConflictException(key: String, attempt: Int)
    extends RuntimeException(s"OCC conflict on key=$key at attempt $attempt")

/** Retry policy configuration with exponential backoff and jitter.
  *
  * The delay between retries follows: baseDelay * (multiplier ^ attempt) +
  * jitter
  *
  * @param maxRetries
  *   Maximum number of retry attempts
  * @param baseDelay
  *   Initial delay between retries
  * @param maxDelay
  *   Maximum delay cap
  * @param multiplier
  *   Exponential backoff multiplier
  * @param jitterFactor
  *   Randomization factor (0.0 to 1.0)
  * @param retryOn
  *   Predicate to determine if an error should be retried
  */
case class RetryPolicy(
    maxRetries: Int = 3,
    baseDelay: FiniteDuration = 100.millis,
    maxDelay: FiniteDuration = 10.seconds,
    multiplier: Double = 2.0,
    jitterFactor: Double = 0.1,
    retryOn: Throwable => Boolean = {
      case _: core.GateError.Retryable => true
      case _ => false
    },
):
  require(maxRetries >= 0, "maxRetries must be non-negative")
  require(baseDelay > Duration.Zero, "baseDelay must be positive")
  require(maxDelay >= baseDelay, "maxDelay must be >= baseDelay")
  require(multiplier >= 1.0, "multiplier must be >= 1.0")
  require(
    jitterFactor >= 0.0 && jitterFactor <= 1.0,
    "jitterFactor must be in [0.0, 1.0]",
  )

object RetryPolicy:
  /** Default policy for transient failures */
  val default: RetryPolicy = RetryPolicy()

  /** Aggressive retry for critical operations */
  val aggressive: RetryPolicy = RetryPolicy(
    maxRetries = 5,
    baseDelay = 50.millis,
    maxDelay = 5.seconds,
    multiplier = 1.5,
  )

  /** Conservative retry for expensive operations */
  val conservative: RetryPolicy = RetryPolicy(
    maxRetries = 2,
    baseDelay = 500.millis,
    maxDelay = 30.seconds,
    multiplier = 3.0,
  )

  /** Retry policy specifically for DynamoDB throttling */
  val dynamoDB: RetryPolicy = RetryPolicy(
    maxRetries = 10,
    baseDelay = 25.millis,
    maxDelay = 3.seconds,
    multiplier = 2.0,
    jitterFactor = 0.2,
    retryOn = {
      case _: core.GateError.Retryable => true
      case _: software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputExceededException =>
        true
      case _: software.amazon.awssdk.core.exception.SdkServiceException => true
      case _ => false
    },
  )

  /** Retry policy for OCC (optimistic concurrency control) conflicts. Retries
    * on GateError.Retryable and on OCCConflictException so that
    * DynamoDBRateLimitStore / LeakyBucketRateLimitStore (which still throw
    * OCCConflictException) get OCC retries.
    */
  val occRetry: RetryPolicy = RetryPolicy(
    maxRetries = 10,
    baseDelay = 1.millis,
    maxDelay = 50.millis,
    multiplier = 1.5,
    jitterFactor = 0.2,
    retryOn = {
      case _: core.GateError.Retryable => true
      case _: OCCConflictException => true
      case _ => false
    },
  )

  /** Retry policy for Kinesis throttling */
  val kinesis: RetryPolicy = RetryPolicy(
    maxRetries = 3,
    baseDelay = 100.millis,
    maxDelay = 5.seconds,
    multiplier = 2.0,
    jitterFactor = 0.15,
    retryOn = {
      case _: core.GateError.Retryable => true
      case _: software.amazon.awssdk.services.kinesis.model.ProvisionedThroughputExceededException =>
        true
      case _: software.amazon.awssdk.core.exception.SdkServiceException => true
      case _ => false
    },
  )

/** Retry execution result tracking.
  */
case class RetryResult[A](result: A, attempts: Int, totalDelay: FiniteDuration)

/** Retry utilities for executing operations with exponential backoff.
  */
object Retry:

  /** Execute an effect with retries according to the given policy.
    *
    * @param policy
    *   Retry configuration
    * @param operationName
    *   Name for logging purposes
    * @param fa
    *   The effect to retry
    * @return
    *   The result if successful, or the last error if all retries fail
    */
  def withPolicy[F[_]: Temporal: Logger, A](
      policy: RetryPolicy,
      operationName: String,
  )(fa: F[A]): F[A] = retryWithTracking(policy, operationName)(fa).map(_.result)

  /** Execute with retries and return tracking information.
    */
  def retryWithTracking[F[_]: Temporal: Logger, A](
      policy: RetryPolicy,
      operationName: String,
  )(fa: F[A]): F[RetryResult[A]] =
    val logger = Logger[F]

    def attempt(
        attemptsRemaining: Int,
        attemptNumber: Int,
        totalDelay: FiniteDuration,
    ): F[RetryResult[A]] = fa.attempt.flatMap {
      case Right(result) =>
        if attemptNumber > 1 then
          logger.info(s"$operationName succeeded after $attemptNumber attempts (total delay: $totalDelay)")
        else Temporal[F].unit
        Temporal[F].pure(RetryResult(result, attemptNumber, totalDelay))

      case Left(error) if attemptsRemaining > 0 && policy.retryOn(error) =>
        val delay = calculateDelay(policy, attemptNumber)
        logger.warn(s"$operationName failed (attempt $attemptNumber/${policy
            .maxRetries +
            1}), " + s"retrying in $delay: ${error.getMessage}") *>
          Temporal[F].sleep(delay) *>
          attempt(attemptsRemaining - 1, attemptNumber + 1, totalDelay + delay)

      case Left(error) =>
        if attemptNumber > 1 then
          logger.error(error)(s"$operationName failed after $attemptNumber attempts (total delay: $totalDelay)")
        Temporal[F].raiseError(error)
    }

    attempt(policy.maxRetries, 1, Duration.Zero)

  /** Calculate delay for a given attempt with exponential backoff and jitter.
    * Uses numeric capping in millis to avoid overflow when attempt is large
    * (e.g. baseDelay * multiplier^attempt can exceed FiniteDuration range).
    */
  private[resilience] def calculateDelay(
      policy: RetryPolicy,
      attempt: Int,
  ): FiniteDuration =
    val baseMs = policy.baseDelay.toMillis.toDouble
    val maxMs = policy.maxDelay.toMillis.toDouble
    val rawMs = baseMs * math.pow(policy.multiplier, (attempt - 1).max(0))
    val cappedMs = rawMs.min(maxMs).min(Long.MaxValue.toDouble / 2)
    val cappedDelay = cappedMs.toLong.millis

    // Add jitter
    val jitter: FiniteDuration =
      if policy.jitterFactor > 0 then
        val jitterRange = cappedDelay.toMillis * policy.jitterFactor
        val randomJitter = (Random.nextDouble() * 2 - 1) * jitterRange
        randomJitter.millis
      else 0.millis

    val totalMs = (cappedDelay.toMillis.toDouble + jitter.toMillis)
      .min(Long.MaxValue.toDouble / 2)
    totalMs.toLong.millis

  /** Convenience method for simple retries without custom policy.
    */
  def apply[F[_]: Temporal: Logger, A](maxRetries: Int, operationName: String)(
      fa: F[A],
  ): F[A] = withPolicy(RetryPolicy(maxRetries = maxRetries), operationName)(fa)

  /** Retry with a predicate for retryable errors.
    */
  def retryIf[F[_]: Temporal: Logger, A](
      maxRetries: Int,
      operationName: String,
      shouldRetry: Throwable => Boolean,
  )(fa: F[A]): F[A] = withPolicy(
    RetryPolicy(maxRetries = maxRetries, retryOn = shouldRetry),
    operationName,
  )(fa)

/** Extension methods for adding retry behavior to effects.
  */
extension [F[_]: Temporal: Logger, A](fa: F[A])
  def retryWithBackoff(policy: RetryPolicy, operationName: String): F[A] = Retry
    .withPolicy(policy, operationName)(fa)

  def retryN(maxRetries: Int, operationName: String): F[A] = Retry
    .apply(maxRetries, operationName)(fa)
