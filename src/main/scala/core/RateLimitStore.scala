package core

import java.time.Instant

import scala.concurrent.duration.FiniteDuration

import cats.effect.Temporal

/** Result of a rate limit check.
  */
sealed trait RateLimitDecision:
  def allowed: Boolean

  /** True when no store produced this answer: the breaker was open, the
    * bulkhead was full, or the call failed, and the degradation mode answered
    * instead. A degraded rejection used to look like an empty bucket, and a
    * degraded admission like a real one.
    */
  def degraded: Boolean

object RateLimitDecision:
  /** Request is allowed within the rate limit.
    *
    * @param tokensRemaining
    *   Number of tokens remaining in the bucket
    * @param resetAt
    *   When the bucket will be fully replenished
    */
  case class Allowed(
      tokensRemaining: Int,
      resetAt: Instant,
      degraded: Boolean = false,
  ) extends RateLimitDecision:
    val allowed: Boolean = true

  /** Request is rejected due to rate limit exceeded.
    *
    * @param retryAfterSeconds
    *   Suggested retry delay
    * @param resetAt
    *   When the bucket will be fully replenished
    */
  case class Rejected(
      retryAfterSeconds: Int,
      resetAt: Instant,
      degraded: Boolean = false,
  ) extends RateLimitDecision:
    val allowed: Boolean = false

/** Configuration for a rate limit profile.
  *
  * @param capacity
  *   Maximum tokens in the bucket
  * @param refillRatePerSecond
  *   Tokens added per second
  * @param ttlSeconds
  *   Time-to-live for the bucket state
  */
case class RateLimitProfile(
    capacity: Int,
    refillRatePerSecond: Double,
    ttlSeconds: Long,
):
  require(capacity > 0, "Capacity must be positive")
  require(refillRatePerSecond > 0, "Refill rate must be positive")
  require(ttlSeconds > 0, "TTL must be positive")

object RateLimitProfile:
  val default: RateLimitProfile = RateLimitProfile(
    capacity = 100,
    refillRatePerSecond = 10.0,
    ttlSeconds = 3600,
  )

/** Trait for rate limit storage backends.
  *
  * Implementations must provide atomic check-and-decrement semantics to prevent
  * race conditions under concurrent load.
  *
  * ==Token-bucket algorithm (contract for implementations)==
  * Refill formulas:
  *   - elapsed_sec = (now_ms - lastRefillMs) / 1000
  *   - tokens_to_add = elapsed_sec * refillRatePerSecond
  *   - refilled = min(capacity, current_tokens + tokens_to_add)
  * Invariants: tokens in [0, capacity]; version increments on every successful
  * write.
  *
  * @tparam F
  *   the effect type
  */
trait RateLimitStore[F[_]]:
  /** Check if a request is allowed and atomically consume tokens if so.
    *
    * This operation must be atomic - either the tokens are consumed and the
    * request is allowed, or no tokens are consumed and the request is rejected.
    * Race conditions must be handled via optimistic concurrency control or
    * similar mechanisms.
    *
    * @param key
    *   Unique identifier for the rate limit bucket (e.g., "user:123")
    * @param cost
    *   Number of tokens to consume (default 1)
    * @param profile
    *   Rate limit configuration to apply
    * @return
    *   Decision indicating whether request is allowed
    */
  def checkAndConsume(
      key: String,
      cost: Int,
      profile: RateLimitProfile,
  ): F[RateLimitDecision]

  /** Get current token count for a key without consuming.
    *
    * Used for status endpoints and monitoring. This is a read-only operation
    * that does not modify state.
    *
    * @param key
    *   Unique identifier for the rate limit bucket
    * @param profile
    *   Rate limit configuration (needed for refill calculation)
    * @return
    *   Current bucket status, or None if bucket doesn't exist
    */
  def getStatus(
      key: String,
      profile: RateLimitProfile,
  ): F[Option[RateLimitDecision.Allowed]]

  /** Health check for the storage backend.
    *
    * @return
    *   `Right(())` if healthy; `Left(reason)` with a human-readable description
    *   of the failure. Never raises an exception — failures are encoded in the
    *   return type.
    */
  def healthCheck: F[Either[String, Unit]]

/** Companion object with utility methods.
  */
object RateLimitStore:
  /** Create an in-memory store for testing.
    */
  def inMemory[F[_]: Temporal]: F[RateLimitStore[F]] =
    import cats.effect.Ref
    import cats.syntax.all.*

    Ref.of[F, Map[String, TokenBucketState]](Map.empty).map { stateRef =>
      new RateLimitStore[F]:
        override def checkAndConsume(
            key: String,
            cost: Int,
            profile: RateLimitProfile,
        ): F[RateLimitDecision] =
          for
            now <- cats.effect.Clock[F].realTime.map(_.toMillis)
            decision <- stateRef.modify { buckets =>
              val current = buckets.getOrElse(
                key,
                TokenBucketState(profile.capacity.toDouble, now, 0L),
              )
              val refilled = TokenBucket.refill(current, now, profile)

              TokenBucket.consume(refilled, cost, now) match
                case Some(newState) =>
                  val resetAt = TokenBucket
                    .resetAt(now, newState.tokens, profile)
                  (
                    buckets + (key -> newState),
                    RateLimitDecision.Allowed(newState.tokensInt, resetAt),
                  )
                case None =>
                  val retryAfter = TokenBucket
                    .retryAfterSeconds(cost, refilled.tokens, profile)
                  val resetAt = TokenBucket
                    .resetAt(now, refilled.tokens, profile)
                  (buckets, RateLimitDecision.Rejected(retryAfter, resetAt))
            }
          yield decision

        override def getStatus(
            key: String,
            profile: RateLimitProfile,
        ): F[Option[RateLimitDecision.Allowed]] =
          for
            now <- cats.effect.Clock[F].realTime.map(_.toMillis)
            status <- stateRef.get.map(buckets =>
              buckets.get(key).map { current =>
                val refilled = TokenBucket.refill(current, now, profile)
                val resetAt = TokenBucket.resetAt(now, refilled.tokens, profile)
                RateLimitDecision.Allowed(refilled.tokensInt, resetAt)
              },
            )
          yield status

        override def healthCheck: F[Either[String, Unit]] = Temporal[F]
          .pure(Right(()))
    }
