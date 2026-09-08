package storage

import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import scala.jdk.FutureConverters.*

import org.typelevel.log4cats.Logger

import cats.effect.*
import cats.syntax.all.*
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.*
import core.{
  RateLimitDecision, RateLimitProfile, RateLimitStore, TokenBucket,
  TokenBucketState,
}
import observability.MetricsPublisher
import resilience.{OCCConflictException, Retry, RetryPolicy}
import DynamoDBOps.*

/** DynamoDB implementation of RateLimitStore using token bucket algorithm.
  *
  * Uses optimistic concurrency control (OCC) via version field to prevent race
  * conditions when multiple instances try to update the same key.
  *
  * ==Token-bucket math==
  * Refill is computed on each check using:
  *   - elapsed_sec = (now_ms - lastRefillMs) / 1000
  *   - tokens_to_add = elapsed_sec * refillRatePerSecond
  *   - refilled = min(capacity, current_tokens + tokens_to_add)
  * Burst cap: tokens never exceed capacity.
  *
  * ==Invariants==
  *   - Tokens in [0, capacity] at all times.
  *   - Version increments on every successful write (OCC).
  *   - lastRefillMs is the timestamp at which refill was last applied (set to
  *     now on consume).
  *
  * ==OCC (optimistic concurrency control)==
  * Conditional write: first write uses attribute_not_exists(pk); updates use
  * version = :expectedVersion. Retry policy: RetryPolicy.occRetry (jittered
  * exponential backoff from 1 ms, up to 10 retries) on
  * ConditionalCheckFailedException. High contention: once retries are exhausted
  * the request is rejected (no over-issuing of tokens). Any other failure
  * propagates unchanged so the resilience layer can classify it.
  *
  * Table Schema:
  *   - pk (S): Partition key - "ratelimit#<key>"
  *   - tokens (N): Current token count
  *   - lastRefillMs (N): Timestamp of last refill calculation
  *   - version (N): Version for OCC
  *   - ttl (N): TTL for automatic cleanup
  */
class DynamoDBRateLimitStore[F[_]: Async: Logger](
    client: DynamoDbAsyncClient,
    tableName: String,
    metrics: MetricsPublisher[F],
    retryPolicy: RetryPolicy = RetryPolicy.occRetry,
) extends RateLimitStore[F]:

  private val logger = Logger[F]

  override def checkAndConsume(
      key: String,
      cost: Int,
      profile: RateLimitProfile,
  ): F[RateLimitDecision] = Retry
    .retryWithTracking(retryPolicy, s"OCC-checkAndConsume($key)")(
      singleAttempt(key, cost, profile),
    ).flatMap(r =>
      metrics.gauge("RateLimitOCCAttempts", r.attempts.toDouble).as(r.result),
    ).handleErrorWith {
      case _: OCCConflictException => Clock[F].realTime.map(_.toMillis)
          .map(now =>
            RateLimitDecision.Rejected(1, TokenBucket.resetAt(now, 0, profile)),
          )
      case other => Async[F].raiseError(other)
    }

  /** Single attempt: read state, refill, try consume+write. On OCC conflict
    * (conditionalPut returns false), raise OCCConflictException so Retry
    * handles backoff.
    */
  private def singleAttempt(
      key: String,
      cost: Int,
      profile: RateLimitProfile,
  ): F[RateLimitDecision] =
    for
      now <- Clock[F].realTime.map(_.toMillis)
      current <- getOrInitState(key, profile, now)
      refilled = TokenBucket.refill(current, now, profile)
      decision <- TokenBucket.consume(refilled, cost, now) match
        case Some(newState) =>
          attemptUpdate(key, current.version, newState, profile.ttlSeconds, now)
            .flatMap {
              case true =>
                val resetAt = TokenBucket.resetAt(now, newState.tokens, profile)
                Async[F]
                  .pure(RateLimitDecision.Allowed(newState.tokensInt, resetAt))
              case false => Async[F].raiseError(OCCConflictException(key, 0))
            }
        case None =>
          val retryAfter = TokenBucket
            .retryAfterSeconds(cost, refilled.tokens, profile)
          val resetAt = TokenBucket.resetAt(now, refilled.tokens, profile)
          Async[F].pure(RateLimitDecision.Rejected(retryAfter, resetAt))
    yield decision

  override def getStatus(
      key: String,
      profile: RateLimitProfile,
  ): F[Option[RateLimitDecision.Allowed]] =
    for
      now <- Clock[F].realTime.map(_.toMillis)
      maybeState <- getState(key)
      result <- maybeState match
        case Some(Right(state)) =>
          val refilled = TokenBucket.refill(state, now, profile)
          val resetAt = TokenBucket.resetAt(now, refilled.tokens, profile)
          Async[F]
            .pure(Some(RateLimitDecision.Allowed(refilled.tokensInt, resetAt)))
        case Some(Left(err)) => logger.error(
            s"Corrupt rate-limit state for key=$key: $err — returning no status",
          ) *> metrics.increment("CorruptStateRead").as(None)
        case None => Async[F].pure(None)
    yield result

  override def healthCheck: F[Either[String, Unit]] =
    dynamoHealthCheck(client, tableName)

  private def getOrInitState(
      key: String,
      profile: RateLimitProfile,
      now: Long,
  ): F[TokenBucketState] = getState(key).flatMap {
    case Some(Right(state)) => Async[F].pure(state)
    case Some(Left(err)) =>
      // Corrupt stored state: fail open (grant full capacity), log, and record metric.
      // Prefer over-issuing tokens to blocking legitimate traffic on bad data.
      logger
        .error(s"Corrupt rate-limit state for key=$key: $err — failing open") *>
        metrics.increment("CorruptStateRead") *>
        Async[F].pure(TokenBucketState(profile.capacity.toDouble, now, 0L))
    case None => Async[F]
        .pure(TokenBucketState(profile.capacity.toDouble, now, 0L))
  }

  private def getState(
      key: String,
  ): F[Option[Either[String, TokenBucketState]]] =
    val request = GetItemRequest.builder().tableName(tableName)
      .key(Map("pk" -> attr(s"ratelimit#$key")).asJava).consistentRead(true)
      .build()

    Async[F].fromCompletableFuture(
      Async[F].delay(client.getItem(request).toCompletableFuture),
    ).map(response =>
      if response.hasItem && !response.item().isEmpty then
        Some(parseState(response.item().asScala.toMap))
      else None,
    )

  private def parseState(
      item: Map[String, AttributeValue],
  ): Either[String, TokenBucketState] =
    try
      val tokens = item.get("tokens").toRight("missing 'tokens' attribute")
        .map(_.n().toDouble)
      val lastRefillMs = item.get("lastRefillMs")
        .toRight("missing 'lastRefillMs' attribute").map(_.n().toLong)
      val version = item.get("version").toRight("missing 'version' attribute")
        .map(_.n().toLong)
      for
        t <- tokens
        l <- lastRefillMs
        v <- version
      yield TokenBucketState(t, l, v)
    catch
      case e: NumberFormatException =>
        Left(s"malformed numeric attribute: ${e.getMessage}")

  private def attemptUpdate(
      key: String,
      expectedVersion: Long,
      newState: TokenBucketState,
      ttlSeconds: Long,
      now: Long,
  ): F[Boolean] =
    val ttl = now / 1000 + ttlSeconds

    val item = Map(
      "pk" -> attr(s"ratelimit#$key"),
      "tokens" -> attrND(newState.tokens),
      "lastRefillMs" -> attrN(newState.lastRefillMs),
      "version" -> attrN(newState.version),
      "ttl" -> attrN(ttl),
    )

    val requestBuilder = PutItemRequest.builder().tableName(tableName)
      .item(item.asJava)

    // OCC: conditional write so only one writer wins. First write: attribute_not_exists(pk);
    // subsequent: version = :expectedVersion. Caller retries on ConditionalCheckFailedException
    // (1ms delay, max 10 attempts total); after that we reject (high contention).
    val request =
      if expectedVersion == 0L then
        requestBuilder.conditionExpression("attribute_not_exists(pk)").build()
      else
        requestBuilder.conditionExpression("version = :expectedVersion")
          .expressionAttributeValues(
            Map(":expectedVersion" -> attrN(expectedVersion)).asJava,
          ).build()

    conditionalPut(client, request)

object DynamoDBRateLimitStore:
  def apply[F[_]: Async: Logger](
      client: DynamoDbAsyncClient,
      tableName: String,
      metrics: MetricsPublisher[F],
      retryPolicy: RetryPolicy = RetryPolicy.occRetry,
  ): DynamoDBRateLimitStore[F] =
    new DynamoDBRateLimitStore[F](client, tableName, metrics, retryPolicy)
