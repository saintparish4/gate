package storage

import java.time.Instant

import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import scala.jdk.FutureConverters.*

import org.typelevel.log4cats.Logger

import cats.effect.*
import cats.syntax.all.*
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.*
import core.{
  LeakyBucket, LeakyBucketState, RateLimitDecision, RateLimitProfile,
  RateLimitStore,
}
import observability.MetricsPublisher
import resilience.{OCCConflictException, Retry, RetryPolicy}
import DynamoDBOps.*

/** DynamoDB implementation of RateLimitStore using leaky bucket algorithm.
  *
  * State per key: level (current "water"), lastLeakMs, version. The leak and
  * pour are [[core.LeakyBucket]], which clamps clock corrections. Reject with
  * resetAt = now + (newLevel + cost - capacity) / leakRatePerSecond seconds.
  *
  * Profile: Reuses RateLimitProfile — capacity = bucket size,
  * refillRatePerSecond = leak rate. DynamoDB: Same schema as token bucket (pk,
  * tokens→level, lastRefillMs→lastLeakMs, version, ttl); OCC.
  */
class LeakyBucketRateLimitStore[F[_]: Async: Logger](
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
    .retryWithTracking(retryPolicy, s"OCC-leaky($key)")(
      singleAttempt(key, cost, profile),
    ).map(_.result).handleErrorWith {
      case _: OCCConflictException => Clock[F].realTime.map(_.toMillis)
          .map { now =>
            val secToAllow = cost.toDouble / profile.refillRatePerSecond
            val resetAt = Instant.ofEpochMilli(now + (secToAllow * 1000).toLong)
            RateLimitDecision.Rejected(secToAllow.ceil.toInt.max(1), resetAt)
          }
      // A lambda of one case is total: any other error became a MatchError,
      // which the resilience wrapper does not retry. The token bucket had the
      // same bug.
      case other => Async[F].raiseError(other)
    }

  /** Single attempt: one read-compute-write cycle. On OCC conflict
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
      currentState <- getOrInitState(key, profile, now)
      leaked = LeakyBucket.leak(currentState, now, profile)
      newLevel = leaked.level
      decision <- LeakyBucket.pour(leaked, cost, now, profile) match
        case Some(newState) =>
          val updatedLevel = newState.level
          attemptUpdate(
            key,
            currentState.version,
            newState,
            profile.ttlSeconds,
            now,
          ).flatMap {
            case true =>
              val resetAt = Instant.ofEpochMilli(
                now + (updatedLevel / profile.refillRatePerSecond * 1000).toLong,
              )
              Async[F].pure(
                RateLimitDecision
                  .Allowed((profile.capacity - updatedLevel).toInt, resetAt),
              )
            case false => Async[F].raiseError(OCCConflictException(key, 0))
          }
        case None =>
          val secToAllow = (newLevel + cost - profile.capacity) /
            profile.refillRatePerSecond
          val resetAt = Instant.ofEpochMilli(now + (secToAllow * 1000).toLong)
          Async[F].pure(
            RateLimitDecision.Rejected(secToAllow.ceil.toInt.max(1), resetAt),
          )
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
          val newLevel = LeakyBucket.leak(state, now, profile).level
          val remaining = (profile.capacity - newLevel).toInt
          val resetAt = Instant.ofEpochMilli(
            now + (newLevel / profile.refillRatePerSecond * 1000).toLong,
          )
          Async[F].pure(Some(RateLimitDecision.Allowed(remaining, resetAt)))
        // What a check would leave behind: a full bucket draining from now.
        case Some(Left(corrupt)) => logger
            .error(s"Corrupt rate-limit state for key=$key: ${corrupt
                .detail}; reporting it full") *>
            metrics.increment("CorruptStateRead")
              .as(Some(RateLimitDecision.Allowed(
                0,
                Instant.ofEpochMilli(
                  now + (profile.capacity / profile.refillRatePerSecond * 1000)
                    .toLong,
                ),
              )))
        case None => Async[F].pure(None)
    yield result

  override def healthCheck: F[Either[String, Unit]] =
    dynamoHealthCheck(client, tableName)

  private def getOrInitState(
      key: String,
      profile: RateLimitProfile,
      now: Long,
  ): F[LeakyBucketState] = getState(key).flatMap {
    case Some(Right(state)) => Async[F].pure(state)
    case Some(Left(corrupt)) => heal(key, corrupt, profile, now) *>
        Async[F].raiseError(OCCConflictException(key, 0))
    case None => Async[F].pure(LeakyBucketState(0.0, now, 0L))
  }

  // Corrupt state fails closed and self-heals: the item is replaced by a full
  // bucket draining from now, conditioned on the raw version read, and the
  // attempt retries through the normal path. It used to "fail open" with an
  // attribute_not_exists write that always failed against the existing item,
  // so the key burned its retries and stayed blocked until TTL.
  private def heal(
      key: String,
      corrupt: CorruptItem,
      profile: RateLimitProfile,
      now: Long,
  ): F[Unit] =
    val full =
      LeakyBucketState(profile.capacity.toDouble, now, corrupt.nextVersion)
    logger.error(s"Corrupt rate-limit state for key=$key: ${corrupt
        .detail}; replacing it with a full bucket") *>
      metrics.increment("CorruptStateRead") *> replaceCorrupt(
        client,
        tableName,
        corrupt,
        item(key, full, profile.ttlSeconds, now),
      ).flatMap(healed =>
        if healed then metrics.increment("CorruptStateHealed")
        else Async[F].unit,
      )

  private def item(
      key: String,
      state: LeakyBucketState,
      ttlSeconds: Long,
      now: Long,
  ): Map[String, AttributeValue] = Map(
    "pk" -> attr(s"ratelimit#$key"),
    "tokens" -> attrND(state.level),
    "lastRefillMs" -> attrN(state.lastLeakMs),
    "version" -> attrN(state.version),
    "ttl" -> attrN(now / 1000 + ttlSeconds),
  )

  private def getState(
      key: String,
  ): F[Option[Either[CorruptItem, LeakyBucketState]]] =
    val request = GetItemRequest.builder().tableName(tableName)
      .key(Map("pk" -> attr(s"ratelimit#$key")).asJava).consistentRead(true)
      .build()
    Async[F].fromCompletableFuture(
      Async[F].delay(client.getItem(request).toCompletableFuture),
    ).map(response =>
      if response.hasItem && !response.item().isEmpty then
        val item = response.item().asScala.toMap
        Some(parseState(item).left.map(CorruptItem.of(item, _)))
      else None,
    )

  private def parseState(
      item: Map[String, AttributeValue],
  ): Either[String, LeakyBucketState] =
    for
      l <- doubleAttr(item, "tokens")
      lm <- longAttr(item, "lastRefillMs")
      v <- longAttr(item, "version")
    yield LeakyBucketState(l, lm, v)

  private def attemptUpdate(
      key: String,
      expectedVersion: Long,
      newState: LeakyBucketState,
      ttlSeconds: Long,
      now: Long,
  ): F[Boolean] =
    val requestBuilder = PutItemRequest.builder().tableName(tableName)
      .item(item(key, newState, ttlSeconds, now).asJava)
    val request =
      if expectedVersion == 0L then
        requestBuilder.conditionExpression("attribute_not_exists(pk)").build()
      else
        requestBuilder.conditionExpression("version = :expectedVersion")
          .expressionAttributeValues(
            Map(":expectedVersion" -> attrN(expectedVersion)).asJava,
          ).build()

    conditionalPut(client, request)

object LeakyBucketRateLimitStore:
  def apply[F[_]: Async: Logger](
      client: DynamoDbAsyncClient,
      tableName: String,
      metrics: MetricsPublisher[F],
      retryPolicy: RetryPolicy = RetryPolicy.occRetry,
  ): LeakyBucketRateLimitStore[F] =
    new LeakyBucketRateLimitStore[F](client, tableName, metrics, retryPolicy)
