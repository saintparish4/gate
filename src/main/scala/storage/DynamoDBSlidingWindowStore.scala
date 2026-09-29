package storage

import scala.jdk.CollectionConverters.*
import scala.jdk.FutureConverters.*

import org.typelevel.log4cats.Logger

import cats.effect.*
import cats.syntax.all.*
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.*
import core.{
  RateLimitDecision, RateLimitProfile, RateLimitStore, SlidingWindow,
  SlidingWindowState,
}
import observability.MetricsPublisher
import resilience.{OCCConflictException, Retry, RetryPolicy}
import DynamoDBOps.*

/** DynamoDB-backed sliding window rate limit store.
  *
  * All sub-window counts for a key are stored in a **single item** so that OCC
  * operates atomically on the complete window state -- no cross-item races.
  *
  * ==DynamoDB schema==
  * Uses the same table as the token-bucket store; keys are namespaced with
  * "sw#" to avoid collision.
  *
  * pk (S): "sw#{clientKey}" counts (M): map of sub_window_start_ms_string ->
  * count_string version (N): OCC version; starts at 0 (first write uses
  * attribute_not_exists(pk)) ttl (N): epoch-seconds for DynamoDB TTL
  * auto-expiry
  *
  * ==RateLimitProfile reuse==
  * capacity = max requests per parent window ttlSeconds = parent window
  * duration in seconds refillRatePerSecond = unused
  *
  * ==OCC strategy==
  * Mirrors DynamoDBRateLimitStore: RetryPolicy.occRetry (10 retries, 1 ms base,
  * 1.5x backoff, 20% jitter). On exhaustion the request is rejected rather than
  * over-admitted.
  *
  * ==Corrupt-state handling==
  * A corrupt item fails closed and self-heals: it is replaced, conditioned on
  * its raw version, by a full window, so the key refuses until that window
  * passes. Logged and counted as CorruptStateRead.
  */
class DynamoDBSlidingWindowStore[F[_]: Async: Logger](
    client: DynamoDbAsyncClient,
    tableName: String,
    metrics: MetricsPublisher[F],
    subWindowCount: Int = SlidingWindow.DefaultSubWindowCount,
) extends RateLimitStore[F]:

  private val logger = Logger[F]
  private val retryPolicy = RetryPolicy.occRetry

  override def checkAndConsume(
      key: String,
      cost: Int,
      profile: RateLimitProfile,
  ): F[RateLimitDecision] = Retry
    .retryWithTracking(retryPolicy, s"SW-OCC-checkAndConsume($key)")(
      singleAttempt(key, cost, profile),
    ).flatMap(r =>
      metrics.gauge("SlidingWindowOCCAttempts", r.attempts.toDouble).as(r.result),
    ).handleErrorWith {
      // OCC exhausted: reject conservatively rather than over-admit.
      case _: OCCConflictException => Clock[F].realTime.map(_.toMillis)
          .map { nowMs =>
            val windowMs = profile.ttlSeconds * 1000L
            val active = SlidingWindow
              .activeSubWindowStarts(nowMs, windowMs, subWindowCount)
            val reset = SlidingWindow.resetAt(Map.empty, active, windowMs)
            RateLimitDecision
              .Rejected(SlidingWindow.retryAfterSeconds(reset, nowMs), reset)
          }
      // Any other error propagates as itself; see LeakyBucketRateLimitStore.
      case other => Async[F].raiseError(other)
    }

  /** Single OCC attempt: read → compute → conditional write. */
  private def singleAttempt(
      key: String,
      cost: Int,
      profile: RateLimitProfile,
  ): F[RateLimitDecision] =
    val windowMs = profile.ttlSeconds * 1000L
    for
      nowMs <- Clock[F].realTime.map(_.toMillis)
      current <- getOrInitState(key, profile, nowMs, windowMs)
      active = SlidingWindow
        .activeSubWindowStarts(nowMs, windowMs, subWindowCount)
      total = SlidingWindow.totalCount(current.counts, active)
      decision <-
        if total + cost > profile.capacity then
          val reset = SlidingWindow.resetAt(current.counts, active, windowMs)
          Async[F].pure(
            RateLimitDecision
              .Rejected(SlidingWindow.retryAfterSeconds(reset, nowMs), reset),
          )
        else
          val curSw = active.head
          val updated = current.counts
            .updated(curSw, current.counts.getOrElse(curSw, 0L) + cost)
          val pruned = SlidingWindow.pruneStale(updated, active, windowMs)
          val newState = SlidingWindowState(pruned, current.version + 1)
          attemptWrite(key, current.version, newState, profile.ttlSeconds, nowMs)
            .flatMap {
              case true =>
                val newTotal = total + cost
                val reset = SlidingWindow.resetAt(pruned, active, windowMs)
                Async[F].pure(RateLimitDecision.Allowed(
                  SlidingWindow.remaining(profile.capacity, newTotal),
                  reset,
                ))
              case false => Async[F]
                  .raiseError(OCCConflictException(key, current.version.toInt))
            }
    yield decision

  override def getStatus(
      key: String,
      profile: RateLimitProfile,
  ): F[Option[RateLimitDecision.Allowed]] =
    val windowMs = profile.ttlSeconds * 1000L
    for
      nowMs <- Clock[F].realTime.map(_.toMillis)
      item <- getState(key)
      result <- item match
        case Some(Right(state)) =>
          val active = SlidingWindow
            .activeSubWindowStarts(nowMs, windowMs, subWindowCount)
          val total = SlidingWindow.totalCount(state.counts, active)
          val reset = SlidingWindow.resetAt(state.counts, active, windowMs)
          Async[F].pure(Some(
            RateLimitDecision
              .Allowed(SlidingWindow.remaining(profile.capacity, total), reset),
          ))
        // What a check would leave behind: a full window from now.
        case Some(Left(corrupt)) => logger
            .error(s"Corrupt sliding-window state key=$key: ${corrupt
                .detail}; reporting it full") *>
            metrics.increment("CorruptStateRead").as(Some(
              RateLimitDecision
                .Allowed(0, java.time.Instant.ofEpochMilli(nowMs + windowMs)),
            ))
        case None => Async[F].pure(None)
    yield result

  override def healthCheck: F[Either[String, Unit]] =
    dynamoHealthCheck(client, tableName)

  // ---------------------------------------------------------------------------
  // Internal helpers
  // ---------------------------------------------------------------------------

  private def getOrInitState(
      key: String,
      profile: RateLimitProfile,
      nowMs: Long,
      windowMs: Long,
  ): F[SlidingWindowState] = getState(key).flatMap {
    case Some(Right(state)) => Async[F].pure(state)
    case Some(Left(corrupt)) => heal(key, corrupt, profile, nowMs, windowMs) *>
        Async[F].raiseError(OCCConflictException(key, 0))
    case None => Async[F].pure(SlidingWindowState(Map.empty, 0L))
  }

  // Corrupt state fails closed and self-heals: the item is replaced by a full
  // window (the whole capacity in the current sub-window, so it frees one
  // window from now), conditioned on the raw version read, and the attempt
  // retries through the normal path. It used to "fail open" with an
  // attribute_not_exists write that always failed against the existing item,
  // so the key burned its retries and stayed blocked until TTL.
  private def heal(
      key: String,
      corrupt: CorruptItem,
      profile: RateLimitProfile,
      nowMs: Long,
      windowMs: Long,
  ): F[Unit] =
    val current = SlidingWindow
      .activeSubWindowStarts(nowMs, windowMs, subWindowCount).head
    val full = SlidingWindowState(
      Map(current -> profile.capacity.toLong),
      corrupt.nextVersion,
    )
    logger.error(s"Corrupt sliding-window state key=$key: ${corrupt
        .detail}; replacing it with a full window") *>
      metrics.increment("CorruptStateRead") *> replaceCorrupt(
        client,
        tableName,
        corrupt,
        item(key, full, profile.ttlSeconds, nowMs),
      ).flatMap(healed =>
        if healed then metrics.increment("CorruptStateHealed")
        else Async[F].unit,
      )

  private def item(
      key: String,
      state: SlidingWindowState,
      ttlSeconds: Long,
      nowMs: Long,
  ): Map[String, AttributeValue] =
    val countsMap = state.counts.map { case (sw, count) =>
      sw.toString -> attrN(count)
    }.asJava
    Map(
      "pk" -> attr(s"sw#$key"),
      "counts" -> AttributeValue.builder().m(countsMap).build(),
      "version" -> attrN(state.version),
      // From the newest count, not this task's clock: a trailing task must not
      // set an expiry that falls before a leading task's counts expire.
      "ttl" ->
        attrN(state.counts.keys.foldLeft(nowMs)(_ max _) / 1000 + ttlSeconds),
    )

  private def getState(
      key: String,
  ): F[Option[Either[CorruptItem, SlidingWindowState]]] =
    val request = GetItemRequest.builder().tableName(tableName)
      .key(Map("pk" -> attr(s"sw#$key")).asJava).consistentRead(true).build()

    Async[F].fromCompletableFuture(
      Async[F].delay(client.getItem(request).toCompletableFuture),
    ).map(response =>
      if response.hasItem && !response.item().isEmpty then
        val item = response.item().asScala.toMap
        Some(parseState(item).left.map(CorruptItem.of(item, _)))
      else None,
    )

  /** Parses a DynamoDB item into SlidingWindowState. Returns Left(reason) on
    * any missing/malformed attribute so callers can log and metric the error
    * without propagating exceptions.
    */
  private def parseState(
      item: Map[String, AttributeValue],
  ): Either[String, SlidingWindowState] =
    for
      version <- longAttr(item, "version")
      raw <- item.get("counts").toRight("missing 'counts' attribute")
      entries <-
        if !raw.hasM then Left("'counts' is not a Map attribute")
        else
          raw.m().asScala.toList.traverse { case (sw, count) =>
            for
              start <- sw.toLongOption
                .toRight(s"'counts' key is not a whole number: $sw")
              c <- longAttr(Map("count" -> count), "count")
            yield start -> c
          }
    yield SlidingWindowState(entries.toMap, version)

  private def attemptWrite(
      key: String,
      expectedVersion: Long,
      newState: SlidingWindowState,
      ttlSeconds: Long,
      nowMs: Long,
  ): F[Boolean] =
    val builder = PutItemRequest.builder().tableName(tableName)
      .item(item(key, newState, ttlSeconds, nowMs).asJava)
    // First write: attribute_not_exists guard; subsequent: version matches.
    val request =
      if expectedVersion == 0L then
        builder.conditionExpression("attribute_not_exists(pk)").build()
      else
        builder.conditionExpression("version = :expected")
          .expressionAttributeValues(
            Map(":expected" -> attrN(expectedVersion)).asJava,
          ).build()

    conditionalPut(client, request)

object DynamoDBSlidingWindowStore:
  def apply[F[_]: Async: Logger](
      client: DynamoDbAsyncClient,
      tableName: String,
      metrics: MetricsPublisher[F],
      subWindowCount: Int = SlidingWindow.DefaultSubWindowCount,
  ): DynamoDBSlidingWindowStore[F] =
    new DynamoDBSlidingWindowStore[F](client, tableName, metrics, subWindowCount)
