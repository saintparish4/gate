package api

import java.security.MessageDigest
import java.time.Instant
import java.util.UUID

import org.http4s.*
import org.http4s.circe.*
import org.http4s.circe.CirceEntityDecoder.*
import org.http4s.circe.CirceEntityEncoder.*
import org.http4s.dsl.Http4sDsl
import org.typelevel.log4cats.Logger
import org.typelevel.otel4s.trace.Tracer

import cats.effect.*
import cats.effect.syntax.spawn.*
import cats.syntax.all.*
import io.circe.generic.auto.*
import io.circe.syntax.*
import config.IdempotencyConfig
import core.*
import events.*
import observability.{MetricsPublisher, TracingMiddleware}
import security.*

/** Idempotency API endpoints.
  *
  * Provides first-writer-wins idempotency for distributed operations.
  */
class IdempotencyApi[F[_]: Async: Tracer](
    store: IdempotencyStore[F],
    idempotencyConfig: IdempotencyConfig,
    eventPublisher: EventPublisher[F],
    metricsPublisher: MetricsPublisher[F],
    logger: Logger[F],
    getRequestId: () => F[String],
) extends Http4sDsl[F]:

  /** POST /v1/idempotency/check
    *
    * Check if an operation with this idempotency key has been processed before.
    */
  def check(request: Request[F], client: AuthenticatedClient): F[Response[F]] =
    (for
      startTime <- Clock[F].realTime.map(_.toMillis)
      checkReq <- request.as[IdempotencyCheckRequest]
      response <- checkReq.ttl.filter(_ <= 0) match
        // It used to be accepted, and wrote a record that was already expired.
        case Some(ttl) => BadRequest(
            ApiError
              .body(ApiError.ValidationError, s"ttl must be positive, got $ttl"),
          )
        case None => runCheck(checkReq, client, startTime)
    yield response).handleErrorWith(storageFailure("check"))

  private def runCheck(
      checkReq: IdempotencyCheckRequest,
      client: AuthenticatedClient,
      startTime: Long,
  ): F[Response[F]] =
    val requestedTtl = checkReq.ttl
      .getOrElse(idempotencyConfig.defaultTtlSeconds)
    val ttlSeconds = math.min(requestedTtl, idempotencyConfig.maxTtlSeconds)
    for
      _ <-
        if requestedTtl > idempotencyConfig.maxTtlSeconds then
          logger.warn(
            s"Idempotency TTL capped: requested=$requestedTtl, max=${idempotencyConfig
                .maxTtlSeconds}, key=${checkReq.idempotencyKey}",
          )
        else ().pure[F]

      requestHash = checkReq.requestBody.map(sha256)

      _ <- logger.debug(s"Idempotency check: key=${checkReq
          .idempotencyKey}, client=${client.clientId}, hasHash=${requestHash
          .isDefined}")
      scoped <- TracingMiddleware.traced("executeIdempotent")(
        metricsPublisher.timed(
          "IdempotencyStoreLatency",
          Map("operation" -> "idempotency_check"),
        )(store.check(
          TenantKey(client.clientId, checkReq.idempotencyKey),
          client.clientId,
          ttlSeconds,
          requestHash,
        )),
      )
      // The store saw the scoped key; the caller and the events see theirs.
      result = IdempotencyApi.withKey(scoped, checkReq.idempotencyKey)

      // Record metrics
      latency <- Clock[F].realTime.map(_.toMillis - startTime)
      _ <- metricsPublisher.recordLatency("idempotency_check", latency.toDouble)
      _ <- countCheck(resultLabel(result))

      // Publish event (fire and forget)
      now <- Clock[F].realTime.map(d => Instant.ofEpochMilli(d.toMillis))
      traceId <- currentTraceId
      _ <- publishEvent(result, client, ttlSeconds, now, traceId).start

      // Build response
      response <- buildCheckResponse(result)
    yield response

  /** POST /v1/idempotency/:key/complete
    *
    * Store the response for a completed idempotent operation.
    */
  def complete(
      key: String,
      request: Request[F],
      client: AuthenticatedClient,
  ): F[Response[F]] = (for
    completeReq <- request.as[IdempotencyCompleteRequest]
    now <- Clock[F].realTime.map(d => Instant.ofEpochMilli(d.toMillis))

    storedResponse = StoredResponse(
      statusCode = completeReq.statusCode,
      body = completeReq.body,
      headers = completeReq.headers.getOrElse(Map.empty),
      completedAt = now,
    )
    storedBytes = IdempotencyApi.storedSize(storedResponse)

    _ <- logger
      .debug(s"Completing idempotency key: $key, client=${client.clientId}")
    response <-
      if storedBytes > IdempotencyApi.MaxStoredResponseBytes then
        PayloadTooLarge(ApiError.body(
          "response_too_large",
          s"The response to store is $storedBytes bytes encoded; the limit is ${IdempotencyApi
              .MaxStoredResponseBytes}. The key stays pending: store a smaller response, such as a reference to the result, or mark the key failed.",
        ))
      else
        store.storeResponse(
          TenantKey(client.clientId, key),
          client.clientId,
          storedResponse,
        ).flatMap(success =>
          if success then
            Ok(
              IdempotencyUpdateResponse(
                idempotencyKey = key,
                status = "completed",
              ).asJson,
            )
          else
            // "conflict", as fail's 409 says: this said "failed", which is
            // also what a successful fail answers.
            Conflict(
              IdempotencyUpdateResponse(
                idempotencyKey = key,
                status = "conflict",
                message =
                  Some("Could not store response - key may not exist or is not pending"),
                error = Some(IdempotencyApi.NotPendingCode),
              ).asJson,
            ),
        )
  yield response).handleErrorWith(storageFailure("complete"))

  /** POST /v1/idempotency/:key/fail
    *
    * Release a pending key after its operation failed without effect, so the
    * next check with the key claims it again and the caller can retry. Without
    * this route a failed operation left its key Pending until TTL, and every
    * retry in between answered in_progress. Only the client that claimed the
    * key can fail it, and only while it is Pending: a completed operation ran,
    * so reopening it would let a retry run it twice.
    */
  def fail(key: String, client: AuthenticatedClient): F[Response[F]] = (for
    _ <- logger
      .debug(s"Failing idempotency key: $key, client=${client.clientId}")
    marked <- store.markFailed(TenantKey(client.clientId, key), client.clientId)
    response <-
      if marked then
        Ok(
          IdempotencyUpdateResponse(idempotencyKey = key, status = "failed")
            .asJson,
        )
      else
        Conflict(
          IdempotencyUpdateResponse(
            idempotencyKey = key,
            status = "conflict",
            message =
              Some("Could not mark failed - key may not exist or is not pending"),
            error = Some(IdempotencyApi.NotPendingCode),
          ).asJson,
        )
  yield response).handleErrorWith(storageFailure("fail"))

  /** Corrupt records and unexpected store failures both become a structured 503
    * so the caller knows not to proceed; body decoding failures pass through so
    * http4s can answer 4xx as usual.
    */
  private def storageFailure(op: String): Throwable => F[Response[F]] =
    case e: CorruptIdempotencyRecordException => logger
        .error(e)(s"Corrupt idempotency record for key=${e.key}") *>
        ServiceUnavailable(ApiError.body(
          "storage_corruption",
          s"Idempotency record corrupted: ${e.detail}",
        ))
    case e: MessageFailure => Async[F].raiseError(e)
    case e => logger.error(e)(s"Idempotency $op failed") *>
        (if op == "check" then countCheck("error") else Async[F].unit) *>
        ServiceUnavailable(ApiError.body(
          ApiError.StorageUnavailable,
          s"Idempotency store failed during $op; retry the request",
        ))

  private def countCheck(result: String): F[Unit] = metricsPublisher
    .increment("IdempotencyCheck", Map("result" -> result))

  private def resultLabel(result: IdempotencyResult): String = result match
    case _: IdempotencyResult.New => "new"
    case _: IdempotencyResult.InProgress => "in_progress"
    case _: IdempotencyResult.Duplicate => "duplicate"
    case _: IdempotencyResult.KeyConflict => "conflict"

  private def buildCheckResponse(result: IdempotencyResult): F[Response[F]] =
    result match
      case IdempotencyResult.New(key, _, _) => Ok(
          IdempotencyCheckResponse(
            status = "new",
            idempotencyKey = key,
            originalResponse = None,
          ).asJson,
        )

      case IdempotencyResult.Duplicate(key, response, firstSeenAt) => Ok(
          IdempotencyCheckResponse(
            status = "duplicate",
            idempotencyKey = key,
            originalResponse = response.map(r =>
              OriginalResponse(
                statusCode = r.statusCode,
                body = r.body,
                headers = r.headers,
              ),
            ),
            firstSeenAt = Some(firstSeenAt.toString),
          ).asJson,
        )

      case IdempotencyResult.InProgress(key, startedAt) => Accepted(
          IdempotencyCheckResponse(
            status = "in_progress",
            idempotencyKey = key,
            originalResponse = None,
            firstSeenAt = Some(startedAt.toString),
            message = Some("Operation is currently being processed"),
          ).asJson,
        )

      case IdempotencyResult.KeyConflict(key, _, _) => Conflict(
          IdempotencyCheckResponse(
            status = "conflict",
            idempotencyKey = key,
            originalResponse = None,
            message =
              Some("Request body does not match the original request for this idempotency key"),
            error = Some(IdempotencyApi.ConflictCode),
          ).asJson,
        )

  private def sha256(input: String): String =
    val digest = MessageDigest.getInstance("SHA-256")
    val hash = digest
      .digest(input.getBytes(java.nio.charset.StandardCharsets.UTF_8))
    hash.map(b => "%02x".format(b)).mkString

  private def currentTraceId: F[Option[String]] = Tracer[F].currentSpanContext
    .map(_.filter(_.isValid).map(_.traceIdHex))

  private def publishEvent(
      result: IdempotencyResult,
      client: AuthenticatedClient,
      ttlSeconds: Long,
      timestamp: Instant,
      traceId: Option[String],
  ): F[Unit] =
    val event = result match
      case IdempotencyResult.New(key, _, _) => RateLimitEvent.IdempotencyNew(
          timestamp = timestamp,
          idempotencyKey = key,
          clientId = client.apiKeyId,
          ttlSeconds = ttlSeconds,
          traceId = traceId,
        )
      case IdempotencyResult.Duplicate(key, _, firstSeenAt) => RateLimitEvent
          .IdempotencyHit(
            timestamp = timestamp,
            idempotencyKey = key,
            clientId = client.apiKeyId,
            originalRequestTime = firstSeenAt,
            traceId = traceId,
          )
      case IdempotencyResult.InProgress(key, startedAt) => RateLimitEvent
          .IdempotencyHit(
            timestamp = timestamp,
            idempotencyKey = key,
            clientId = client.apiKeyId,
            originalRequestTime = startedAt,
            traceId = traceId,
          )
      case IdempotencyResult.KeyConflict(key, _, _) => RateLimitEvent
          .IdempotencyHit(
            timestamp = timestamp,
            idempotencyKey = key,
            clientId = client.apiKeyId,
            originalRequestTime = timestamp,
            traceId = traceId,
          )

    val publishMain = eventPublisher.publish(event).handleErrorWith(error =>
      logger.warn(s"Failed to publish idempotency event: ${error.getMessage}"),
    )

    val publishAudit = result match
      case IdempotencyResult.KeyConflict(key, _, _) => getRequestId()
          .flatMap { requestId =>
            val auditEvent = RateLimitEvent.AuditEvent(
              timestamp = timestamp,
              requestId = requestId,
              apiKey = client.apiKeyId,
              clientId = client.apiKeyId,
              decision = "conflict",
              reason = "Request hash mismatch on idempotency key",
              endpoint = None,
              sourceIp = None,
              tier = None,
              traceId = traceId,
            )
            logger.info(s"AUDIT decision=conflict client=${client
                .apiKeyId} idempotencyKey=$key") *>
              eventPublisher.publish(auditEvent).handleErrorWith(error =>
                logger
                  .warn(s"Failed to publish audit event: ${error.getMessage}"),
              )
          }
      case _ => Async[F].unit

    publishMain *> publishAudit

// Request/Response models
case class IdempotencyCheckRequest(
    idempotencyKey: String,
    ttl: Option[Long] = None,
    requestBody: Option[String] = None,
)

case class IdempotencyCheckResponse(
    status: String,
    idempotencyKey: String,
    originalResponse: Option[OriginalResponse] = None,
    firstSeenAt: Option[String] = None,
    message: Option[String] = None,
    // Set with `message` on the 409, like every other error body. `status`
    // still carries the outcome.
    error: Option[String] = None,
)

case class OriginalResponse(
    statusCode: Int,
    body: String,
    headers: Map[String, String],
)

case class IdempotencyCompleteRequest(
    statusCode: Int,
    body: String,
    headers: Option[Map[String, String]] = None,
)

case class IdempotencyUpdateResponse(
    idempotencyKey: String,
    status: String,
    message: Option[String] = None,
    error: Option[String] = None,
)

object IdempotencyApi:
  val ConflictCode = "idempotency_conflict"
  val NotPendingCode = "not_pending"

  /** The largest stored response, in bytes of its encoded JSON. A DynamoDB item
    * holds at most 400 KB, and the response is stored inline in the record
    * (replay is not streamed), so a larger one failed the write and answered
    * 503 as if the store were down. The margin covers the record's other
    * attributes, whose largest is the key (at most 2 KB as a partition key).
    */
  val MaxStoredResponseBytes: Int = 350 * 1024

  /** The size the store writes: the response as encoded JSON, so escaping in
    * the body and the headers count, not just the body's characters.
    */
  def storedSize(response: StoredResponse): Int = response.asJson.noSpaces
    .getBytes(java.nio.charset.StandardCharsets.UTF_8).length

  /** `result` with its key replaced by `key`. The store works on the scoped key
    * (ADR-005), which must not leak into responses or events.
    */
  def withKey(result: IdempotencyResult, key: String): IdempotencyResult =
    result match
      case r: IdempotencyResult.New => r.copy(idempotencyKey = key)
      case r: IdempotencyResult.Duplicate => r.copy(idempotencyKey = key)
      case r: IdempotencyResult.InProgress => r.copy(idempotencyKey = key)
      case r: IdempotencyResult.KeyConflict => r.copy(idempotencyKey = key)

  def apply[F[_]: Async: Tracer](
      store: IdempotencyStore[F],
      idempotencyConfig: IdempotencyConfig,
      eventPublisher: EventPublisher[F],
      metricsPublisher: MetricsPublisher[F],
      logger: Logger[F],
      getRequestId: () => F[String],
  ): IdempotencyApi[F] = new IdempotencyApi[F](
    store,
    idempotencyConfig,
    eventPublisher,
    metricsPublisher,
    logger,
    getRequestId,
  )
