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

      requestedTtl = checkReq.ttl.getOrElse(idempotencyConfig.defaultTtlSeconds)
      ttlSeconds = math.min(requestedTtl, idempotencyConfig.maxTtlSeconds)
      _ <-
        if requestedTtl > idempotencyConfig.maxTtlSeconds then
          logger.warn(
            s"Idempotency TTL capped: requested=$requestedTtl, max=${idempotencyConfig
                .maxTtlSeconds}, key=${checkReq.idempotencyKey}",
          )
        else ().pure[F]

      requestHash = checkReq.requestBody.map(sha256)

      _ <- logger.debug(s"Idempotency check: key=${checkReq
          .idempotencyKey}, client=${client.apiKeyId}, hasHash=${requestHash
          .isDefined}")
      result <- TracingMiddleware.traced("executeIdempotent")(
        metricsPublisher.timed(
          "IdempotencyStoreLatency",
          Map("operation" -> "idempotency_check"),
        )(store.check(
          checkReq.idempotencyKey,
          client.apiKeyId,
          ttlSeconds,
          requestHash,
        )),
      )

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
    yield response).handleErrorWith(storageFailure("check"))

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

    _ <- logger
      .debug(s"Completing idempotency key: $key, client=${client.apiKeyId}")
    success <- store.storeResponse(key, storedResponse)

    response <-
      if success then
        Ok(
          IdempotencyCompleteResponse(idempotencyKey = key, status = "completed")
            .asJson,
        )
      else
        Conflict(
          IdempotencyCompleteResponse(
            idempotencyKey = key,
            status = "failed",
            message = Some(
              "Could not store response - key may not exist or is not pending",
            ),
          ).asJson,
        )
  yield response).handleErrorWith(storageFailure("complete"))

  /** Corrupt records and unexpected store failures both become a structured 503
    * so the caller knows not to proceed; body decoding failures pass through so
    * http4s can answer 4xx as usual.
    */
  private def storageFailure(op: String): Throwable => F[Response[F]] =
    case e: CorruptIdempotencyRecordException => logger
        .error(e)(s"Corrupt idempotency record for key=${e.key}") *>
        ServiceUnavailable(io.circe.Json.obj(
          "error" -> io.circe.Json.fromString("storage_corruption"),
          "message" ->
            io.circe.Json
              .fromString(s"Idempotency record corrupted: ${e.detail}"),
        ))
    case e: MessageFailure => Async[F].raiseError(e)
    case e => logger.error(e)(s"Idempotency $op failed") *>
        (if op == "check" then countCheck("error") else Async[F].unit) *>
        ServiceUnavailable(io.circe.Json.obj(
          "error" -> io.circe.Json.fromString("storage_unavailable"),
          "message" -> io.circe.Json.fromString(
            s"Idempotency store failed during $op; retry the request",
          ),
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
      case IdempotencyResult.New(key, _) => Ok(
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
      case IdempotencyResult.New(key, _) => RateLimitEvent.IdempotencyNew(
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

case class IdempotencyCompleteResponse(
    idempotencyKey: String,
    status: String,
    message: Option[String] = None,
)

object IdempotencyApi:
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
