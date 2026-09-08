package api

import java.time.Instant
import java.util.UUID

import org.http4s.*
import org.http4s.circe.*
import org.http4s.circe.CirceEntityDecoder.*
import org.http4s.circe.CirceEntityEncoder.*
import org.http4s.dsl.Http4sDsl
import org.typelevel.ci.*
import org.typelevel.log4cats.Logger
import org.typelevel.otel4s.trace.Tracer

import cats.effect.*
import cats.effect.syntax.spawn.*
import cats.syntax.all.*
import io.circe.generic.auto.*
import io.circe.syntax.*
import core.*
import events.*
import observability.{MetricsPublisher, TracingMiddleware}
import security.*

class TokenQuotaApi[F[_]: Async: Tracer](
    quotaService: TokenQuotaService[F],
    eventPublisher: EventPublisher[F],
    metricsPublisher: MetricsPublisher[F],
    logger: Logger[F],
    getRequestId: () => F[String],
) extends Http4sDsl[F]:
  import TokenQuotaApi.ContendedRetryAfterSeconds

  /** POST /v1/quota/check
    *
    * Pre-request: atomically reserve the estimated tokens at every level, or
    * reject without reserving anything.
    */
  def check(request: Request[F], client: AuthenticatedClient): F[Response[F]] =
    for
      startTime <- Clock[F].realTime.map(_.toMillis)
      req <- request.as[TokenQuotaCheckRequest]
      response <- nonNegative(
        req.estimatedInputTokens,
        req.estimatedOutputTokens,
      )("token estimates must be non-negative")(runCheck(req, client, startTime))
    yield response

  /** POST /v1/quota/reconcile
    *
    * Post-LLM-call: replace the estimate with actual usage. Actual usage is
    * recorded even when it lands past the limit.
    */
  def reconcile(
      request: Request[F],
      client: AuthenticatedClient,
  ): F[Response[F]] =
    for
      startTime <- Clock[F].realTime.map(_.toMillis)
      req <- request.as[TokenQuotaReconcileRequest]
      response <- nonNegative(
        req.actualInputTokens,
        req.actualOutputTokens,
        req.estimatedInputTokens,
        req.estimatedOutputTokens,
      )("token counts must be non-negative")(runReconcile(req, startTime))
    yield response

  private def runCheck(
      req: TokenQuotaCheckRequest,
      client: AuthenticatedClient,
      startTime: Long,
  ): F[Response[F]] =
    val identifier = QuotaIdentifier(req.userId, req.agentId, req.orgId)
    for
      decision <- TracingMiddleware.traced("checkQuota")(quotaService.checkQuota(
        identifier,
        req.estimatedInputTokens,
        req.estimatedOutputTokens,
      ))
      latency <- Clock[F].realTime.map(_.toMillis - startTime)
      _ <- metricsPublisher.recordLatency("token_quota_check", latency.toDouble)
      now <- Clock[F].realTime.map(d => Instant.ofEpochMilli(d.toMillis))
      traceId <- currentTraceId
      _ <- publishQuotaEvent(decision, req, client, now, traceId).start
      resp <- buildCheckResponse(decision)
    yield resp

  private def runReconcile(
      req: TokenQuotaReconcileRequest,
      startTime: Long,
  ): F[Response[F]] =
    val identifier = QuotaIdentifier(req.userId, req.agentId, req.orgId)
    for
      result <- quotaService.reconcile(
        identifier,
        req.actualInputTokens,
        req.actualOutputTokens,
        req.estimatedInputTokens,
        req.estimatedOutputTokens,
      )
      latency <- Clock[F].realTime.map(_.toMillis - startTime)
      _ <- metricsPublisher
        .recordLatency("token_quota_reconcile", latency.toDouble)
      resp <- buildReconcileResponse(result, req)
    yield resp

  private def nonNegative(
      values: Long*,
  )(message: String)(ok: => F[Response[F]]): F[Response[F]] =
    if values.exists(_ < 0) then
      BadRequest(io.circe.Json.obj(
        "error" -> io.circe.Json.fromString("validation_error"),
        "message" -> io.circe.Json.fromString(message),
      ))
    else ok

  private def buildCheckResponse(decision: QuotaDecision): F[Response[F]] =
    decision match
      case QuotaDecision.Available(remaining) => Ok(
          TokenQuotaCheckResponse(
            allowed = true,
            remainingTokens = remaining
              .map { case (level, rem) => level.prefix -> rem },
            exceededLevel = None,
            retryAfter = None,
          ).asJson,
        )
      case QuotaDecision.Exceeded(level, limit, used, retryAfter) =>
        TooManyRequests(
          TokenQuotaCheckResponse(
            allowed = false,
            remainingTokens = Map.empty,
            exceededLevel = Some(level.prefix),
            retryAfter = Some(retryAfter),
            message =
              Some(s"${level.prefix} quota exceeded: $used/$limit tokens used"),
          ).asJson,
        ).map(withRetryAfter(retryAfter))
      case QuotaDecision.Contended(attempts) => ServiceUnavailable(
          TokenQuotaCheckResponse(
            allowed = false,
            remainingTokens = Map.empty,
            exceededLevel = None,
            retryAfter = Some(ContendedRetryAfterSeconds),
            message =
              Some(s"quota state contended after $attempts attempts; nothing was reserved, retry shortly"),
          ).asJson,
        ).map(withRetryAfter(ContendedRetryAfterSeconds))

  private def buildReconcileResponse(
      result: ReconcileResult,
      req: TokenQuotaReconcileRequest,
  ): F[Response[F]] = result match
    case ReconcileResult.Reconciled(inputDelta, outputDelta) => Ok(
        TokenQuotaReconcileResponse("reconciled", inputDelta, outputDelta)
          .asJson,
      )
    case ReconcileResult.Contended(_) => ServiceUnavailable(
        TokenQuotaReconcileResponse(
          status = "contended",
          inputDelta = req.actualInputTokens - req.estimatedInputTokens,
          outputDelta = req.actualOutputTokens - req.estimatedOutputTokens,
        ).asJson,
      ).map(withRetryAfter(ContendedRetryAfterSeconds))

  private def withRetryAfter(seconds: Int)(resp: Response[F]): Response[F] =
    resp.putHeaders(Header.Raw(ci"Retry-After", seconds.toString))

  private def currentTraceId: F[Option[String]] = Tracer[F].currentSpanContext
    .map(_.filter(_.isValid).map(_.traceIdHex))

  private def publishQuotaEvent(
      decision: QuotaDecision,
      req: TokenQuotaCheckRequest,
      client: AuthenticatedClient,
      timestamp: Instant,
      traceId: Option[String],
  ): F[Unit] = decision match
    case QuotaDecision.Exceeded(level, limit, used, _) =>
      val event = RateLimitEvent.TokenQuotaExceeded(
        timestamp = timestamp,
        userId = req.userId,
        agentId = req.agentId.getOrElse("none"),
        orgId = req.orgId.getOrElse("none"),
        level = level.prefix,
        limit = limit,
        used = used,
        apiKey = client.apiKeyId,
        traceId = traceId,
      )
      getRequestId().flatMap { requestId =>
        val auditEvent = RateLimitEvent.AuditEvent(
          timestamp = timestamp,
          requestId = requestId,
          apiKey = client.apiKeyId,
          clientId = client.apiKeyId,
          decision = "quota_exceeded",
          reason = s"${level.prefix} quota exceeded: $used/$limit tokens",
          endpoint = Some("/v1/quota/check"),
          sourceIp = None,
          tier = None,
          traceId = traceId,
        )
        val publishMain = eventPublisher.publish(event).handleErrorWith(error =>
          logger
            .warn(s"Failed to publish token quota event: ${error.getMessage}"),
        )
        val publishAudit = logger
          .info(s"AUDIT decision=quota_exceeded user=${req.userId} level=${level
              .prefix} used=$used/$limit") *>
          eventPublisher.publish(auditEvent).handleErrorWith(error =>
            logger.warn(s"Failed to publish audit event: ${error.getMessage}"),
          )
        publishMain *> publishAudit
      }
    case _ => Async[F].unit

// Request/Response models

case class TokenQuotaCheckRequest(
    userId: String,
    agentId: Option[String] = None,
    orgId: Option[String] = None,
    estimatedInputTokens: Long,
    estimatedOutputTokens: Long = 0,
)

object TokenQuotaCheckRequest:
  // See RateLimitCheckRequest: default values are not part of the derived
  // decoder, so `estimatedOutputTokens` has to be read explicitly to stay
  // optional on the wire.
  given io.circe.Decoder[TokenQuotaCheckRequest] = c =>
    for
      userId <- c.get[String]("userId")
      agentId <- c.get[Option[String]]("agentId")
      orgId <- c.get[Option[String]]("orgId")
      estimatedInputTokens <- c.get[Long]("estimatedInputTokens")
      estimatedOutputTokens <- c.getOrElse[Long]("estimatedOutputTokens")(0L)
    yield TokenQuotaCheckRequest(
      userId,
      agentId,
      orgId,
      estimatedInputTokens,
      estimatedOutputTokens,
    )

case class TokenQuotaCheckResponse(
    allowed: Boolean,
    remainingTokens: Map[String, Long],
    exceededLevel: Option[String] = None,
    retryAfter: Option[Int] = None,
    message: Option[String] = None,
)

case class TokenQuotaReconcileRequest(
    userId: String,
    agentId: Option[String] = None,
    orgId: Option[String] = None,
    actualInputTokens: Long,
    actualOutputTokens: Long,
    estimatedInputTokens: Long,
    estimatedOutputTokens: Long,
)

case class TokenQuotaReconcileResponse(
    status: String,
    inputDelta: Long,
    outputDelta: Long,
)

object TokenQuotaApi:
  /** What a 503 tells clients to wait before retrying a contended write. */
  val ContendedRetryAfterSeconds: Int = 1

  def apply[F[_]: Async: Tracer](
      quotaService: TokenQuotaService[F],
      eventPublisher: EventPublisher[F],
      metricsPublisher: MetricsPublisher[F],
      logger: Logger[F],
      getRequestId: () => F[String],
  ): TokenQuotaApi[F] = new TokenQuotaApi[F](
    quotaService,
    eventPublisher,
    metricsPublisher,
    logger,
    getRequestId,
  )
