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
import config.RateLimitConfig

/** Rate limit API endpoints.
  *
  * Developer note: The API passes `resetAt` from `RateLimitDecision` into both
  * the JSON body and the `X-RateLimit-Reset` header. Store implementations
  * (token bucket, leaky-bucket, sliding-window) each return the correct reset
  * instant for their algorithm; no algorithm-specific handling is needed here —
  * "epoch seconds at which capacity resets" is algorithm-agnostic.
  */
class RateLimitApi[F[_]: Async: Tracer](
    store: RateLimitStore[F],
    eventPublisher: EventPublisher[F],
    metricsPublisher: MetricsPublisher[F],
    config: RateLimitConfig,
    logger: Logger[F],
    getRequestId: () => F[String],
) extends Http4sDsl[F]:

  /** POST /v1/ratelimit/check
    *
    * Check if a request is allowed under the rate limit.
    */
  def check(request: Request[F], client: AuthenticatedClient): F[Response[F]] =
    for
      startTime <- Clock[F].realTime.map(_.toMillis)
      checkReq <- request.as[RateLimitCheckRequest]

      // Validate cost before hitting the store; zero/negative cost is a client error
      response <-
        if checkReq.cost <= 0 then
          BadRequest(io.circe.Json.obj(
            "error" -> io.circe.Json.fromString("validation_error"),
            "message" -> io.circe.Json.fromString("cost must be positive"),
          ))
        else
          for
            // Get profile for this client tier
            profile <- Async[F].pure(getProfile(client.tier, checkReq.profile))

            // Perform rate limit check
            _ <- logger.debug(s"Rate limit check: key=${checkReq
                .key}, cost=${checkReq.cost}, tier=${client.tier}")
            decision <- TracingMiddleware.traced("checkAndConsume")(
              store.checkAndConsume(checkReq.key, checkReq.cost, profile),
            )

            // Record metrics
            latency <- Clock[F].realTime.map(_.toMillis - startTime)
            _ <- metricsPublisher
              .recordLatency("rate_limit_check", latency.toDouble)
            _ <- decision match
              case RateLimitDecision.Allowed(_, _) => metricsPublisher
                  .recordRateLimitDecision(allowed = true, client.apiKeyId)
              case RateLimitDecision.Rejected(_, _) => metricsPublisher
                  .recordRateLimitDecision(allowed = false, client.apiKeyId)

            // Publish event (fire and forget)
            now <- Clock[F].realTime.map(d => Instant.ofEpochMilli(d.toMillis))
            traceId <- currentTraceId
            _ <- publishEvent(decision, checkReq, client, now, traceId).start

            // Build response
            resp <- buildCheckResponse(decision, profile)
          yield resp
    yield response

  /** GET /v1/ratelimit/status/:key
    *
    * Get current rate limit status for a key.
    */
  def status(key: String, client: AuthenticatedClient): F[Response[F]] =
    val profile = getProfile(client.tier, None)
    for
      maybeStatus <- store.getStatus(key, profile)
      nowMs <- Clock[F].realTime.map(_.toMillis)
      response <- maybeStatus match
        case Some(state) =>
          // Calculate reset time based on current state
          val resetAt = Instant.ofEpochMilli(nowMs).plusSeconds(
            ((profile.capacity - state.tokensRemaining) /
              profile.refillRatePerSecond).ceil.toLong,
          )
          Ok(
            RateLimitStatusResponse(
              key = key,
              tokensRemaining = state.tokensRemaining,
              limit = profile.capacity,
              resetAt = resetAt.toString,
            ).asJson,
          )
        case None =>
          // No state means full capacity (never seen this key)
          Ok(
            RateLimitStatusResponse(
              key = key,
              tokensRemaining = profile.capacity,
              limit = profile.capacity,
              resetAt = Instant.now().plusSeconds(60).toString,
            ).asJson,
          )
    yield response

  // Explicit profile name wins, then tier-named profile from config, then
  // config defaults. No hardcoded values that can drift from application.conf
  private def getProfile(
      tier: ClientTier,
      profileName: Option[String],
  ): RateLimitProfile =
    val fromExplicitName = profileName.flatMap(config.profiles.get)
    val fromTierName = config.profiles.get(tier.toString.toLowerCase)
    fromExplicitName.orElse(fromTierName)
      .map(p => RateLimitProfile(p.capacity, p.refillRatePerSecond, p.ttlSeconds))
      .getOrElse(RateLimitProfile(
        config.defaultCapacity,
        config.defaultRefillRatePerSecond,
        config.defaultTtlSeconds,
      ))

  private def buildCheckResponse(
      decision: RateLimitDecision,
      profile: RateLimitProfile,
  ): F[Response[F]] = decision match
    case RateLimitDecision.Allowed(tokensRemaining, resetAt) => Ok(
        RateLimitCheckResponse(
          allowed = true,
          tokensRemaining = Some(tokensRemaining),
          retryAfter = None,
          limit = profile.capacity,
          resetAt = resetAt.toString,
          message = None,
        ).asJson,
      ).map(_.putHeaders(
        Header.Raw(ci"X-RateLimit-Limit", profile.capacity.toString),
        Header.Raw(ci"X-RateLimit-Remaining", tokensRemaining.toString),
        Header.Raw(ci"X-RateLimit-Reset", resetAt.getEpochSecond.toString),
      ))

    case RateLimitDecision.Rejected(retryAfter, resetAt) => TooManyRequests(
        RateLimitCheckResponse(
          allowed = false,
          tokensRemaining = None,
          retryAfter = Some(retryAfter),
          limit = profile.capacity,
          resetAt = resetAt.toString,
          message = Some("Rate limit exceeded"),
        ).asJson,
      ).map(_.putHeaders(Header.Raw(ci"Retry-After", retryAfter.toString)))

  private def currentTraceId: F[Option[String]] = Tracer[F].currentSpanContext
    .map(_.filter(_.isValid).map(_.traceIdHex))

  private def publishEvent(
      decision: RateLimitDecision,
      request: RateLimitCheckRequest,
      client: AuthenticatedClient,
      timestamp: Instant,
      traceId: Option[String],
  ): F[Unit] =
    val event = decision match
      case RateLimitDecision.Allowed(tokensRemaining, _) => RateLimitEvent
          .Allowed(
            timestamp = timestamp,
            apiKey = client.apiKeyId,
            clientId = client.clientId,
            endpoint = request.endpoint.getOrElse("unknown"),
            tokensRemaining = tokensRemaining,
            cost = request.cost,
            tier = client.tier.toString,
            traceId = traceId,
          )
      case RateLimitDecision.Rejected(retryAfter, _) => RateLimitEvent.Rejected(
          timestamp = timestamp,
          apiKey = client.apiKeyId,
          clientId = client.clientId,
          endpoint = request.endpoint.getOrElse("unknown"),
          retryAfterSeconds = retryAfter,
          reason = "Rate limit exceeded",
          tier = client.tier.toString,
          traceId = traceId,
        )

    val publishMain = eventPublisher.publish(event).handleErrorWith(error =>
      logger.warn(s"Failed to publish rate limit event: ${error.getMessage}"),
    )

    val publishAudit = decision match
      case RateLimitDecision.Rejected(_, _) => getRequestId()
          .flatMap { requestId =>
            val auditEvent = RateLimitEvent.AuditEvent(
              timestamp = timestamp,
              requestId = requestId,
              apiKey = client.apiKeyId,
              clientId = client.clientId,
              decision = "rejected",
              reason = "Rate limit exceeded",
              endpoint = request.endpoint,
              sourceIp = None,
              tier = Some(client.tier.toString),
              traceId = traceId,
            )
            logger.info(s"AUDIT decision=rejected client=${client
                .clientId} key=${request.key} tier=${client.tier}") *>
              eventPublisher.publish(auditEvent).handleErrorWith(error =>
                logger
                  .warn(s"Failed to publish audit event: ${error.getMessage}"),
              )
          }
      case _ => Async[F].unit

    publishMain *> publishAudit

// Request/Response models
case class RateLimitCheckRequest(
    key: String,
    cost: Int = 1,
    profile: Option[String] = None,
    endpoint: Option[String] = None,
)

object RateLimitCheckRequest:
  // Circe's auto derivation ignores Scala default values, so a body that
  // legitimately omits `cost` would fail to decode. I decode it explicitly so
  // the wire contract matches what docs/API.md documents as optional.
  given io.circe.Decoder[RateLimitCheckRequest] = c =>
    for
      key <- c.get[String]("key")
      cost <- c.getOrElse[Int]("cost")(1)
      profile <- c.get[Option[String]]("profile")
      endpoint <- c.get[Option[String]]("endpoint")
    yield RateLimitCheckRequest(key, cost, profile, endpoint)

case class RateLimitCheckResponse(
    allowed: Boolean,
    tokensRemaining: Option[Int],
    retryAfter: Option[Int],
    limit: Int,
    resetAt: String,
    message: Option[String] = None,
)

case class RateLimitStatusResponse(
    key: String,
    tokensRemaining: Int,
    limit: Int,
    resetAt: String,
)

object RateLimitApi:
  def apply[F[_]: Async: Tracer](
      store: RateLimitStore[F],
      eventPublisher: EventPublisher[F],
      metricsPublisher: MetricsPublisher[F],
      config: RateLimitConfig,
      logger: Logger[F],
      getRequestId: () => F[String],
  ): RateLimitApi[F] = new RateLimitApi[F](
    store,
    eventPublisher,
    metricsPublisher,
    config,
    logger,
    getRequestId,
  )
