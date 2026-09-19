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
        if checkReq.cost <= 0 then validationError("cost must be positive")
        else
          RateLimitApi
            .selectProfile(config, client.tier, checkReq.profile) match
            case Left(refusal) => refuseProfile(refusal, client)
            case Right(profile) => consume(checkReq, profile, client, startTime)
    yield response

  private def consume(
      checkReq: RateLimitCheckRequest,
      profile: RateLimitProfile,
      client: AuthenticatedClient,
      startTime: Long,
  ): F[Response[F]] =
    for
      _ <- logger.debug(s"Rate limit check: key=${checkReq.key}, cost=${checkReq
          .cost}, tier=${client.tier}")
      decision <- TracingMiddleware.traced("checkAndConsume")(
        store.checkAndConsume(checkReq.key, checkReq.cost, profile),
      )

      // Record metrics
      latency <- Clock[F].realTime.map(_.toMillis - startTime)
      _ <- metricsPublisher.recordLatency("rate_limit_check", latency.toDouble)
      _ <- decision match
        case RateLimitDecision.Allowed(_, _) => metricsPublisher
            .recordRateLimitDecision(allowed = true, client.apiKeyId)
        case RateLimitDecision.Rejected(_, _) => metricsPublisher
            .recordRateLimitDecision(allowed = false, client.apiKeyId)

      // Publish event (fire and forget)
      now <- Clock[F].realTime.map(d => Instant.ofEpochMilli(d.toMillis))
      traceId <- currentTraceId
      _ <- publishEvent(decision, checkReq, client, now, traceId).start

      resp <- buildCheckResponse(decision, profile)
    yield resp

  private def validationError(message: String): F[Response[F]] = BadRequest(
    io.circe.Json.obj(
      "error" -> io.circe.Json.fromString("validation_error"),
      "message" -> io.circe.Json.fromString(message),
    ),
  )

  private def refuseProfile(
      refusal: ProfileRefusal,
      client: AuthenticatedClient,
  ): F[Response[F]] = refusal match
    case ProfileRefusal.Unknown(name) =>
      validationError(s"unknown profile '$name'")
    case ProfileRefusal.AboveTier(name, tier) =>
      val tierName = tier.toString.toLowerCase
      logger.warn(s"AUDIT decision=profile_refused client=${client
          .clientId} profile=$name tier=$tierName") *>
        Forbidden(io.circe.Json.obj(
          "error" -> io.circe.Json.fromString("profile_not_permitted"),
          "message" ->
            io.circe.Json
              .fromString(s"profile '$name' exceeds the $tierName tier's limits"),
        ))

  /** GET /v1/ratelimit/status/:key
    *
    * Get current rate limit status for a key.
    */
  def status(key: String, client: AuthenticatedClient): F[Response[F]] =
    val profile = RateLimitApi.tierProfile(config, client.tier)
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

/** Why a `profile` named on a check request was not used. */
enum ProfileRefusal:
  case Unknown(name: String)
  case AboveTier(name: String, tier: ClientTier)

object RateLimitApi:

  /** The client's tier picks its profile; a `profile` named in the request may
    * only narrow it, never widen it. It used to win outright, so a free key
    * sending `"profile":"enterprise"` got 10,000 tokens at 1,000/s. An unknown
    * name is refused rather than quietly falling back to the tier, so a typo
    * surfaces instead of looking like a limit the caller never asked for.
    */
  def selectProfile(
      config: RateLimitConfig,
      tier: ClientTier,
      requested: Option[String],
  ): Either[ProfileRefusal, RateLimitProfile] =
    val own = tierProfile(config, tier)
    requested match
      case None => Right(own)
      case Some(name) => config.profiles.get(name) match
          case None => Left(ProfileRefusal.Unknown(name))
          case Some(p) =>
            val asked =
              RateLimitProfile(p.capacity, p.refillRatePerSecond, p.ttlSeconds)
            if withinTier(asked, own) then Right(asked)
            else Left(ProfileRefusal.AboveTier(name, tier))

  // Tier-named profile from config, then config defaults. No hardcoded values
  // that can drift from application.conf.
  def tierProfile(config: RateLimitConfig, tier: ClientTier): RateLimitProfile =
    config.profiles.get(tier.toString.toLowerCase)
      .map(p => RateLimitProfile(p.capacity, p.refillRatePerSecond, p.ttlSeconds))
      .getOrElse(RateLimitProfile(
        config.defaultCapacity,
        config.defaultRefillRatePerSecond,
        config.defaultTtlSeconds,
      ))

  // Both dimensions: a larger burst at a slower refill still beats the tier
  // over a short window, and a faster refill beats it over a long one.
  private def withinTier(
      asked: RateLimitProfile,
      own: RateLimitProfile,
  ): Boolean = asked.capacity <= own.capacity &&
    asked.refillRatePerSecond <= own.refillRatePerSecond

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
