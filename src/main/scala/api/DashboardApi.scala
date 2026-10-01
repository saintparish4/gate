package api

import java.time.Instant

import scala.concurrent.duration.*

import org.http4s.*
import org.http4s.circe.CirceEntityDecoder.*
import org.http4s.circe.CirceEntityEncoder.*
import org.http4s.dsl.Http4sDsl
import org.http4s.server.middleware.CORS
import org.typelevel.log4cats.Logger

import cats.effect.*
import cats.effect.std.Queue
import cats.syntax.all.*
import fs2.Stream
import io.circe.Json
import io.circe.syntax.*
import core.*
import config.RateLimitConfig
import events.{EventPublisher, RateLimitEvent}

/** Dashboard API — public (no auth) endpoints that power the live HTML
  * dashboard.
  *
  * Uses a fixed demo key ("dashboard-demo") against the real RateLimitStore so
  * visitors can see the token-bucket algorithm working in real-time without
  * needing an API key.
  *
  * When dashboardEventQueue is Some, GET /v1/ratelimit/dashboard/stats streams
  * real-time decision events (allow/reject) from the EventPublisher to the
  * browser.
  */
class DashboardApi[F[_]: Async](
    rateLimitStore: RateLimitStore[F],
    rateLimitConfig: RateLimitConfig,
    logger: Logger[F],
    demoProfileRef: Ref[F, RateLimitProfile],
    dashboardEventQueue: Option[Queue[F, RateLimitEvent]],
    eventPublisher: EventPublisher[F],
) extends Http4sDsl[F]:

  // Fixed demo key
  private val demoKey = "dashboard-demo"

  // Helper to get current profile
  private def getProfile: F[RateLimitProfile] = demoProfileRef.get

  val routes: HttpRoutes[F] = HttpRoutes.of[F] {

    // GET /dashboard — serve the single-page HTML dashboard from classpath
    case req @ GET -> Root / "dashboard" => StaticFile
        .fromResource[F]("static/dashboard.html", Some(req)).getOrElseF(NotFound(
          ApiError.body(ApiError.NotFound, "Dashboard resource not found"),
        ))

    // GET /dashboard/api/config — bucket configuration for the UI
    case GET -> Root / "dashboard" / "api" / "config" =>
      for
        profile <- getProfile
        response <- Ok(Json.obj(
          "capacity" -> Json.fromInt(profile.capacity),
          "refillRatePerSecond" ->
            Json.fromDoubleOrNull(profile.refillRatePerSecond),
          "ttlSeconds" -> Json.fromLong(profile.ttlSeconds),
        ))
      yield response

    // POST /dashboard/api/config — update bucket configuration
    case req @ POST -> Root / "dashboard" / "api" / "config" => (for
        updateReq <- req.as[Json]
        capacity <- Async[F].fromEither(
          updateReq.hcursor.downField("capacity").as[Int].left
            .map(_ => new IllegalArgumentException("Invalid capacity")),
        )
        refillRate <- Async[F].fromEither(
          updateReq.hcursor.downField("refillRatePerSecond").as[Double].left
            .map(_ => new IllegalArgumentException("Invalid refillRatePerSecond")),
        )
        ttlSeconds <- Async[F].fromEither(
          updateReq.hcursor.downField("ttlSeconds").as[Long].left
            .map(_ => new IllegalArgumentException("Invalid ttlSeconds")),
        )
        // Validate values
        _ <-
          if capacity <= 0 then
            Async[F].raiseError(new IllegalArgumentException(
              "Capacity must be positive",
            ))
          else Async[F].unit
        _ <-
          if refillRate <= 0 then
            Async[F].raiseError(new IllegalArgumentException(
              "Refill rate must be positive",
            ))
          else Async[F].unit
        _ <-
          if ttlSeconds <= 0 then
            Async[F]
              .raiseError(new IllegalArgumentException("TTL must be positive"))
          else Async[F].unit
        // Update the profile
        newProfile = RateLimitProfile(capacity, refillRate, ttlSeconds)
        _ <- demoProfileRef.set(newProfile)
        response <- Ok(Json.obj(
          "capacity" -> Json.fromInt(newProfile.capacity),
          "refillRatePerSecond" ->
            Json.fromDoubleOrNull(newProfile.refillRatePerSecond),
          "ttlSeconds" -> Json.fromLong(newProfile.ttlSeconds),
          "message" -> Json.fromString("Configuration updated successfully"),
        ))
      yield response).handleErrorWith(error =>
        // It answered `{"error": reason}` alone, with the reason where every
        // other route puts a code.
        BadRequest(ApiError.body(ApiError.ValidationError, error.getMessage)),
      )

    // POST /dashboard/api/check — consume one token from the demo bucket
    case POST -> Root / "dashboard" / "api" / "check" =>
      for
        profile <- getProfile
        decision <- rateLimitStore.checkAndConsume(demoKey, cost = 1, profile)
        now <- Clock[F].realTime.map(d => Instant.ofEpochMilli(d.toMillis))
        _ <- publishDashboardDecision(decision, now)
        response <- decision match
          case RateLimitDecision.Allowed(tokensRemaining, resetAt, _) =>
            Ok(Json.obj(
              "allowed" -> Json.True,
              "tokensRemaining" -> Json.fromInt(tokensRemaining),
              "limit" -> Json.fromInt(profile.capacity),
              "resetAt" -> Json.fromString(resetAt.toString),
            ))
          case RateLimitDecision.Rejected(retryAfter, resetAt, _) =>
            // Always 200 so dashboard JS can read the body without error handling.
            Ok(Json.obj(
              "allowed" -> Json.False,
              "tokensRemaining" -> Json.fromInt(0),
              "retryAfter" -> Json.fromInt(retryAfter),
              "limit" -> Json.fromInt(profile.capacity),
              "resetAt" -> Json.fromString(resetAt.toString),
            ))
      yield response

    // GET /dashboard/api/status — read-only peek at the demo bucket
    case GET -> Root / "dashboard" / "api" / "status" =>
      for
        profile <- getProfile
        maybeStatus <- rateLimitStore.getStatus(demoKey, profile)
        now <- Clock[F].realTime.map(d => Instant.ofEpochMilli(d.toMillis))
        // resetAt was always "". It is the store's value, as on
        // /v1/ratelimit/status; a bucket not yet created is full, so it
        // resets now.
        response <- Ok(Json.obj(
          "tokensRemaining" ->
            Json.fromInt(maybeStatus.fold(profile.capacity)(_.tokensRemaining)),
          "limit" -> Json.fromInt(profile.capacity),
          "resetAt" -> Json.fromString(maybeStatus.fold(now)(_.resetAt).toString),
        ))
      yield response

    // GET /dashboard/api/stats — Server-Sent Events stream for real-time updates
    case GET -> Root / "dashboard" / "api" / "stats" =>
      val stream = Stream.awakeEvery[F](500.millis).evalMap { _ =>
        for
          profile <- getProfile
          maybeStatus <- rateLimitStore.getStatus(demoKey, profile)
          tokens = maybeStatus.map(_.tokensRemaining).getOrElse(profile.capacity)
          data = Json.obj(
            "tokensRemaining" -> Json.fromInt(tokens),
            "limit" -> Json.fromInt(profile.capacity),
            "timestamp" -> Json.fromLong(System.currentTimeMillis()),
          )
        yield ServerSentEvent(data = Some(data.noSpaces))
      }.handleErrorWith(error =>
        Stream.eval(Async[F].delay(ServerSentEvent(data =
          Some(
            ApiError.body(ApiError.StorageUnavailable, error.getMessage)
              .noSpaces,
          ),
        ))),
      )

      Ok(stream)

    // GET /v1/ratelimit/dashboard/stats — SSE stream of real-time decision events
    case GET -> Root / "v1" / "ratelimit" / "dashboard" / "stats" =>
      dashboardEventQueue match
        case Some(queue) =>
          val eventStream = fs2.Stream.repeatEval(queue.take)
            .map(ev => ServerSentEvent(data = Some(ev.asJson.noSpaces)))
          Ok(eventStream)
        case None => NotFound(
            ApiError
              .body(ApiError.NotFound, "The decision stream is not enabled"),
          )
  }

  private def publishDashboardDecision(
      decision: RateLimitDecision,
      now: Instant,
  ): F[Unit] =
    val event = decision match
      case RateLimitDecision.Allowed(tokensRemaining, _, _) => RateLimitEvent
          .Allowed(
            timestamp = now,
            apiKey = "dashboard-demo",
            clientId = "dashboard",
            endpoint = "dashboard",
            tokensRemaining = tokensRemaining,
            cost = 1,
            tier = "dashboard",
          )
      case RateLimitDecision.Rejected(retryAfter, _, degraded) => RateLimitEvent
          .Rejected(
            timestamp = now,
            apiKey = "dashboard-demo",
            clientId = "dashboard",
            endpoint = "dashboard",
            retryAfterSeconds = retryAfter,
            reason = RateLimitApi.rejectionReason(degraded),
            tier = "dashboard",
          )
    eventPublisher.publish(event).handleError(e =>
      logger.warn(s"Dashboard event publish failed: ${e.getMessage}"),
    )

object DashboardApi:
  def apply[F[_]: Async](
      rateLimitStore: RateLimitStore[F],
      rateLimitConfig: RateLimitConfig,
      logger: Logger[F],
      dashboardEventQueue: Option[Queue[F, RateLimitEvent]] = None,
      eventPublisher: EventPublisher[F],
  ): F[DashboardApi[F]] =
    for
      demoProfileRef <- Ref.of[F, RateLimitProfile](RateLimitProfile(
        capacity = rateLimitConfig.defaultCapacity,
        refillRatePerSecond = rateLimitConfig.defaultRefillRatePerSecond,
        ttlSeconds = rateLimitConfig.defaultTtlSeconds,
      ))
      api = new DashboardApi[F](
        rateLimitStore,
        rateLimitConfig,
        logger,
        demoProfileRef,
        dashboardEventQueue,
        eventPublisher,
      )
    yield api
