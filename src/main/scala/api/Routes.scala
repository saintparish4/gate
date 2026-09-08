package api

import java.nio.charset.StandardCharsets
import java.time.Instant

import org.http4s.*
import org.http4s.circe.*
import org.http4s.circe.CirceEntityDecoder.*
import org.http4s.circe.CirceEntityEncoder.*
import org.http4s.dsl.Http4sDsl
import org.http4s.headers.`Content-Type`
import org.http4s.server.{AuthMiddleware, Router}
import org.typelevel.log4cats.Logger
import org.typelevel.otel4s.trace.Tracer

import fs2.Stream
import cats.effect.*
import cats.effect.std.Queue
import cats.syntax.all.*
import io.circe.generic.auto.*
import io.circe.syntax.*
import core.*
import events.*
import observability.{MetricsPublisher, PrometheusMetrics, TracingMiddleware}
import resilience.AggregateHealth
import security.*
import config.{IdempotencyConfig, RateLimitConfig, TokenQuotaConfig}

/** HTTP routes for the rate limiter API.
  *
  * Provides endpoints for:
  *   - Rate limit checks
  *   - Idempotency checks
  *   - Health and readiness probes
  *   - Metrics (admin only)
  */
class Routes[F[_]: Async: Tracer](
    rateLimitStore: RateLimitStore[F],
    idempotencyStore: IdempotencyStore[F],
    eventPublisher: EventPublisher[F],
    metricsPublisher: MetricsPublisher[F],
    authMiddleware: AuthMiddleware[F, AuthenticatedClient],
    rateLimitConfig: RateLimitConfig,
    idempotencyConfig: IdempotencyConfig,
    logger: Logger[F],
    dashboardApi: DashboardApi[F],
    tokenQuotaApi: Option[TokenQuotaApi[F]],
    prometheusMetrics: Option[PrometheusMetrics[F]],
    healthCheck: F[AggregateHealth],
    getRequestId: () => F[String],
) extends Http4sDsl[F]:

  private val rateLimitApi = RateLimitApi[F](
    rateLimitStore,
    eventPublisher,
    metricsPublisher,
    rateLimitConfig,
    logger,
    getRequestId,
  )

  private val idempotencyApi = IdempotencyApi[F](
    idempotencyStore,
    idempotencyConfig,
    eventPublisher,
    metricsPublisher,
    logger,
    getRequestId,
  )

  // Public routes (no auth required)
  private val publicRoutes: HttpRoutes[F] = HttpRoutes.of[F] {
    // Liveness probe - always returns 200 if service is running
    case GET -> Root / "health" =>
      Ok(HealthResponse("healthy", BuildInfo.version).asJson)

    // Prometheus metrics scrape endpoint.
    //
    // We build the response by writing raw UTF-8 bytes to the body stream
    // instead of `withEntity(body)` / `Ok(body)`. The wildcard
    // `org.http4s.circe.CirceEntityEncoder.*` import in this file otherwise
    // resolves an `EntityEncoder[F, String]` that JSON-encodes the payload
    // (wrapping it in quotes and escaping newlines), which Prometheus
    // rejects with: expected a valid start token, got "\"".
    case GET -> Root / "metrics" => prometheusMetrics match
        case Some(prom) => prom.scrape.map { body =>
            val bytes = body.getBytes(StandardCharsets.UTF_8)
            Response[F](status = Status.Ok)
              .withBodyStream(Stream.emits(bytes).covary[F]).withContentType(
                `Content-Type`(org.http4s.MediaType.text.plain, Charset.`UTF-8`),
              ).putHeaders(org.http4s.headers.`Content-Length`.unsafeFromLong(
                bytes.length.toLong,
              ))
          }
        case None => NotFound()

    // Readiness probe - aggregated dependency health
    case GET -> Root / "ready" => healthCheck.flatMap { health =>
        val json = health.asJson
        if health.isHealthy then Ok(json) else ServiceUnavailable(json)
      }
  }

  // Authenticated routes
  private val authedRoutes: AuthedRoutes[AuthenticatedClient, F] = AuthedRoutes
    .of {
      // Rate limit check
      case req @ POST -> Root / "v1" / "ratelimit" / "check" as client =>
        rateLimitApi.check(req.req, client)

      // Rate limit status
      case GET -> Root / "v1" / "ratelimit" / "status" / key as client =>
        rateLimitApi.status(key, client)

      // Idempotency check
      case req @ POST -> Root / "v1" / "idempotency" / "check" as client =>
        idempotencyApi.check(req.req, client)

      // Store idempotency response
      case req @ POST -> Root / "v1" / "idempotency" / key / "complete" as
          client => idempotencyApi.complete(key, req.req, client)

      // Token quota check
      case req @ POST -> Root / "v1" / "quota" / "check" as client =>
        tokenQuotaApi match
          case Some(api) => api.check(req.req, client)
          case None => Response[F](status = Status.NotFound).pure[F]

      // Token quota reconcile
      case req @ POST -> Root / "v1" / "quota" / "reconcile" as client =>
        tokenQuotaApi match
          case Some(api) => api.reconcile(req.req, client)
          case None => Response[F](status = Status.NotFound).pure[F]
    }

  // Combined routes — public (health, metrics, ready) first so scrapers never hit auth
  val routes: HttpRoutes[F] = TracingMiddleware[F](
    publicRoutes <+> dashboardApi.routes <+> authMiddleware(authedRoutes),
  )

  def httpApp: HttpApp[F] = org.http4s.server.middleware.ErrorHandling
    .httpApp(Router("/" -> routes).orNotFound)

// API models
case class HealthResponse(status: String, version: String)
case class ReadyResponse(
    status: String,
    checks: Map[String, Boolean],
    failing: Option[List[String]] = None,
)

object BuildInfo:
  val version = "0.2.0"

object Routes:
  def apply[F[_]: Async: Tracer](
      rateLimitStore: RateLimitStore[F],
      idempotencyStore: IdempotencyStore[F],
      eventPublisher: EventPublisher[F],
      metricsPublisher: MetricsPublisher[F],
      authMiddleware: AuthMiddleware[F, AuthenticatedClient],
      rateLimitConfig: RateLimitConfig,
      idempotencyConfig: IdempotencyConfig,
      logger: Logger[F],
      dashboardEventQueue: Option[Queue[F, RateLimitEvent]] = None,
      tokenQuotaApi: Option[TokenQuotaApi[F]] = None,
      prometheusMetrics: Option[PrometheusMetrics[F]] = None,
      healthCheck: F[AggregateHealth],
      getRequestId: () => F[String],
  ): F[Routes[F]] =
    for
      dashboardApi <- DashboardApi.apply[F](
        rateLimitStore,
        rateLimitConfig,
        logger,
        dashboardEventQueue,
        eventPublisher,
      )
      routes = new Routes[F](
        rateLimitStore,
        idempotencyStore,
        eventPublisher,
        metricsPublisher,
        authMiddleware,
        rateLimitConfig,
        idempotencyConfig,
        logger,
        dashboardApi,
        tokenQuotaApi,
        prometheusMetrics,
        healthCheck,
        getRequestId,
      )
    yield routes
