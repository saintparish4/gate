package api

import java.nio.charset.StandardCharsets
import java.time.Instant

import org.http4s.*
import org.http4s.circe.*
import org.http4s.circe.CirceEntityDecoder.*
import org.http4s.circe.CirceEntityEncoder.*
import org.http4s.dsl.Http4sDsl
import org.http4s.headers.`Content-Type`
import org.http4s.server.AuthMiddleware
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
  *   - The demo dashboard, only when `dashboardEventQueue` is given
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
    dashboardApi: Option[DashboardApi[F]],
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

    // Readiness probe: 503 only when a component needed to serve decisions is
    // down. A failing optional one (Kinesis) reads "degraded" with a 200, so
    // the ALB keeps the task in service.
    case GET -> Root / "ready" => healthCheck.flatMap { health =>
        val json = health.asJson
        if health.isServing then Ok(json) else ServiceUnavailable(json)
      }
  }

  // Authenticated routes. Each one names the permission it needs; only
  // /metrics used to check, so a key's permissions decided nothing anywhere
  // else. The check runs before the route, so a refused request touches no
  // state.
  private val authedRoutes: AuthedRoutes[AuthenticatedClient, F] = AuthedRoutes
    .of {
      case req @ POST -> Root / "v1" / "ratelimit" / "check" as client =>
        guard(client, Permission.RateLimitCheck)(
          rateLimitApi.check(req.req, client),
        )

      case GET -> Root / "v1" / "ratelimit" / "status" / key as client =>
        guard(client, Permission.RateLimitStatus)(
          rateLimitApi.status(key, client),
        )

      case req @ POST -> Root / "v1" / "idempotency" / "check" as client =>
        guard(client, Permission.IdempotencyCheck)(
          idempotencyApi.check(req.req, client),
        )

      // Store idempotency response
      case req @ POST -> Root / "v1" / "idempotency" / key / "complete" as
          client => guard(client, Permission.IdempotencyComplete)(
          idempotencyApi.complete(key, req.req, client),
        )

      // Release a pending key so the operation can be retried. It ends the
      // key's pending state, as completing does, so it takes the same grant.
      case POST -> Root / "v1" / "idempotency" / key / "fail" as client =>
        guard(client, Permission.IdempotencyComplete)(
          idempotencyApi.fail(key, client),
        )

      case req @ POST -> Root / "v1" / "quota" / "check" as client =>
        guard(client, Permission.QuotaCheck)(
          tokenQuotaApi match
            case Some(api) => api.check(req.req, client)
            case None => quotaDisabled,
        )

      case req @ POST -> Root / "v1" / "quota" / "reconcile" as client =>
        guard(client, Permission.QuotaReconcile)(
          tokenQuotaApi match
            case Some(api) => api.reconcile(req.req, client)
            case None => quotaDisabled,
        )

      // Prometheus scrape. It sat on the public routes while AdminMetrics went
      // unchecked, so anyone who could reach the listener could read it.
      case GET -> Root / "metrics" as client =>
        guard(client, Permission.AdminMetrics)(scrapeMetrics)

      // The auth middleware answers an empty 404 itself for a path no route
      // here matches, so the fallback in toHttpApp never sees it.
      case req as _ => Routes.noRoute(req).pure[F]
    }

  private def quotaDisabled: F[Response[F]] =
    NotFound(ApiError.body(ApiError.NotFound, "Token quotas are not enabled"))

  private def guard(client: AuthenticatedClient, permission: Permission)(
      route: => F[Response[F]],
  ): F[Response[F]] = ApiKeyAuth.requirePermission(client, permission)(route)

  // We build the response by writing raw UTF-8 bytes to the body stream
  // instead of `withEntity(body)` / `Ok(body)`. The wildcard
  // `org.http4s.circe.CirceEntityEncoder.*` import in this file otherwise
  // resolves an `EntityEncoder[F, String]` that JSON-encodes the payload
  // (wrapping it in quotes and escaping newlines), which Prometheus
  // rejects with: expected a valid start token, got "\"".
  private def scrapeMetrics: F[Response[F]] = prometheusMetrics match
    case Some(prom) => prom.scrape.map { body =>
        val bytes = body.getBytes(StandardCharsets.UTF_8)
        Response[F](status = Status.Ok)
          .withBodyStream(Stream.emits(bytes).covary[F]).withContentType(
            `Content-Type`(org.http4s.MediaType.text.plain, Charset.`UTF-8`),
          ).putHeaders(org.http4s.headers.`Content-Length`.unsafeFromLong(
            bytes.length.toLong,
          ))
      }
    case None => NotFound(
        ApiError.body(ApiError.NotFound, "Prometheus metrics are not enabled"),
      )

  // Combined routes — public probes first so health checks never hit auth
  val routes: HttpRoutes[F] = TracingMiddleware[F](
    publicRoutes <+> dashboardApi.fold(HttpRoutes.empty[F])(_.routes) <+>
      authMiddleware(authedRoutes),
  )

  def httpApp: HttpApp[F] = Routes.toHttpApp(routes, logger)

// API models
case class HealthResponse(status: String, version: String)
case class ReadyResponse(
    status: String,
    checks: Map[String, Boolean],
    failing: Option[List[String]] = None,
)

object BuildInfo:
  val version = buildinfo.BuildInfo.version

object Routes:

  /** Close `routes` into an app whose every answer has a JSON error body.
    *
    * http4s' own `orNotFound` and `ErrorHandling` did this job and answered in
    * plain text: `Not found`, `The request body was malformed.`, and an empty
    * 500. Statuses are unchanged: 400 for a body that is not JSON, 422 for one
    * that does not match the schema, 404 for an unknown path, 500 otherwise.
    * Main and the tests both build their app here, so they cannot drift.
    */
  def toHttpApp[F[_]: Async](
      routes: HttpRoutes[F],
      logger: Logger[F],
  ): HttpApp[F] = cats.data.Kleisli { request =>
    val dsl = Http4sDsl[F]
    import dsl.*
    routes.run(request).getOrElse(noRoute(request)).handleErrorWith {
      case failure: MessageFailure =>
        Response[F](failure.toHttpResponse[F](request.httpVersion).status)
          .withEntity(ApiError.body(ApiError.InvalidRequest, describe(failure)))
          .pure[F]
      case error => logger.error(error)(s"Unhandled error serving ${request
            .method} ${request.uri.path}") *> InternalServerError(ApiError.body(
          ApiError.InternalError,
          "The request failed unexpectedly; whether it took effect is unknown",
        ))
    }
  }

  private def noRoute[F[_]](request: Request[F]): Response[F] =
    Response[F](Status.NotFound).withEntity(ApiError.body(
      ApiError.NotFound,
      s"No route for ${request.method} ${request.uri.path}",
    ))

  // The path of the field that failed, never its value: the body is the
  // caller's, but it has no place in a log line or an error another system
  // may store.
  private def describe(failure: MessageFailure): String = failure match
    case InvalidMessageBodyFailure(
          _,
          Some(decoding: io.circe.DecodingFailure),
        ) =>
      val path = io.circe.CursorOp.opsToPath(decoding.history)
      if path.isEmpty then "The request body was invalid."
      else s"The request body was invalid at $path"
    case _: InvalidMessageBodyFailure => "The request body was invalid."
    case _: MalformedMessageBodyFailure =>
      "The request body was not valid JSON."
    case _ => "The request could not be read."

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
      dashboardApi <- dashboardEventQueue.traverse(queue =>
        DashboardApi.apply[F](
          rateLimitStore,
          rateLimitConfig,
          logger,
          Some(queue),
          eventPublisher,
        ),
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
