package observability

import org.http4s.*

import cats.data.{Kleisli, OptionT}
import cats.effect.*
import cats.syntax.all.*
import api.RateLimitApi

/** Counts and times every answer, for the SLOs (docs/SLO.md).
  *
  * It goes in `Routes.toHttpApp`'s `around`, so it sees the 404s, 400s and 500s
  * that `toHttpApp` itself writes, which a middleware around the routes alone
  * would miss.
  */
object HttpMetrics:

  /** The routes a client's decisions go through: the SLOs' denominator. */
  val DecisionRoutes: List[String] = List(
    "/v1/ratelimit/check",
    "/v1/ratelimit/status/{key}",
    "/v1/idempotency/check",
    "/v1/idempotency/{key}/complete",
    "/v1/idempotency/{key}/fail",
    "/v1/quota/check",
    "/v1/quota/reconcile",
  )

  /** The route template, so a key in the path never becomes a label. Anything
    * that is not a route is "other": a scanner cannot add series.
    */
  def route(path: Uri.Path): String = path.segments.map(_.decoded()) match
    case Vector("v1", "ratelimit", "check") => "/v1/ratelimit/check"
    case Vector("v1", "ratelimit", "status", _) => "/v1/ratelimit/status/{key}"
    case Vector("v1", "idempotency", "check") => "/v1/idempotency/check"
    case Vector("v1", "idempotency", _, "complete") =>
      "/v1/idempotency/{key}/complete"
    case Vector("v1", "idempotency", _, "fail") => "/v1/idempotency/{key}/fail"
    case Vector("v1", "quota", "check") => "/v1/quota/check"
    case Vector("v1", "quota", "reconcile") => "/v1/quota/reconcile"
    case Vector("health") => "/health"
    case Vector("ready") => "/ready"
    case Vector("metrics") => "/metrics"
    case "dashboard" +: _ => "/dashboard"
    case Vector("v1", "ratelimit", "dashboard", "stats") => "/dashboard"
    case _ => "other"

  def middleware[F[_]: Async](
      prom: PrometheusMetrics[F],
  )(routes: HttpRoutes[F]): HttpRoutes[F] = Kleisli { (req: Request[F]) =>
    val template = route(req.uri.path)
    def record(code: Int, degraded: Boolean, started: Long): F[Unit] = Clock[F]
      .monotonic.flatMap(now =>
        Sync[F].delay {
          prom.httpRequests.labels(template, code.toString, degraded.toString)
            .inc()
          prom.httpDuration.labels(template)
            .observe((now.toNanos - started) / 1e9)
        },
      )
    // In flight from request to response headers: the server's side of a
    // Little's Law check against the client's throughput and latency.
    val inFlight = Resource.make(Sync[F].delay(prom.httpInFlight.inc()))(_ =>
      Sync[F].delay(prom.httpInFlight.dec()),
    )
    OptionT(
      inFlight.surround(Clock[F].monotonic.map(_.toNanos).flatMap(started =>
        routes(req).value.flatTap {
          case Some(resp) => record(
              resp.status.code,
              resp.headers.get(RateLimitApi.DegradedHeader).isDefined,
              started,
            )
          case None => Sync[F].unit
        }.onError(_ => record(500, degraded = false, started)),
      )),
    )
  }
