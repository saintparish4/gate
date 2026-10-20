package observability

import org.http4s.*
import org.http4s.dsl.io.*
import org.http4s.implicits.*
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.typelevel.log4cats.noop.NoOpLogger

import cats.effect.*
import cats.effect.testing.scalatest.AsyncIOSpec
import api.{RateLimitApi, Routes}

class HttpMetricsSpec extends AsyncFreeSpec with AsyncIOSpec with Matchers:

  "HttpMetrics.route" - {
    "names every route by its template, never by the key in it" in {
      val cases = Map(
        "/v1/ratelimit/check" -> "/v1/ratelimit/check",
        "/v1/ratelimit/status/user:123" -> "/v1/ratelimit/status/{key}",
        "/v1/idempotency/check" -> "/v1/idempotency/check",
        "/v1/idempotency/order-9/complete" -> "/v1/idempotency/{key}/complete",
        "/v1/idempotency/order-9/fail" -> "/v1/idempotency/{key}/fail",
        "/v1/quota/check" -> "/v1/quota/check",
        "/v1/quota/reconcile" -> "/v1/quota/reconcile",
        "/health" -> "/health",
        "/ready" -> "/ready",
        "/metrics" -> "/metrics",
        "/dashboard/api/stats" -> "/dashboard",
        "/wp-login.php" -> "other",
        "/v1/ratelimit/check/extra" -> "other",
      )
      cases.foreach((path, template) =>
        HttpMetrics.route(Uri.Path.unsafeFromString(path)) shouldBe template,
      )
      succeed
    }
  }

  private def count(
      prom: PrometheusMetrics[IO],
      route: String,
      code: String,
      degraded: String,
  ) = Option(prom.registry.getSampleValue(
    "gate_http_requests_total",
    Array("route", "code", "degraded"),
    Array(route, code, degraded),
  )).fold(0.0)(_.doubleValue)

  "PrometheusMetrics" - {
    // A counter series that appears at its first increment loses that increment
    // to rate(), so the failures the SLOs count must exist, at zero, from start.
    "exposes every decision route's failure series before any request" in
      PrometheusMetrics[IO].map { prom =>
        HttpMetrics.DecisionRoutes.foreach(route =>
          List("500", "503").foreach(code =>
            // Present at zero, not absent: `count` would read absent as zero.
            Option(prom.registry.getSampleValue(
              "gate_http_requests_total",
              Array("route", "code", "degraded"),
              Array(route, code, "false"),
            )) shouldBe Some(0.0),
          ),
        )
        Option(prom.registry.getSampleValue(
          "gate_http_requests_total",
          Array("route", "code", "degraded"),
          Array("/v1/ratelimit/check", "429", "true"),
        )) shouldBe Some(0.0)
      }
  }

  "HttpMetrics.middleware" - {
    "counts the answers toHttpApp writes, 5xx and degraded included" in {
      val routes = HttpRoutes.of[IO] {
        case POST -> Root / "v1" / "ratelimit" / "check" => TooManyRequests("{}")
            .map(_.putHeaders(Header.Raw(RateLimitApi.DegradedHeader, "true")))
        case POST -> Root / "v1" / "quota" / "check" => IO
            .raiseError(new RuntimeException("boom"))
      }
      for
        prom <- PrometheusMetrics[IO]
        app = Routes
          .toHttpApp(routes, NoOpLogger[IO], HttpMetrics.middleware(prom))
        _ <- app.run(Request[IO](Method.POST, uri"/v1/ratelimit/check"))
        failed <- app.run(Request[IO](Method.POST, uri"/v1/quota/check"))
        missing <- app.run(Request[IO](Method.GET, uri"/nowhere"))
      yield
        failed.status shouldBe Status.InternalServerError
        missing.status shouldBe Status.NotFound
        count(prom, "/v1/ratelimit/check", "429", "true") shouldBe 1.0
        count(prom, "/v1/quota/check", "500", "false") shouldBe 1.0
        count(prom, "other", "404", "false") shouldBe 1.0
        Option(prom.registry.getSampleValue(
          "gate_http_request_duration_seconds_count",
          Array("route"),
          Array("/v1/quota/check"),
        )).map(_.doubleValue) shouldBe Some(1.0)
    }
  }
