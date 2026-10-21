package observability

import scala.concurrent.duration.*

import org.http4s.*
import org.http4s.dsl.io.*
import org.http4s.implicits.*
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.typelevel.ci.*

import cats.effect.*
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.testkit.TestControl

/** The split must be exact, so it is checked on virtual time: 50 ms in a store
  * call (one in a forked fiber), 20 ms in gate, nothing else.
  */
class RequestTimingSpec extends AsyncFreeSpec with AsyncIOSpec with Matchers:

  "RequestTiming" - {
    "reports store and gate time separately, counting store calls on forked fibers" in
      TestControl.executeEmbed {
        for
          local <- RequestTiming.makeLocal
          metrics = RequestTiming.publisher(MetricsPublisher.noop[IO], local)
          routes = HttpRoutes
            .of[IO] { case GET -> Root / "v1" / "ratelimit" / "status" / _ =>
              for
                _ <- IO.sleep(20.millis)
                _ <- metrics.timed("RateLimitStatusLatency")(IO.sleep(30.millis))
                fiber <- metrics
                  .timed("RateLimitCheckLatency")(IO.sleep(20.millis)).start
                _ <- fiber.join
                // Not a store timing: it is gate's time.
                _ <- metrics.timed("SomethingElse")(IO.unit)
                resp <- Ok("{}")
              yield resp
            }
          prom <- PrometheusMetrics[IO]
          app = RequestTiming.middleware(local, Some(prom), "replica-1")(routes)
            .orNotFound
          resp <- app.run(Request[IO](Method.GET, uri"/v1/ratelimit/status/k"))
          after <- local.get
        yield (resp, after, prom)
      }.asserting { case (resp, after, prom) =>
        resp.headers.get(ci"Server-Timing").map(_.head.value) shouldBe
          Some("store;dur=50.000, gate;dur=20.000")
        resp.headers.get(ci"X-Gate-Instance").map(_.head.value) shouldBe
          Some("replica-1")
        after shouldBe None
        prom.registry.getSampleValue(
          "gate_http_store_seconds_sum",
          Array("route"),
          Array("/v1/ratelimit/status/{key}"),
        ) shouldBe 0.05
      }
  }
