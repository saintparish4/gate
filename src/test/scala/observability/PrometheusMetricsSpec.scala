package observability

import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.noop.NoOpLogger

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all.*

/** What reaches /metrics from each MetricsPublisher call. Every panel on the
  * Grafana dashboard reads one of these series, and several used to be
  * registered but never fed, or fed with a label that was always "unknown".
  */
class PrometheusMetricsSpec
    extends AsyncFreeSpec with AsyncIOSpec with Matchers:

  given Logger[IO] = NoOpLogger[IO]

  private def withDual[A](
      f: (MetricsPublisher[IO], PrometheusMetrics[IO]) => IO[A],
  ): IO[A] = PrometheusMetrics[IO].flatMap(prom =>
    f(PrometheusMetrics.dual(MetricsPublisher.noop[IO], prom), prom),
  )

  private def sample(
      prom: PrometheusMetrics[IO],
      name: String,
      labels: (String, String)*,
  ): Option[Double] = Option(
    prom.registry
      .getSampleValue(name, labels.map(_._1).toArray, labels.map(_._2).toArray),
  ).map(_.doubleValue)

  "gate_requests_total" - {

    "is labelled by the caller's tier from the API decision" in
      withDual((pub, prom) =>
        pub.recordRateLimitDecision(allowed = true, "key_1", "free") *>
          pub.recordRateLimitDecision(allowed = false, "key_1", "free") *> IO((
            sample(
              prom,
              "gate_requests_total",
              "tier" -> "free",
              "result" -> "allowed",
            ),
            sample(
              prom,
              "gate_requests_total",
              "tier" -> "free",
              "result" -> "rejected",
            ),
          )),
      ).asserting(_ shouldBe (Some(1.0), Some(1.0)))

    "is not fed by the store-level counters, which know no tier" in
      withDual((pub, prom) =>
        pub.increment("RateLimitAllowed") *> IO(sample(
          prom,
          "gate_requests_total",
          "tier" -> "unknown",
          "result" -> "allowed",
        )),
      ).asserting(_ shouldBe None)
  }

  "previously dead series are fed" - {

    "gate_idempotency_total by result" in withDual((pub, prom) =>
      pub.increment("IdempotencyCheck", Map("result" -> "duplicate")) *>
        IO(sample(prom, "gate_idempotency_total", "result" -> "duplicate")),
    ).asserting(_ shouldBe Some(1.0))

    "gate_events_published_total by event type" in withDual((pub, prom) =>
      pub.increment(
        "KinesisEventPublished",
        Map("event_type" -> "rate_limit_allowed"),
      ) *> IO(sample(
        prom,
        "gate_events_published_total",
        "event_type" -> "rate_limit_allowed",
      )),
    ).asserting(_ shouldBe Some(1.0))

    "gate_dynamodb_latency_seconds from store-call timings" in
      withDual((pub, prom) =>
        pub.recordLatency(
          "RateLimitCheckLatency",
          12.0,
          Map("operation" -> "checkAndConsume"),
        ) *> IO(sample(
          prom,
          "gate_dynamodb_latency_seconds_count",
          "operation" -> "checkAndConsume",
        )),
      ).asserting(_ shouldBe Some(1.0))
  }

  "gate_quota_tokens_admitted_total adds the amount, per level" in
    withDual((pub, prom) =>
      pub.count("QuotaTokensAdmitted", 2500.0, Map("level" -> "user")) *>
        pub.count("QuotaTokensAdmitted", 500.0, Map("level" -> "user")) *>
        IO(sample(prom, "gate_quota_tokens_admitted_total", "level" -> "user")),
    ).asserting(_ shouldBe Some(3000.0))

  "bounded series exist at zero before any traffic, so panels show 0, not No data" in
    PrometheusMetrics[IO].map(prom =>
      List(
        sample(prom, "gate_degraded_total", "reason" -> "circuit_breaker"),
        sample(prom, "gate_idempotency_total", "result" -> "new"),
        sample(prom, "gate_quota_tokens_admitted_total", "level" -> "org"),
        sample(
          prom,
          "gate_requests_total",
          "tier" -> "enterprise",
          "result" -> "rejected",
        ),
      ),
    ).asserting(_ shouldBe List.fill(4)(Some(0.0)))
