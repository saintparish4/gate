package observability

import java.io.StringWriter

import org.typelevel.log4cats.Logger

import cats.effect.*
import cats.syntax.all.*
import io.prometheus.client.*
import io.prometheus.client.exporter.common.TextFormat

/** Prometheus metrics registry for Gate
  *
  * All Prometheus metric objects are created once at startup and reused. The
  * `observe` / `inc` calls are thread-safe (Prometheus client uses CAS
  * internally), so no additional synchronization is needed.
  *
  * The `scrape` method serializes the registry into Prometheus text format for
  * the GET /metrics endpoint
  */
class PrometheusMetrics[F[_]: Sync](val registry: CollectorRegistry):

  // -- Counters --

  // `tier`, not the rate-limit key: keys are unbounded, tiers are four. It was
  // labelled `key` and fed from the store, which never knew either, so every
  // sample read "unknown".
  val requestsTotal: Counter = Counter.build().name("gate_requests_total")
    .help("Rate limit checks answered by the API").labelNames("tier", "result")
    .register(registry)

  val idempotencyTotal: Counter = Counter.build().name("gate_idempotency_total")
    .help("Total idempotency checks").labelNames("result").register(registry)

  val tokenQuotaTotal: Counter = Counter.build().name("gate_token_quota_total")
    .help("Total token quota checks").labelNames("level", "result")
    .register(registry)

  val eventsPublished: Counter = Counter.build()
    .name("gate_events_published_total").help("Total Kinesis events published")
    .labelNames("event_type").register(registry)

  val eventsDropped: Counter = Counter.build().name("gate_events_dropped_total")
    .help("Kinesis events dropped after retry exhaustion").register(registry)

  val degradedTotal: Counter = Counter.build().name("gate_degraded_total")
    .help("Decisions served by degradation mode instead of the store (breaker open, bulkhead full, or store error)")
    .labelNames("reason").register(registry)

  // -- Gauges --

  // Replaces a per-user gauge: one series per user in Prometheus and one
  // custom metric per user in CloudWatch, and it only moved on reconcile.
  val quotaTokensAdmitted: Counter = Counter.build()
    .name("gate_quota_tokens_admitted_total")
    .help("Tokens reserved by admitted quota checks (the pre-request estimate)")
    .labelNames("level").register(registry)

  val circuitBreakerState: Gauge = Gauge.build()
    .name("gate_circuit_breaker_state")
    .help("Circuit breaker state (0=closed, 0.5=half-open, 1=open)")
    .labelNames("name").register(registry)

  // Every answer the service gives, by route template, status and whether it
  // was a degraded one. The SLOs in docs/SLO.md are ratios of these; the
  // decision counters above only ever saw rate-limit checks, and no 5xx.
  val httpRequests: Counter = Counter.build().name("gate_http_requests_total")
    .help("HTTP answers by route template, status code, and X-Gate-Degraded")
    .labelNames("route", "code", "degraded").register(registry)

  // Sampled every second by Main, for the overload characterization
  // (docs/perf): where requests wait is where overload shows first.
  val httpInFlight: Gauge = Gauge.build().name("gate_http_in_flight")
    .help("Requests being answered right now, across all routes")
    .register(registry)

  val eventQueueDepth: Gauge = Gauge.build().name("gate_events_queue_depth")
    .help("Events waiting to be put to Kinesis; the oldest are evicted at kinesis.queue-size")
    .register(registry)

  val bulkheadInFlight: Gauge = Gauge.build().name("gate_bulkhead_in_flight")
    .help("Calls holding a bulkhead permit").labelNames("name")
    .register(registry)

  val bulkheadQueued: Gauge = Gauge.build().name("gate_bulkhead_queued")
    .help("Calls waiting for a bulkhead permit, up to its max-wait")
    .labelNames("name").register(registry)

  // -- Histograms --

  // The part of each answer spent in store calls, OCC retries included (see
  // RequestTiming). Request time less this is time in gate itself.
  val httpStoreDuration: Histogram = Histogram.build()
    .name("gate_http_store_seconds")
    .help("Time each answer spent waiting on the store, by route template")
    .labelNames("route").buckets(
      0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
    ).register(registry)

  // Buckets include 0.1: the latency objective's threshold must be one.
  val httpDuration: Histogram = Histogram.build()
    .name("gate_http_request_duration_seconds")
    .help("Time from request to response headers, by route template")
    .labelNames("route")
    .buckets(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)
    .register(registry)

  val dynamoLatency: Histogram = Histogram.build()
    .name("gate_dynamodb_latency_seconds")
    .help("Store call latency, including any conditional-write retries")
    .labelNames("operation")
    .buckets(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0)
    .register(registry)

  val rateLimitCheckLatency: Histogram = Histogram.build()
    .name("gate_rate_limit_check_seconds").help("Rate limit check latency")
    .buckets(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5).register(registry)

  val idempotencyCheckLatency: Histogram = Histogram.build()
    .name("gate_idempotency_check_seconds").help("Idempotency check latency")
    .buckets(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5).register(registry)

  val tokenQuotaCheckLatency: Histogram = Histogram.build()
    .name("gate_token_quota_check_seconds").help("Token quota check latency")
    .buckets(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5).register(registry)

  // A labelled series does not exist until its first sample, so a quiet one
  // reads as "No data" rather than zero. I create the bounded ones up front.
  List("free", "basic", "premium", "enterprise").foreach(tier =>
    List("allowed", "rejected").foreach(requestsTotal.labels(tier, _)),
  )
  List("new", "in_progress", "duplicate", "conflict", "error")
    .foreach(idempotencyTotal.labels(_))
  List("circuit_breaker", "bulkhead", "error").foreach(degradedTotal.labels(_))
  List("user", "agent", "org").foreach(quotaTokensAdmitted.labels(_))
  List("checkAndConsume", "getStatus", "idempotency_check", "quota_reserve")
    .foreach(dynamoLatency.labels(_))
  // The failures the availability SLO counts, per decision route. A series
  // born at its first 503 has no earlier sample, so rate() never sees that
  // first jump: on the first local multi-replica run, three 503s read as an
  // error ratio of exactly zero.
  HttpMetrics.DecisionRoutes.foreach { route =>
    List("500", "503").foreach(httpRequests.labels(route, _, "false"))
    httpDuration.labels(route)
  }
  List("200", "429")
    .foreach(httpRequests.labels("/v1/ratelimit/check", _, "true"))

  /** Serialize all registered metrics to Prometheus text exposition format. */
  def scrape: F[String] = Sync[F].delay {
    val writer = new StringWriter(16384)
    TextFormat.write004(writer, registry.metricFamilySamples())
    writer.toString
  }

object PrometheusMetrics:

  // Timings taken around a store call rather than a whole request. These are
  // the names the stores and APIs already give CloudWatch.
  private val storeLatencyNames = Set(
    "RateLimitCheckLatency",
    "RateLimitStatusLatency",
    "IdempotencyStoreLatency",
    "TokenQuotaStoreLatency",
  )

  def apply[F[_]: Sync]: F[PrometheusMetrics[F]] = Sync[F].delay {
    val registry = new CollectorRegistry(true)
    new PrometheusMetrics[F](registry)
  }

  /** Wraps an existing MetricsPublisher to also record into Prometheus.
    *
    * CloudWatch remains the primary sink; Prometheus mirrors the same data
    * points so the /metrics endpoint is consistent.
    */
  def dual[F[_]: Async: Logger](
      primary: MetricsPublisher[F],
      prom: PrometheusMetrics[F],
  ): MetricsPublisher[F] = new MetricsPublisher[F]:

    override def increment(
        name: String,
        dimensions: Map[String, String],
    ): F[Unit] = primary.increment(name, dimensions) *> Sync[F].delay {
      name match
        case "IdempotencyCheck" => prom.idempotencyTotal
            .labels(dimensions.getOrElse("result", "unknown")).inc()
        case "KinesisEventPublished" => prom.eventsPublished
            .labels(dimensions.getOrElse("event_type", "unknown")).inc()
        case "TokenQuotaExceeded" => prom.tokenQuotaTotal
            .labels(dimensions.getOrElse("level", "unknown"), "exceeded").inc()
        case "RateLimitDegraded" => prom.degradedTotal
            .labels(dimensions.getOrElse("reason", "unknown")).inc()
        case "TokenQuotaContended" => prom.tokenQuotaTotal
            .labels("all", "contended").inc()
        case "TokenQuotaReconcileFailed" => prom.tokenQuotaTotal
            .labels("all", "reconcile_failed").inc()
        case "DroppedKinesisEvent" => prom.eventsDropped.inc()
        case _ => ()
    }

    override def count(
        name: String,
        amount: Double,
        dimensions: Map[String, String],
    ): F[Unit] = primary.count(name, amount, dimensions) *> Sync[F].delay(
      name match
        case "QuotaTokensAdmitted" => prom.quotaTokensAdmitted
            .labels(dimensions.getOrElse("level", "unknown")).inc(amount)
        case _ => (),
    )

    override def gauge(
        name: String,
        value: Double,
        dimensions: Map[String, String],
    ): F[Unit] = primary.gauge(name, value, dimensions) *> Sync[F].delay(
      name match
        case "CircuitBreakerState" => prom.circuitBreakerState
            .labels(dimensions.getOrElse("CircuitBreaker", "unknown")).set(value)
        case _ => (),
    )

    override def recordLatency(
        name: String,
        latencyMs: Double,
        dimensions: Map[String, String],
    ): F[Unit] = primary.recordLatency(name, latencyMs, dimensions) *>
      Sync[F].delay {
        val seconds = latencyMs / 1000.0
        name match
          case "rate_limit_check" => prom.rateLimitCheckLatency.observe(seconds)
          case "idempotency_check" => prom.idempotencyCheckLatency
              .observe(seconds)
          case "token_quota_check" => prom.tokenQuotaCheckLatency
              .observe(seconds)
          case n if storeLatencyNames.contains(n) =>
            prom.dynamoLatency.labels(dimensions.getOrElse("operation", name))
              .observe(seconds)
          case _ => ()
      }

    override def timed[A](name: String, dimensions: Map[String, String])(
        fa: F[A],
    ): F[A] =
      for
        start <- Clock[F].monotonic
        result <- fa
        end <- Clock[F].monotonic
        latencyMs = (end - start).toMillis.toDouble
        _ <- recordLatency(name, latencyMs, dimensions)
      yield result

    override def recordRateLimitDecision(
        allowed: Boolean,
        clientId: String,
        tier: String,
    ): F[Unit] = primary.recordRateLimitDecision(allowed, clientId, tier) *>
      Sync[F].delay(
        prom.requestsTotal
          .labels(tier, if allowed then "allowed" else "rejected").inc(),
      )

    override def recordCircuitBreakerState(
        name: String,
        state: String,
        failureCount: Int,
    ): F[Unit] = primary.recordCircuitBreakerState(name, state, failureCount) *>
      Sync[F].delay(prom.circuitBreakerState.labels(name).set(
        state.toLowerCase match
          case "closed" => 0.0
          case "halfopen" | "half_open" => 0.5
          case "open" => 1.0
          case _ => -1.0,
      ))

    override def flush: F[Unit] = primary.flush
