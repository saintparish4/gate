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

  val requestsTotal: Counter = Counter.build().name("gate_requests_total")
    .help("Total rate limit requests").labelNames("key", "result")
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

  val tokensConsumed: Gauge = Gauge.build().name("gate_tokens_consumed")
    .help("Token quota consumed gauge").labelNames("user", "agent", "org")
    .register(registry)

  val circuitBreakerState: Gauge = Gauge.build()
    .name("gate_circuit_breaker_state")
    .help("Circuit breaker state (0=closed, 0.5=half-open, 1=open)")
    .labelNames("name").register(registry)

  // -- Histograms --

  val dynamoLatency: Histogram = Histogram.build()
    .name("gate_dynamodb_latency_seconds").help("DynamoDB operation latency")
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

  // -- Cache --

  val idempotencyCacheHitRatio: Gauge = Gauge.build()
    .name("gate_idempotency_cache_hit_ratio")
    .help("Idempotency cache hit ratio (0.0 - 1.0)").register(registry)

  /** Serialize all registered metrics to Prometheus text exposition format. */
  def scrape: F[String] = Sync[F].delay {
    val writer = new StringWriter(16384)
    TextFormat.write004(writer, registry.metricFamilySamples())
    writer.toString
  }

object PrometheusMetrics:
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
        case "RateLimitAllowed" => prom.requestsTotal
            .labels(dimensions.getOrElse("ClientTier", "unknown"), "allowed")
            .inc()
        case "RateLimitRejected" => prom.requestsTotal
            .labels(dimensions.getOrElse("ClientTier", "unknown"), "rejected")
            .inc()
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

    override def gauge(
        name: String,
        value: Double,
        dimensions: Map[String, String],
    ): F[Unit] = primary.gauge(name, value, dimensions) *> Sync[F].delay {
      name match
        case "gate_tokens_consumed" => prom.tokensConsumed.labels(
            dimensions.getOrElse("user", ""),
            dimensions.getOrElse("agent", ""),
            dimensions.getOrElse("org", ""),
          ).set(value)
        case "CircuitBreakerState" => prom.circuitBreakerState
            .labels(dimensions.getOrElse("CircuitBreaker", "unknown")).set(value)
        case "CacheHitRate"
            if dimensions.get("CacheName").contains("idempotency") =>
          prom.idempotencyCacheHitRatio.set(value / 100.0)
        case _ => ()
    }

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
          case n if n.startsWith("dynamodb") =>
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
    ): F[Unit] = primary.recordRateLimitDecision(allowed, clientId, tier)

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

    override def recordCacheMetrics(
        cacheName: String,
        hitRate: Double,
        size: Long,
    ): F[Unit] = primary.recordCacheMetrics(cacheName, hitRate, size)

    override def recordDegradedOperation(operation: String): F[Unit] = primary
      .recordDegradedOperation(operation)

    override def flush: F[Unit] = primary.flush
