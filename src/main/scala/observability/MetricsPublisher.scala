package observability

import java.time.Instant

import scala.collection.immutable.Queue
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

import org.typelevel.log4cats.Logger

import cats.FlatMap
import cats.effect.*
import cats.effect.syntax.all.*
import cats.syntax.all.*
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.cloudwatch.model.*

case class MetricsConfig(
    namespace: String = "RateLimiter",
    environment: String = "dev",
    flushInterval: FiniteDuration = 60.seconds,
    batchSize: Int = 20,
    enabled: Boolean = true,
    highResolution: Boolean = false,
    maxBufferSize: Int = 50000,
    flushThreshold: Int = 1000,
)

object MetricsConfig:
  val default: MetricsConfig = MetricsConfig()

  val production: MetricsConfig = MetricsConfig(
    namespace = "RateLimiter/Production",
    flushInterval = 30.seconds,
    highResolution = true,
  )

case class MetricDataPoint(
    name: String,
    value: Double,
    unit: StandardUnit,
    dimensions: Map[String, String] = Map.empty,
    timestamp: Option[Instant] = None,
)

/** Internal buffer state: Queue for O(1) enqueue/dequeue, separate size counter
  * to avoid O(n) Queue.size calls on every metric write.
  */
private[observability] case class BufferState(
    queue: Queue[MetricDataPoint] = Queue.empty,
    size: Int = 0,
):
  def enqueue(point: MetricDataPoint, maxSize: Int): BufferState =
    if size >= maxSize then
      val (_, trimmed) = queue.dequeue
      BufferState(trimmed.enqueue(point), size)
    else BufferState(queue.enqueue(point), size + 1)

  def drainAll: (BufferState, List[MetricDataPoint]) =
    (BufferState(), queue.toList)

trait MetricsPublisher[F[_]: Clock: FlatMap]:
  def increment(
      name: String,
      dimensions: Map[String, String] = Map.empty,
  ): F[Unit]

  /** Add `amount` to a count, for quantities larger than one per event. */
  def count(
      name: String,
      amount: Double,
      dimensions: Map[String, String] = Map.empty,
  ): F[Unit]

  def gauge(
      name: String,
      value: Double,
      dimensions: Map[String, String] = Map.empty,
  ): F[Unit]

  def recordLatency(
      name: String,
      latencyMs: Double,
      dimensions: Map[String, String] = Map.empty,
  ): F[Unit]

  /** Default implementation; implementations need only provide recordLatency.
    */
  def timed[A](name: String, dimensions: Map[String, String] = Map.empty)(
      fa: F[A],
  ): F[A] =
    for
      start <- Clock[F].monotonic
      result <- fa
      end <- Clock[F].monotonic
      latencyMs = (end - start).toMillis.toDouble
      _ <- recordLatency(name, latencyMs, dimensions)
    yield result

  def recordRateLimitDecision(
      allowed: Boolean,
      clientId: String,
      tier: String = "unknown",
  ): F[Unit]

  def recordCircuitBreakerState(
      name: String,
      state: String,
      failureCount: Int,
  ): F[Unit]

  def recordCacheMetrics(cacheName: String, hitRate: Double, size: Long): F[Unit]

  def recordDegradedOperation(operation: String): F[Unit]

  def flush: F[Unit]

object MetricsPublisher:

  def cloudWatch[F[_]: Async: Logger](
      client: CloudWatchAsyncClient,
      config: MetricsConfig,
  ): Resource[F, MetricsPublisher[F]] =
    val logger = Logger[F]
    for
      bufferRef <- Resource.eval(Ref.of[F, BufferState](BufferState()))
      lastFlushRef <- Resource.eval(Ref.of[F, Long](System.currentTimeMillis()))
      flushingRef <- Resource.eval(Ref.of[F, Boolean](false))
      _ <-
        if config.enabled then
          Resource.make(
            flushLoop(bufferRef, lastFlushRef, flushingRef, client, config)
              .start,
          )(fiber =>
            fiber.cancel *> doFlush(
              bufferRef,
              lastFlushRef,
              flushingRef,
              client,
              config,
              logger,
            ),
          )
        else Resource.pure[F, Fiber[F, Throwable, Nothing]](null)
    yield new CloudWatchMetricsPublisher[F](
      client,
      config,
      bufferRef,
      lastFlushRef,
      flushingRef,
    )

  def cloudWatch[F[_]: Async: Logger](
      region: String,
      config: MetricsConfig,
  ): Resource[F, MetricsPublisher[F]] =
    import software.amazon.awssdk.regions.Region
    import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider

    val clientResource = Resource.make(Async[F].delay(
      CloudWatchAsyncClient.builder().region(Region.of(region))
        .credentialsProvider(DefaultCredentialsProvider.create()).build(),
    ))(client => Async[F].delay(client.close()))

    clientResource.flatMap(client => cloudWatch(client, config))

  def cloudWatch[F[_]: Async: Logger](
      region: String,
      namespace: String,
  ): Resource[F, MetricsPublisher[F]] =
    cloudWatch(region, MetricsConfig(namespace = namespace))

  def noop[F[_]: Async]: MetricsPublisher[F] = new MetricsPublisher[F]:
    override def increment(
        name: String,
        dimensions: Map[String, String],
    ): F[Unit] = Async[F].unit
    override def count(
        name: String,
        amount: Double,
        dimensions: Map[String, String],
    ): F[Unit] = Async[F].unit
    override def gauge(
        name: String,
        value: Double,
        dimensions: Map[String, String],
    ): F[Unit] = Async[F].unit
    override def recordLatency(
        name: String,
        latencyMs: Double,
        dimensions: Map[String, String],
    ): F[Unit] = Async[F].unit
    override def recordRateLimitDecision(
        allowed: Boolean,
        clientId: String,
        tier: String,
    ): F[Unit] = Async[F].unit
    override def recordCircuitBreakerState(
        name: String,
        state: String,
        failureCount: Int,
    ): F[Unit] = Async[F].unit
    override def recordCacheMetrics(
        cacheName: String,
        hitRate: Double,
        size: Long,
    ): F[Unit] = Async[F].unit
    override def recordDegradedOperation(operation: String): F[Unit] = Async[F]
      .unit
    override def flush: F[Unit] = Async[F].unit

  private def flushLoop[F[_]: Async: Logger](
      bufferRef: Ref[F, BufferState],
      lastFlushRef: Ref[F, Long],
      flushingRef: Ref[F, Boolean],
      client: CloudWatchAsyncClient,
      config: MetricsConfig,
  ): F[Nothing] =
    val logger = Logger[F]
    (Async[F].sleep(config.flushInterval) *>
      doFlush(bufferRef, lastFlushRef, flushingRef, client, config, logger))
      .foreverM

  def doFlush[F[_]: Async](
      bufferRef: Ref[F, BufferState],
      lastFlushRef: Ref[F, Long],
      flushingRef: Ref[F, Boolean],
      client: CloudWatchAsyncClient,
      config: MetricsConfig,
      logger: Logger[F],
  ): F[Unit] =
    // CAS guard: only one flush at a time. If another flush is in progress,
    // skip this one — the periodic timer or next threshold trigger will retry.
    flushingRef.getAndSet(true).flatMap {
      case true => Async[F].unit // another flush is already running
      case false =>
        val work =
          for
            metrics <- bufferRef.modify(_.drainAll)
            _ <-
              if metrics.nonEmpty then
                publishMetrics(client, config, metrics, logger) *>
                  lastFlushRef.set(System.currentTimeMillis())
              else Async[F].unit
          yield ()
        work.guarantee(flushingRef.set(false))
    }

  private def publishMetrics[F[_]: Async](
      client: CloudWatchAsyncClient,
      config: MetricsConfig,
      metrics: List[MetricDataPoint],
      logger: Logger[F],
  ): F[Unit] =
    val metricData = metrics.map { point =>
      val builder = MetricDatum.builder().metricName(point.name)
        .value(point.value).unit(point.unit)
        .timestamp(point.timestamp.getOrElse(Instant.now()))

      val dims = (point.dimensions + ("Environment" -> config.environment))
        .map { case (k, v) => Dimension.builder().name(k).value(v).build() }
        .toList

      if dims.nonEmpty then builder.dimensions(dims.asJava)

      if config.highResolution then builder.storageResolution(1)

      builder.build()
    }

    val batches = metricData.grouped(config.batchSize).toList

    batches.traverse_ { batch =>
      val request = PutMetricDataRequest.builder().namespace(config.namespace)
        .metricData(batch.asJava).build()

      Async[F].fromCompletableFuture(Async[F].delay(client.putMetricData(request)))
        .void.handleErrorWith(error =>
          logger.error(error)("Failed to publish metrics to CloudWatch"),
        )
    }

private class CloudWatchMetricsPublisher[F[_]: Async: Logger](
    client: CloudWatchAsyncClient,
    config: MetricsConfig,
    bufferRef: Ref[F, BufferState],
    lastFlushRef: Ref[F, Long],
    flushingRef: Ref[F, Boolean],
) extends MetricsPublisher[F]:

  private val logger = Logger[F]

  override def increment(
      name: String,
      dimensions: Map[String, String],
  ): F[Unit] =
    addMetric(MetricDataPoint(name, 1.0, StandardUnit.COUNT, dimensions))

  override def count(
      name: String,
      amount: Double,
      dimensions: Map[String, String],
  ): F[Unit] =
    addMetric(MetricDataPoint(name, amount, StandardUnit.COUNT, dimensions))

  override def gauge(
      name: String,
      value: Double,
      dimensions: Map[String, String],
  ): F[Unit] =
    addMetric(MetricDataPoint(name, value, StandardUnit.NONE, dimensions))

  override def recordLatency(
      name: String,
      latencyMs: Double,
      dimensions: Map[String, String],
  ): F[Unit] = addMetric(
    MetricDataPoint(name, latencyMs, StandardUnit.MILLISECONDS, dimensions),
  )

  override def recordRateLimitDecision(
      allowed: Boolean,
      clientId: String,
      tier: String,
  ): F[Unit] =
    val metricName = if allowed then "RateLimitAllowed" else "RateLimitRejected"
    val dims = Map(
      "ClientTier" -> tier,
      "Decision" -> (if allowed then "allowed" else "rejected"),
    )
    increment(metricName, dims) *> increment("RateLimitDecisions", dims)

  override def recordCircuitBreakerState(
      name: String,
      state: String,
      failureCount: Int,
  ): F[Unit] =
    val dims = Map("CircuitBreaker" -> name, "State" -> state)
    gauge("CircuitBreakerState", stateToValue(state), dims) *>
      gauge("CircuitBreakerFailures", failureCount.toDouble, dims)

  override def recordCacheMetrics(
      cacheName: String,
      hitRate: Double,
      size: Long,
  ): F[Unit] =
    val dims = Map("CacheName" -> cacheName)
    gauge("CacheHitRate", hitRate * 100, dims) *>
      gauge("CacheSize", size.toDouble, dims)

  override def recordDegradedOperation(operation: String): F[Unit] =
    increment("DegradedOperation", Map("Operation" -> operation))

  override def flush: F[Unit] = MetricsPublisher
    .doFlush(bufferRef, lastFlushRef, flushingRef, client, config, logger)

  private def addMetric(metric: MetricDataPoint): F[Unit] =
    if config.enabled then
      bufferRef.update(_.enqueue(metric, config.maxBufferSize))
        .flatMap(_ => triggerFlushIfNeeded)
    else Async[F].unit

  private def triggerFlushIfNeeded: F[Unit] = bufferRef.get.flatMap(buf =>
    if buf.size >= config.flushThreshold then
      MetricsPublisher
        .doFlush(bufferRef, lastFlushRef, flushingRef, client, config, logger)
        .start.void
    else Async[F].unit,
  )

  private def stateToValue(state: String): Double = state.toLowerCase match
    case "closed" => 0.0
    case "halfopen" | "half_open" => 0.5
    case "open" => 1.0
    case _ => -1.0

object LoggingMetrics:
  def apply[F[_]: Async: Logger]: F[MetricsPublisher[F]] =
    val logger = Logger[F]

    Async[F].pure {
      new MetricsPublisher[F]:
        override def increment(
            name: String,
            dimensions: Map[String, String],
        ): F[Unit] = logger.info(s"METRIC: $name +1 ${formatDims(dimensions)}")

        override def count(
            name: String,
            amount: Double,
            dimensions: Map[String, String],
        ): F[Unit] = logger
          .info(s"METRIC: $name +$amount ${formatDims(dimensions)}")

        override def gauge(
            name: String,
            value: Double,
            dimensions: Map[String, String],
        ): F[Unit] = logger
          .info(s"METRIC: $name = $value ${formatDims(dimensions)}")

        override def recordLatency(
            name: String,
            latencyMs: Double,
            dimensions: Map[String, String],
        ): F[Unit] = logger.info(
          s"METRIC: $name latency=${latencyMs}ms ${formatDims(dimensions)}",
        )

        override def recordRateLimitDecision(
            allowed: Boolean,
            clientId: String,
            tier: String,
        ): F[Unit] = logger.info(s"METRIC: RateLimit decision=${
            if allowed then "allowed" else "rejected"
          } client=$clientId tier=$tier")

        override def recordCircuitBreakerState(
            name: String,
            state: String,
            failureCount: Int,
        ): F[Unit] = logger.info(
          s"METRIC: CircuitBreaker name=$name state=$state failures=$failureCount",
        )

        override def recordCacheMetrics(
            cacheName: String,
            hitRate: Double,
            size: Long,
        ): F[Unit] = logger
          .info(s"METRIC: Cache name=$cacheName hitRate=${hitRate *
              100}% size=$size")

        override def recordDegradedOperation(operation: String): F[Unit] =
          logger.warn(s"METRIC: DegradedOperation operation=$operation")

        override def flush: F[Unit] = Async[F].unit

        private def formatDims(dims: Map[String, String]): String =
          if dims.isEmpty then ""
          else dims.map { case (k, v) => s"$k=$v" }.mkString("[", ", ", "]")
    }
