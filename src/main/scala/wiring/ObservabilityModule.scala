package wiring

import org.typelevel.log4cats.Logger

import cats.effect.*
import cats.effect.std.Queue
import cats.syntax.all.*
import config.AppConfig
import events.RateLimitEvent
import observability.{MetricsConfig, MetricsPublisher, PrometheusMetrics}

case class ObservabilityModule[F[_]](
    metricsPublisher: MetricsPublisher[F],
    promMetrics: Option[PrometheusMetrics[F]],
    dashboardQueue: Queue[F, RateLimitEvent],
)

object ObservabilityModule:
  def resource[F[_]: Async: Logger](
      config: AppConfig,
  ): Resource[F, ObservabilityModule[F]] =
    for
      basePublisher <- (config.metrics.enabled && !config.aws.localstack) match
        case true =>
          val logger = summon[Logger[F]]
          Resource.make(
            MetricsPublisher
              .cloudWatch[F](config.aws.region, cloudWatchConfig(config.metrics))
              .allocated,
          ) { case (publisher, releaseInner) =>
            logger.info("Flushing metrics buffer before shutdown...") *>
              publisher.flush *> logger.info("Metrics buffer flushed.") *>
              releaseInner
          }.map(_._1)
        case false => Resource
            .pure[F, MetricsPublisher[F]](MetricsPublisher.noop[F])

      promMetrics <- config.prometheus.enabled match
        case true => Resource.eval(PrometheusMetrics[F].map(Some(_)))
        case false => Resource.pure[F, Option[PrometheusMetrics[F]]](None)

      metricsPublisher = promMetrics match
        case Some(prom) => PrometheusMetrics.dual[F](basePublisher, prom)
        case None => basePublisher

      dashboardQueue <- Resource.eval(Queue.bounded[F, RateLimitEvent](512))
    yield ObservabilityModule(metricsPublisher, promMetrics, dashboardQueue)

  /** Only namespace and the buffer sizes used to be passed on, so the flush
    * interval and resolution were fixed at their defaults and every datum said
    * `Environment=dev`, in every environment.
    */
  def cloudWatchConfig(metrics: config.MetricsConfig): MetricsConfig =
    MetricsConfig(
      namespace = metrics.namespace,
      environment = metrics.environment,
      flushInterval = metrics.flushInterval,
      highResolution = metrics.highResolution,
      maxBufferSize = metrics.maxBufferSize,
      flushThreshold = metrics.flushThreshold,
    )
