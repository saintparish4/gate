import scala.concurrent.duration.*

import org.http4s.ember.server.EmberServerBuilder
import org.http4s.server.Server
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.otel4s.oteljava.OtelJava
import org.typelevel.otel4s.trace.Tracer

import com.comcast.ip4s.*

import cats.effect.*
import cats.syntax.all.*
import api.Routes
import config.*
import core.*
import events.{BroadcastingEventPublisher, EventPublisher, KinesisPublisher}
import observability.CorrelationIdMiddleware
import resilience.HealthAggregator
import wiring.*

object Main extends IOApp:

  override def run(args: List[String]): IO[ExitCode] = application.useForever
    .as(ExitCode.Success)

  private def application: Resource[IO, Server] =
    for
      given Logger[IO] <- Resource.eval(Slf4jLogger.create[IO])
      _ <- Resource.eval(summon[Logger[IO]].info("Starting Gate..."))

      config <- Resource.eval(AppConfig.load[IO].onError(e =>
        summon[Logger[IO]].error(s"Refusing to start: ${e.getMessage}"),
      ))
      _ <- Resource
        .eval(summon[Logger[IO]].info(s"Configuration loaded: ${config.server
            .host}:${config.server.port}"))
      _ <- Resource.eval(summon[Logger[IO]].info(s"AWS: localstack=${config.aws
          .localstack}, dynamodbEndpoint=${config.aws.dynamodbEndpoint
          .getOrElse("(none)")}"))

      obs <- ObservabilityModule.resource[IO](config)
      _ <- Resource.eval(summon[Logger[IO]].info("Observability initialized"))

      underlyingEvents <- config.kinesis.enabled match
        case true =>
          for
            kinesisClient <- storage.AwsClients
              .kinesisClient[IO](config.aws, config.dynamodb)
            publisher <- KinesisPublisher
              .resource[IO](kinesisClient, config.kinesis, obs.metricsPublisher)
          yield publisher
        case false => Resource
            .pure[IO, EventPublisher[IO]](EventPublisher.noop[IO])

      // The dashboard queue only exists to feed the dashboard; with it off,
      // nothing drains it.
      dashboardQueue = Option.when(config.dashboard.enabled)(obs.dashboardQueue)
      eventPublisher = dashboardQueue
        .fold(underlyingEvents)(BroadcastingEventPublisher(underlyingEvents, _))

      stores <- StoreModule
        .resource[IO](config, obs.metricsPublisher, eventPublisher)
      _ <- Resource.eval(summon[Logger[IO]].info("Stores initialized"))

      given Tracer[IO] <- config.tracing.enabled match
        case true =>
          for
            otelJava <- OtelJava.autoConfigured[IO]()
            tracer <- Resource.eval(otelJava.tracerProvider.get("gate"))
          yield tracer
        case false => Resource.pure[IO, Tracer[IO]](Tracer.noop[IO])

      correlationLocal <- Resource.eval(CorrelationIdMiddleware.makeLocal)

      tokenQuotaApi <- Resource.eval(stores.tokenQuotaStore.traverse { tqs =>
        val svc = core.TokenQuotaService[IO](
          tqs,
          config.tokenQuota,
          obs.metricsPublisher,
          summon[Logger[IO]],
        )
        IO.pure(api.TokenQuotaApi[IO](
          svc,
          eventPublisher,
          obs.metricsPublisher,
          summon[Logger[IO]],
          () =>
            correlationLocal.get
              .map(_.getOrElse(java.util.UUID.randomUUID().toString)),
        ))
      })

      security <- SecurityModule.resource[IO](config)
      _ <- Resource.eval(summon[Logger[IO]].info("Security initialized"))

      // The quota table is required when quotas are on; it was missing, so
      // /ready said ready while /v1/quota/check could not reach its table.
      healthSources = List(
        HealthAggregator.dynamoDbSource(
          "dynamodb_ratelimit",
          stores.rateLimitStore.healthCheck,
        ),
        HealthAggregator.dynamoDbSource(
          "dynamodb_idempotency",
          stores.idempotencyStore.healthCheck,
        ),
      ) ++ stores.tokenQuotaStore.map(tqs =>
        HealthAggregator.dynamoDbSource("dynamodb_quota", tqs.healthCheck),
      ) :+ HealthAggregator.kinesisSource(eventPublisher.healthCheck)
      healthCheck = HealthAggregator
        .aggregate(healthSources, config.resilience.timeout.healthCheck)

      getRequestId = () =>
        correlationLocal.get
          .map(_.getOrElse(java.util.UUID.randomUUID().toString))

      routes <- Resource.eval(Routes[IO](
        stores.resilientStore,
        stores.idempotencyStore,
        eventPublisher,
        obs.metricsPublisher,
        security.authMiddleware,
        config.rateLimit,
        config.idempotency,
        summon[Logger[IO]],
        dashboardQueue,
        tokenQuotaApi,
        obs.promMetrics,
        healthCheck,
        getRequestId,
      ))

      // toHttpApp turns a MessageFailure into a 4xx (400 for a body that is
      // not JSON, 422 for one that does not match the schema); without it an
      // undecodable body surfaces as an empty 500. The correlation middleware
      // goes around that, so those answers carry X-Request-Id too.
      httpApp: org.http4s.HttpApp[IO] = Routes.toHttpApp(
        routes.routes,
        summon[Logger[IO]],
        CorrelationIdMiddleware.middleware(correlationLocal),
      )

      // Last before the port opens, so a task that answers has run every path.
      _ <- Resource.eval(wiring.Warmup.run[IO](
        stores.rateLimitStore,
        stores.rawIdempotencyStore,
        stores.rawTokenQuotaStore,
        config.rateLimit,
        config.idempotency,
        config.tokenQuota,
        summon[Logger[IO]],
        if config.aws.localstack then wiring.Warmup.Settings.localStack
        else wiring.Warmup.Settings(),
      ))

      _ <- Resource.make(Async[IO].unit)(_ =>
        summon[Logger[IO]]
          .info("Shutdown signal received -- draining in-flight requests"),
      )

      server <- EmberServerBuilder.default[IO]
        .withHost(Host.fromString(config.server.host).getOrElse(host"0.0.0.0"))
        .withPort(Port.fromInt(config.server.port).getOrElse(port"8080"))
        .withHttpApp(httpApp).withShutdownTimeout(config.server.shutdownTimeout)
        .build
      _ <- Resource.eval(summon[Logger[IO]].info(s"Server started on ${config
          .server.host}:${config.server.port}"))
    yield server
