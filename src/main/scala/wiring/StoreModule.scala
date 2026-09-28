package wiring

import org.typelevel.log4cats.Logger

import cats.effect.*
import cats.syntax.all.*
import config.AppConfig
import core.*
import observability.MetricsPublisher
import events.EventPublisher
import storage.*
import resilience.*

case class StoreModule[F[_]](
    rateLimitStore: RateLimitStore[F],
    resilientStore: RateLimitStore[F],
    idempotencyStore: IdempotencyStore[F],
    tokenQuotaStore: Option[TokenQuotaStore[F]],
)

object StoreModule:
  def resource[F[_]: Async: Logger](
      config: AppConfig,
      metrics: MetricsPublisher[F],
      events: EventPublisher[F],
  ): Resource[F, StoreModule[F]] =
    for
      rateLimitStore <- config.storage.backend match
        case "in-memory" => Resource.eval(RateLimitStore.inMemory[F])
        case _ => config.rateLimit.algorithm match
            case "leaky-bucket" =>
              for
                client <- AwsClients
                  .dynamoDbClient[F](config.aws, config.dynamodb)
                store = LeakyBucketRateLimitStore[F](
                  client,
                  config.dynamodb.rateLimitTable,
                  metrics,
                )
              yield store
            case "sliding-window" =>
              for
                client <- AwsClients
                  .dynamoDbClient[F](config.aws, config.dynamodb)
                store = DynamoDBSlidingWindowStore[F](
                  client,
                  config.dynamodb.rateLimitTable,
                  metrics,
                )
              yield store
            case _ =>
              for
                client <- AwsClients
                  .dynamoDbClient[F](config.aws, config.dynamodb)
                store = new DynamoDBRateLimitStore[F](
                  client,
                  config.dynamodb.rateLimitTable,
                  metrics,
                )
              yield store

      resilientStore <- ResilientRateLimitStore[F](
        rateLimitStore,
        config.resilience,
        metrics,
        config.resilience.parsedDegradationMode,
      )

      rawIdempotencyStore <- config.storage.backend match
        case "in-memory" => Resource.eval(IdempotencyStore.inMemory[F])
        case _ =>
          for
            client <- AwsClients.dynamoDbClient[F](config.aws, config.dynamodb)
            store = new DynamoDBIdempotencyStore[F](
              client,
              config.dynamodb.idempotencyTable,
            )
          yield store
      idempotencyGuard <- StoreGuard.resource[F](
        "idempotency",
        config.resilience.timeout.idempotencyCheck,
        config.resilience.bulkhead,
      )
      idempotencyStore = StoreGuard
        .idempotency(rawIdempotencyStore, idempotencyGuard)

      quotaGuard <- StoreGuard.resource[F](
        "quota",
        config.resilience.timeout.quotaCheck,
        config.resilience.bulkhead,
      )
      rawTokenQuotaStore <-
        if config.tokenQuota.enabled then
          config.storage.backend match
            case "in-memory" => Resource
                .eval(TokenQuotaStore.inMemory[F].map(Some(_)))
            case _ =>
              for
                client <- AwsClients
                  .dynamoDbClient[F](config.aws, config.dynamodb)
                store = storage.DynamoDBTokenQuotaStore[F](
                  client,
                  config.tokenQuota.tableName,
                  summon[Logger[F]],
                  metrics,
                )
              yield Some(store)
        else Resource.pure[F, Option[TokenQuotaStore[F]]](None)
    yield StoreModule(
      rateLimitStore,
      resilientStore,
      idempotencyStore,
      rawTokenQuotaStore.map(StoreGuard.tokenQuota(_, quotaGuard)),
    )
