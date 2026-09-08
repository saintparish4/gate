package wiring

import org.http4s.server.AuthMiddleware
import org.typelevel.log4cats.Logger

import cats.effect.*
import cats.syntax.all.*
import config.AppConfig
import security.*

case class SecurityModule[F[_]](
    apiKeyStore: ApiKeyStore[F],
    authMiddleware: AuthMiddleware[F, AuthenticatedClient],
)

object SecurityModule:
  def resource[F[_]: Async: Logger](
      config: AppConfig,
  ): Resource[F, SecurityModule[F]] =
    for
      apiKeyStore <- config.security.secrets.enabled match
        case true =>
          for
            secretsClient <- SecretsManagerStore.clientResource[F](config.aws)
            secretsConfig = SecretsConfig(
              environment = config.security.secrets.environment,
              secretPrefix = config.security.secrets.secretPrefix,
              cacheTtl = config.security.secrets.cacheTtl,
              apiKeysSecretName = config.security.secrets.apiKeysSecretName,
            )
            secretStore <- Resource
              .eval(SecretsManagerStore[F](secretsClient, secretsConfig))
            store <- Resource.eval(SecretsManagerApiKeyStore[F](
              secretStore,
              config.security.secrets.cacheTtl,
            ))
          yield store
        case false => Resource.pure[F, ApiKeyStore[F]](
            ApiKeyStore.inMemory[F](ApiKeyStore.testKeys),
          )

      authRateLimiter <- Resource.eval(AuthRateLimiter.inMemory[F](
        maxRequestsPerMinute = config.security.authentication.rateLimitPerMinute,
        maxFailedAttemptsPerMinute =
          config.security.authentication.maxFailedAttempts,
      ))

      middleware = ApiKeyAuth.middleware[F](apiKeyStore, Some(authRateLimiter))
    yield SecurityModule(apiKeyStore, middleware)
