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
      apiKeyStore <- config.security match
        case security if security.secrets.enabled =>
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
        case security if security.allowBuiltInKeys =>
          Resource.eval(
            Logger[F].warn("Serving the built-in API keys (ALLOW_BUILT_IN_KEYS=true). They are public and include an admin key: local development only.")
              .as(ApiKeyStore.inMemory[F](ApiKeyStore.testKeys)),
          )
        // AppConfig.validate already refuses this. I check again here because
        // this is where the keys are chosen, and a config built in code skips
        // validate.
        case security => Resource
            .raiseError[F, ApiKeyStore[F], Throwable](new IllegalStateException(
              AppConfig.validateKeySource(security)
                .getOrElse("no API key source"),
            ))

      authRateLimiter <- Resource.eval(AuthRateLimiter.inMemory[F](
        maxRequestsPerMinute = config.security.authentication.rateLimitPerMinute,
        maxFailedAttemptsPerMinute =
          config.security.authentication.maxFailedAttempts,
      ))

      middleware = ApiKeyAuth.middleware[F](apiKeyStore, Some(authRateLimiter))
    yield SecurityModule(apiKeyStore, middleware)
