package security

import scala.concurrent.duration.*
import scala.jdk.FutureConverters.*

import org.typelevel.log4cats.Logger

import cats.effect.*
import cats.syntax.all.*
import io.circe.*
import io.circe.generic.auto.*
import io.circe.parser.*
import software.amazon.awssdk.services.secretsmanager.SecretsManagerAsyncClient
import software.amazon.awssdk.services.secretsmanager.model.*

/** AWS Secrets Manager integration for secure credential storage.
  *
  * Provides:
  *   - Secure API key storage and retrieval
  *   - Automatic secret caching with TTL
  *   - Secret rotation support
  *   - Environment-aware secret naming
  */

/** Secret store trait for retrieving secrets.
  */
trait SecretStore[F[_]]:
  /** Get a secret value by name */
  def getSecret(secretName: String): F[Option[String]]

  /** Get a secret as JSON and parse it */
  def getSecretAs[A: Decoder](secretName: String): F[Option[A]]

  /** Get the active API keys. Fails when the keys secret is missing or is not a
    * JSON list of keys, so a misnamed secret cannot read as "no keys".
    */
  def getApiKeys: F[Map[String, AuthenticatedClient]]

/** Configuration for Secrets Manager.
  */
case class SecretsConfig(
    environment: String = "dev",
    secretPrefix: String = "rate-limiter",
    cacheTtl: FiniteDuration = 5.minutes,
    apiKeysSecretName: String = "api-keys",
):
  def fullSecretName(name: String): String = s"$secretPrefix/$environment/$name"

object SecretsConfig:
  val default: SecretsConfig = SecretsConfig()

/** Secret value with metadata.
  */
private case class CachedSecret(
    value: String,
    cachedAt: Long,
    versionId: Option[String],
)

/** API key configuration stored in Secrets Manager.
  *
  * Every field is required, `active` included. It used to default to true, but
  * the derived decoder ignores Scala defaults, so an entry without it failed to
  * parse and the whole secret silently read as empty.
  */
case class ApiKeyConfig(
    apiKey: String,
    apiKeyId: String,
    clientName: String,
    tier: String,
    permissions: List[String],
    active: Boolean,
)

/** AWS Secrets Manager implementation of SecretStore.
  */
object SecretsManagerStore:

  /** Create a Secrets Manager client as a Resource.
    */
  def clientResource[F[_]: Async](
      awsConfig: _root_.config.AwsConfig,
  ): Resource[F, SecretsManagerAsyncClient] =
    import software.amazon.awssdk.regions.Region
    import software.amazon.awssdk.auth.credentials.*
    import java.net.URI

    Resource.make(Async[F].delay {
      val builder = SecretsManagerAsyncClient.builder()
        .region(Region.of(awsConfig.region))

      if awsConfig.endpoint.nonEmpty then
        builder.endpointOverride(URI.create(awsConfig.endpoint))
          .credentialsProvider(StaticCredentialsProvider.create(
            AwsBasicCredentials.create("test", "test"),
          ))

      builder.build()
    })(client => Async[F].delay(client.close()))

  /** Create a SecretStore backed by AWS Secrets Manager.
    */
  def apply[F[_]: Async: Logger](
      client: SecretsManagerAsyncClient,
      config: SecretsConfig = SecretsConfig.default,
  ): F[SecretStore[F]] =
    for cacheRef <- Ref.of[F, Map[String, CachedSecret]](Map.empty)
    yield new SecretStore[F]:
      private val logger = Logger[F]

      override def getSecret(secretName: String): F[Option[String]] =
        val fullName = config.fullSecretName(secretName)

        // Check cache first
        cacheRef.get.flatMap(cache =>
          cache.get(fullName) match
            case Some(cached) => Clock[F].realTime.map(_.toMillis)
                .flatMap(now =>
                  if now - cached.cachedAt < config.cacheTtl.toMillis then
                    logger.debug(s"Cache hit for secret: $fullName") *>
                      Async[F].pure(Some(cached.value))
                  else fetchAndCache(fullName),
                )
            case None => fetchAndCache(fullName),
        )

      override def getSecretAs[A: Decoder](secretName: String): F[Option[A]] =
        getSecret(secretName).flatMap {
          case Some(json) => decode[A](json) match
              case Right(value) => Async[F].pure(Some(value))
              case Left(error) => logger
                  .error(s"Failed to parse secret $secretName: ${error
                      .getMessage}") *> Async[F].pure(None)
          case None => Async[F].pure(None)
        }

      // A missing or unparseable secret used to fall back to the built-in keys,
      // admin included, whenever the environment was "dev", which Terraform's
      // dev environment is. The warning meant to announce it sat inside a
      // `.map`, so it was built and never run. Both cases now fail instead.
      override def getApiKeys: F[Map[String, AuthenticatedClient]] =
        val fullName = config.fullSecretName(config.apiKeysSecretName)
        getSecret(config.apiKeysSecretName).flatMap {
          case None => Async[F].raiseError(new IllegalStateException(
              s"API keys secret $fullName not found. Write the keys into it before starting the service.",
            ))
          case Some(json) => decode[List[ApiKeyConfig]](json) match
              case Left(error) => Async[F].raiseError(new IllegalStateException(
                  s"API keys secret $fullName is not a JSON list of keys: ${error
                      .getMessage}",
                ))
              case Right(configs) => Async[F].pure(toClients(configs))
        }

      private def toClients(
          configs: List[ApiKeyConfig],
      ): Map[String, AuthenticatedClient] = configs.filter(_.active).flatMap(
        cfg =>
          ClientTier.fromString(cfg.tier).map(tier =>
            cfg.apiKey -> AuthenticatedClient(
              apiKeyId = cfg.apiKeyId,
              clientId = cfg.apiKeyId,
              clientName = cfg.clientName,
              tier = tier,
              permissions = cfg.permissions.flatMap(parsePermission).toSet,
            ),
          ),
      ).toMap

      private def fetchAndCache(fullName: String): F[Option[String]] =
        val request = GetSecretValueRequest.builder().secretId(fullName).build()

        Async[F]
          .fromCompletableFuture(Async[F].delay(client.getSecretValue(request)))
          .flatMap { response =>
            val value = response.secretString()
            val versionId = Option(response.versionId())

            Clock[F].realTime.map(_.toMillis).flatMap(now =>
              cacheRef.update(
                _ + (fullName -> CachedSecret(value, now, versionId)),
              ) *> logger.debug(s"Cached secret: $fullName (version: ${versionId
                  .getOrElse("unknown")})") *> Async[F].pure(Some(value)),
            )
          }.handleErrorWith(error =>
            error match
              case _: ResourceNotFoundException => logger
                  .warn(s"Secret not found: $fullName") *> Async[F].pure(None)
              case e => logger.error(e)(s"Failed to fetch secret: $fullName") *>
                  Async[F].raiseError(e),
          )

      private def parsePermission(s: String): Option[Permission] =
        s.toLowerCase match
          case "ratelimit_check" | "ratelimitcheck" =>
            Some(Permission.RateLimitCheck)
          case "ratelimit_status" | "ratelimitstatus" =>
            Some(Permission.RateLimitStatus)
          case "idempotency_check" | "idempotencycheck" =>
            Some(Permission.IdempotencyCheck)
          case "idempotency_complete" | "idempotencycomplete" =>
            Some(Permission.IdempotencyComplete)
          case "quota_check" | "quotacheck" => Some(Permission.QuotaCheck)
          case "quota_reconcile" | "quotareconcile" =>
            Some(Permission.QuotaReconcile)
          case "admin_metrics" | "adminmetrics" => Some(Permission.AdminMetrics)
          case _ => None

/** API key store backed by Secrets Manager.
  */
object SecretsManagerApiKeyStore:

  /** Create an API key store that loads keys from Secrets Manager.
    *
    * The first load happens here and must yield at least one active key, so a
    * missing, malformed, or still-placeholder secret stops startup. It used to
    * load lazily on the first request, so a deploy with no usable keys passed
    * every health check and then answered 401 to everyone. Later refreshes keep
    * the current keys when the secret cannot be read, but a readable secret
    * with every key inactive does take effect, so revoking all keys works.
    */
  def apply[F[_]: Async: Logger](
      secretStore: SecretStore[F],
      refreshInterval: FiniteDuration = 5.minutes,
  ): F[ApiKeyStore[F]] =
    for
      initial <- secretStore.getApiKeys
      _ <- Async[F].raiseWhen(initial.isEmpty)(new IllegalStateException(
        "The API keys secret has no active keys. Write at least one key with \"active\": true before starting the service.",
      ))
      _ <- Logger[F]
        .info(s"Loaded ${initial.size} API keys from Secrets Manager")
      now <- Clock[F].realTime.map(_.toMillis)
      keysRef <- Ref.of[F, Map[String, AuthenticatedClient]](initial)
      lastRefreshRef <- Ref.of[F, Long](now)
    yield new ApiKeyStore[F]:
      private val logger = Logger[F]

      override def findByKey(apiKey: String): F[Option[AuthenticatedClient]] =
        maybeRefresh *> keysRef.get.map(_.get(apiKey))

      override def isKeyValid(apiKey: String): F[Boolean] = maybeRefresh *>
        keysRef.get.map(_.contains(apiKey))

      private def maybeRefresh: F[Unit] =
        for
          now <- Clock[F].realTime.map(_.toMillis)
          lastRefresh <- lastRefreshRef.get
          _ <-
            if now - lastRefresh > refreshInterval.toMillis then
              refresh *> lastRefreshRef.set(now)
            else Async[F].unit
        yield ()

      private def refresh: F[Unit] = secretStore.getApiKeys.flatMap(keys =>
        keysRef.set(keys) *>
          (if keys.isEmpty then
             logger.warn("The API keys secret has no active keys; every request will answer 401")
           else
             logger.info(s"Refreshed ${keys.size} API keys from Secrets Manager")),
      ).handleErrorWith(error =>
        logger.error(error)("Failed to refresh API keys, keeping existing") *>
          Async[F].unit,
      )
