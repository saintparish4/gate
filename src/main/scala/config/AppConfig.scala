package config

import scala.concurrent.duration.FiniteDuration

import cats.effect.Sync
import cats.syntax.all.*
import pureconfig.*
import pureconfig.generic.derivation.default.*

// Server configuration
case class ServerConfig(
    host: String,
    port: Int,
    shutdownTimeout: FiniteDuration = scala.concurrent.duration
      .Duration(30, "seconds"),
) derives ConfigReader

// AWS configuration for SDK clients
/** HTTP connection pool settings for each AWS SDK client. These are passed
  * directly to NettyNioAsyncHttpClient.
  */
case class AwsClientConfig(
    maxConnections: Int = 50,
    maxPendingAcquires: Int = 100,
    connectionAcquisitionTimeoutSeconds: Int = 5,
    connectionTimeToLiveSeconds: Int = 60,
    connectionMaxIdleSeconds: Int = 5,
    disableSdkRetries: Boolean = true,
) derives ConfigReader

case class AwsConfig(
    region: String,
    localstack: Boolean,
    endpoint: String = "",
    dynamodbEndpoint: Option[String] = None,
    kinesisEndpoint: Option[String] = None,
    client: AwsClientConfig = AwsClientConfig(),
) derives ConfigReader

// DynamoDB table configuration
case class DynamoDBConfig(
    rateLimitTable: String,
    idempotencyTable: String,
    maxRetries: Int = 3,
    connectionTimeout: FiniteDuration = scala.concurrent.duration
      .Duration(5, "seconds"),
    requestTimeout: FiniteDuration = scala.concurrent.duration
      .Duration(10, "seconds"),
) derives ConfigReader

// Kinesis stream configuration
case class KinesisConfig(
    streamName: String,
    enabled: Boolean,
    batchSize: Int = 100,
    flushInterval: FiniteDuration = scala.concurrent.duration
      .Duration(1, "second"),
    maxRetries: Int = 3,
    queueSize: Int = 10000,
) derives ConfigReader

// Metrics configuration
case class MetricsConfig(
    enabled: Boolean = true,
    namespace: String = "RateLimiter",
    flushInterval: FiniteDuration = scala.concurrent.duration
      .Duration(60, "seconds"),
    highResolution: Boolean = false,
    maxBufferSize: Int = 50000,
    flushThreshold: Int = 1000,
) derives ConfigReader

case class PrometheusConfig(enabled: Boolean = true) derives ConfigReader

case class TracingConfig(
    enabled: Boolean = false,
    serviceName: String = "gate",
    exporterEndpoint: String = "http://localhost:4317",
) derives ConfigReader

// Security configuration
case class AuthenticationConfig(
    enabled: Boolean = true,
    headerName: String = "Authorization",
    apiKeyPrefix: String = "Bearer",
    rateLimitPerMinute: Int = 1000,
    maxFailedAttempts: Int = 10,
) derives ConfigReader

case class SecretsConfig(
    enabled: Boolean = false,
    secretPrefix: String = "rate-limiter",
    environment: String = "dev",
    apiKeysSecretName: String = "api-keys",
    cacheTtl: FiniteDuration = scala.concurrent.duration.Duration(5, "minutes"),
) derives ConfigReader

case class SecurityConfig(
    authentication: AuthenticationConfig,
    secrets: SecretsConfig,
) derives ConfigReader

// Rate limiting profile
case class RateLimitProfileConfig(
    capacity: Int,
    refillRatePerSecond: Double,
    ttlSeconds: Long,
) derives ConfigReader:
  /** Validate profile fields; called at config load time so bad config fails
    * startup rather than silently producing wrong runtime behaviour.
    */
  def validate(name: String): Either[String, Unit] =
    if name.isEmpty then Left("profile name cannot be empty")
    else if capacity < 1 then
      Left(s"profile '$name': capacity must be >= 1, got $capacity")
    else if refillRatePerSecond <= 0 then
      Left(s"profile '$name': refillRatePerSecond must be > 0, got $refillRatePerSecond")
    else Right(())

// Rate limiting defaults
case class RateLimitConfig(
    defaultCapacity: Int,
    defaultRefillRatePerSecond: Double,
    defaultTtlSeconds: Long,
    algorithm: String = "token-bucket",
    profiles: Map[String, RateLimitProfileConfig] = Map.empty,
) derives ConfigReader

// Idempotency TTL: default and max cap for client-supplied TTL
case class IdempotencyConfig(
    defaultTtlSeconds: Long = 86400,
    maxTtlSeconds: Long = 86400,
) derives ConfigReader

// Token quota limits for AI workloads (per-user, per-agent, per-org)
case class TokenQuotaConfig(
    enabled: Boolean = false,
    tableName: String = "gate-token-quotas",
    userLimit: Long = 1_000_000,
    userWindowSeconds: Long = 3600,
    agentLimit: Long = 500_000,
    agentWindowSeconds: Long = 3600,
    orgLimit: Long = 10_000_000,
    orgWindowSeconds: Long = 86400,
) derives ConfigReader
// Resilience configuration
case class CircuitBreakerConfig(
    maxFailures: Int = 5,
    resetTimeout: FiniteDuration = scala.concurrent.duration
      .Duration(30, "seconds"),
    halfOpenMaxCalls: Int = 3,
) derives ConfigReader

case class CircuitBreakerSettings(
    enabled: Boolean = true,
    dynamodb: CircuitBreakerConfig = CircuitBreakerConfig(),
    kinesis: CircuitBreakerConfig = CircuitBreakerConfig(),
) derives ConfigReader

case class RetryConfig(
    maxRetries: Int = 3,
    baseDelay: FiniteDuration = scala.concurrent.duration.Duration(100, "millis"),
    maxDelay: FiniteDuration = scala.concurrent.duration.Duration(10, "seconds"),
    multiplier: Double = 2.0,
) derives ConfigReader

case class RetrySettings(
    dynamodb: RetryConfig = RetryConfig(),
    kinesis: RetryConfig = RetryConfig(),
) derives ConfigReader

case class BulkheadSettings(
    enabled: Boolean = true,
    maxConcurrent: Int = 25,
    maxWait: FiniteDuration = scala.concurrent.duration.Duration(100, "millis"),
) derives ConfigReader

case class TimeoutSettings(
    rateLimitCheck: FiniteDuration = scala.concurrent.duration
      .Duration(500, "millis"),
    idempotencyCheck: FiniteDuration = scala.concurrent.duration
      .Duration(2, "seconds"),
    healthCheck: FiniteDuration = scala.concurrent.duration
      .Duration(5, "seconds"),
) derives ConfigReader

case class ResilienceConfig(
    circuitBreaker: CircuitBreakerSettings = CircuitBreakerSettings(),
    retry: RetrySettings = RetrySettings(),
    bulkhead: BulkheadSettings = BulkheadSettings(),
    timeout: TimeoutSettings = TimeoutSettings(),
    degradationMode: String = "reject-all",
) derives ConfigReader:
  import resilience.GracefulDegradation.DegradationMode
  def parsedDegradationMode: DegradationMode = degradationMode match
    case "allow-all" => DegradationMode.AllowAll
    case "reject-all" => DegradationMode.RejectAll
    case "use-cached" => DegradationMode.UseCached
    case _ => DegradationMode.RejectAll

case class StorageConfig(
    backend: String = "dynamodb", // "in-memory" | "dynamodb"
) derives ConfigReader

case class CacheConfig(
    enabled: Boolean = true,
    maxSize: Int = 10000,
    ttl: FiniteDuration = scala.concurrent.duration.Duration(1, "second"),
    recordStats: Boolean = true,
) derives ConfigReader

/** Audit trail configuration for PCI DSS 4.0.1 compliance.
  *
  * NOTE: PureConfig's Scala 3 derivation uses a fixed camelCase→kebab-case
  * field mapping and converts `s3Prefix` into `s-3-prefix` (digits start a new
  * "word"), which doesn't match the `s3-prefix` key in application.conf.
  * Without the explicit reader below, `ConfigSource.default.loadOrThrow` fails
  * and the service silently falls back to `loadOrDefault`, discarding every
  * `${?ENV}` override in the HOCON file — a very expensive silent failure (it
  * made `AUTH_RATE_LIMIT_PER_MINUTE` look broken during load tests).
  */
case class AuditConfig(
    enabled: Boolean = true,
    retentionYears: Int = 7,
    s3Prefix: String = "audit/",
)

object AuditConfig:
  private def readOpt[A: ConfigReader](
      obj: ConfigObjectCursor,
      key: String,
      default: A,
  ): ConfigReader.Result[A] =
    val c = obj.atKeyOrUndefined(key)
    if c.isUndefined then Right(default) else ConfigReader[A].from(c)

  given ConfigReader[AuditConfig] = ConfigReader.fromCursor(cur =>
    cur.asObjectCursor.flatMap(obj =>
      for
        enabled <- readOpt[Boolean](obj, "enabled", true)
        retention <- readOpt[Int](obj, "retention-years", 7)
        prefix <- readOpt[String](obj, "s3-prefix", "audit/")
      yield AuditConfig(enabled, retention, prefix),
    ),
  )

// Root application configuration
case class AppConfig(
    server: ServerConfig,
    aws: AwsConfig,
    tokenQuota: TokenQuotaConfig = TokenQuotaConfig(),
    dynamodb: DynamoDBConfig,
    kinesis: KinesisConfig,
    rateLimit: RateLimitConfig,
    idempotency: IdempotencyConfig = IdempotencyConfig(),
    metrics: MetricsConfig = MetricsConfig(),
    prometheus: PrometheusConfig = PrometheusConfig(),
    tracing: TracingConfig = TracingConfig(),
    security: SecurityConfig = SecurityConfig(
      authentication = AuthenticationConfig(),
      secrets = SecretsConfig(),
    ),
    resilience: ResilienceConfig = ResilienceConfig(),
    cache: CacheConfig = CacheConfig(),
    storage: StorageConfig = StorageConfig(),
    audit: AuditConfig = AuditConfig(),
) derives ConfigReader

object AppConfig:
  def load[F[_]: Sync]: F[AppConfig] = Sync[F]
    .delay(ConfigSource.default.loadOrThrow[AppConfig]).flatMap { config =>
      val profileErrors = config.rateLimit.profiles.toList
        .flatMap { case (name, p) => p.validate(name).left.toOption }
      val agentCap = (config.tokenQuota.userLimit * 0.8).toLong
      val quotaErrors =
        if config.tokenQuota.enabled && config.tokenQuota.agentLimit > agentCap
        then
          List(s"agentLimit (${config.tokenQuota
              .agentLimit}) exceeds 80% of userLimit ($agentCap)")
        else Nil
      val allErrors = profileErrors ++ quotaErrors
      if allErrors.nonEmpty then
        Sync[F]
          .raiseError(new IllegalArgumentException(s"Invalid config: ${allErrors
              .mkString("; ")}"))
      else Sync[F].pure(config)
    }

  def loadOrDefault[F[_]: Sync]: F[AppConfig] = load[F].handleErrorWith { err =>
    Sync[F].delay {
      System.err
        .println(s"[WARN] AppConfig.load failed, using env-var fallback: ${err
            .getMessage}")
      val useLocalstack = Option(System.getenv("USE_LOCALSTACK"))
        .exists(v => v.equalsIgnoreCase("true") || v == "1")
      val dynamoEndpoint = Option(System.getenv("DYNAMODB_ENDPOINT"))
      val kinesisEndpoint = Option(System.getenv("KINESIS_ENDPOINT"))
      val awsEndpoint = Option(System.getenv("AWS_ENDPOINT")).getOrElse("")
      val degradationMode = Option(System.getenv("DEGRADATION_MODE"))
        .getOrElse("reject-all")
      val algorithm = Option(System.getenv("RATE_LIMIT_ALGORITHM"))
        .getOrElse("token-bucket")
      val timeout =
        if useLocalstack then
          TimeoutSettings(
            rateLimitCheck = scala.concurrent.duration.Duration(5, "seconds"),
            healthCheck = scala.concurrent.duration.Duration(5, "seconds"),
          )
        else TimeoutSettings()
      AppConfig(
        server = ServerConfig("0.0.0.0", 8080),
        aws = AwsConfig(
          region = Option(System.getenv("AWS_REGION")).getOrElse("us-east-1"),
          localstack = useLocalstack,
          endpoint = awsEndpoint,
          dynamodbEndpoint = dynamoEndpoint,
          kinesisEndpoint = kinesisEndpoint,
        ),
        dynamodb = DynamoDBConfig("rate-limits", "idempotency"),
        kinesis = KinesisConfig("rate-limit-events", false),
        rateLimit = RateLimitConfig(100, 10.0, 3600, algorithm),
        idempotency = IdempotencyConfig(),
        resilience =
          ResilienceConfig(timeout = timeout, degradationMode = degradationMode),
      )
    }
  }
