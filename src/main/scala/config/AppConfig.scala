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
    connectionTimeout: FiniteDuration = scala.concurrent.duration
      .Duration(5, "seconds"),
    requestTimeout: FiniteDuration = scala.concurrent.duration
      .Duration(10, "seconds"),
) derives ConfigReader

// Kinesis stream configuration
case class KinesisConfig(
    streamName: String,
    enabled: Boolean,
    queueSize: Int = 10000,
) derives ConfigReader

// Metrics configuration
case class MetricsConfig(
    enabled: Boolean = true,
    namespace: String = "RateLimiter",
    environment: String = "dev",
    flushInterval: FiniteDuration = scala.concurrent.duration
      .Duration(60, "seconds"),
    highResolution: Boolean = false,
    maxBufferSize: Int = 50000,
    flushThreshold: Int = 1000,
) derives ConfigReader

case class PrometheusConfig(enabled: Boolean = true) derives ConfigReader

// Off by default: every dashboard route is unauthenticated, the config POST
// rewrites the live demo profile, and the decision stream carries every
// client's key ID. Compose turns it on; Terraform pins it off.
case class DashboardConfig(enabled: Boolean = false) derives ConfigReader

// The service name and exporter endpoint come from OTEL_* variables, which the
// OpenTelemetry SDK reads itself.
case class TracingConfig(enabled: Boolean = false) derives ConfigReader

// Security configuration. `enabled`, `header-name` and `api-key-prefix` used to
// sit here too: nothing read them, auth was always on, and the headers were
// hard-coded, so each one was a switch that did nothing.
case class AuthenticationConfig(rateLimitPerMinute: Int = 1000)
    derives ConfigReader

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
    allowBuiltInKeys: Boolean = false,
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
    // RateLimitProfile requires it too, but only when a request builds one, so
    // ttl-seconds = 0 used to start fine and answer 500 to every request.
    else if ttlSeconds <= 0 then
      Left(s"profile '$name': ttlSeconds must be > 0, got $ttlSeconds")
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
    reservationTtlSeconds: Long = 3600,
) derives ConfigReader
// Resilience configuration
case class CircuitBreakerConfig(
    maxFailures: Int = 5,
    resetTimeout: FiniteDuration = scala.concurrent.duration
      .Duration(30, "seconds"),
    halfOpenMaxCalls: Int = 3,
) derives ConfigReader

// One breaker, on the rate-limit store. Kinesis settings sat here unread: the
// publisher has no breaker, by design (ADR-003).
case class CircuitBreakerSettings(
    enabled: Boolean = true,
    dynamodb: CircuitBreakerConfig = CircuitBreakerConfig(),
) derives ConfigReader

case class RetryConfig(
    maxRetries: Int = 3,
    baseDelay: FiniteDuration = scala.concurrent.duration.Duration(100, "millis"),
    maxDelay: FiniteDuration = scala.concurrent.duration.Duration(10, "seconds"),
    multiplier: Double = 2.0,
) derives ConfigReader

case class RetrySettings(dynamodb: RetryConfig = RetryConfig())
    derives ConfigReader

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
    // Longer than the others: a quota check runs its own OCC loop (up to 25
    // attempts) inside this bound, and it guards an LLM call that takes
    // seconds anyway.
    quotaCheck: FiniteDuration = scala.concurrent.duration.Duration(5, "seconds"),
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
    case _ => DegradationMode.RejectAll

case class StorageConfig(
    backend: String = "dynamodb", // "in-memory" | "dynamodb"
) derives ConfigReader

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
    dashboard: DashboardConfig = DashboardConfig(),
    tracing: TracingConfig = TracingConfig(),
    security: SecurityConfig = SecurityConfig(
      authentication = AuthenticationConfig(),
      secrets = SecretsConfig(),
    ),
    resilience: ResilienceConfig = ResilienceConfig(),
    storage: StorageConfig = StorageConfig(),
) derives ConfigReader

object AppConfig:

  val validDegradationModes: Set[String] = Set("allow-all", "reject-all")

  /** An unrecognised degradation-mode used to fall through to reject-all in
    * ResilienceConfig.parsedDegradationMode, so a typo in DEGRADATION_MODE
    * silently armed a full outage for the first time the circuit breaker
    * opened. Rejected at startup instead.
    *
    * `use-cached` gets its own message because it used to be accepted. No cache
    * was ever wired in, so it served every degraded decision as allow-all: an
    * option that read as enforcement and failed open.
    *
    * @return
    *   Some(message) when the value is not usable.
    */
  def validateDegradationMode(mode: String): Option[String] =
    if validDegradationModes.contains(mode) then None
    else if mode == "use-cached" then
      Some("resilience.degradation-mode 'use-cached' is no longer accepted: no cache is wired in, so it failed open. Choose allow-all to fail open deliberately, or reject-all")
    else
      Some(
        s"resilience.degradation-mode '$mode' is not one of ${validDegradationModes
            .toList.sorted.mkString(", ")}",
      )

  /** The built-in keys are public and include an admin key. Secrets Manager
    * defaulted off, so every Terraform deploy served them on a public ALB. They
    * now need an explicit flag, which only docker-compose and `make run` set.
    *
    * @return
    *   Some(message) when the service has no key source it may use.
    */
  def validateKeySource(security: SecurityConfig): Option[String] =
    if security.secrets.enabled || security.allowBuiltInKeys then None
    else
      Some("no API key source: set SECRETS_MANAGER_ENABLED=true to load keys from Secrets Manager, or ALLOW_BUILT_IN_KEYS=true for local development only (the built-in keys are public and include an admin key)")

  val validAlgorithms: Set[String] =
    Set("token-bucket", "leaky-bucket", "sliding-window")
  val validStorageBackends: Set[String] = Set("dynamodb", "in-memory")

  /** An unknown RATE_LIMIT_ALGORITHM used to become the token bucket and an
    * unknown STORAGE_BACKEND became DynamoDB, so a typo quietly ran something
    * nobody chose.
    */
  def validateChoices(config: AppConfig): List[String] =
    def one(name: String, value: String, valid: Set[String]) = Option
      .when(!valid.contains(value))(s"$name '$value' is not one of ${valid
          .toList.sorted.mkString(", ")}")
    one("rate-limit.algorithm", config.rateLimit.algorithm, validAlgorithms)
      .toList ++
      one("storage.backend", config.storage.backend, validStorageBackends)

  /** Every rule `load` enforces beyond what the types already do. */
  def validate(config: AppConfig): List[String] =
    val rl = config.rateLimit
    // The defaults are the profile of any tier without a named one.
    val defaultProfile = RateLimitProfileConfig(
      rl.defaultCapacity,
      rl.defaultRefillRatePerSecond,
      rl.defaultTtlSeconds,
    )
    val profileErrors = (("default" -> defaultProfile) :: rl.profiles.toList)
      .flatMap { case (name, p) => p.validate(name).left.toOption }
    val idem = config.idempotency
    val idempotencyErrors = List(
      "default-ttl-seconds" -> idem.defaultTtlSeconds,
      "max-ttl-seconds" -> idem.maxTtlSeconds,
    ).collect {
      case (key, v) if v <= 0 => s"idempotency.$key must be > 0, got $v"
    }
    val agentCap = (config.tokenQuota.userLimit * 0.8).toLong
    val quotaErrors =
      if config.tokenQuota.enabled && config.tokenQuota.agentLimit > agentCap
      then
        List(s"agentLimit (${config.tokenQuota
            .agentLimit}) exceeds 80% of userLimit ($agentCap)")
      else Nil
    val degradationErrors =
      validateDegradationMode(config.resilience.degradationMode).toList
    val keySourceErrors = validateKeySource(config.security).toList
    profileErrors ++ idempotencyErrors ++ quotaErrors ++ degradationErrors ++
      keySourceErrors ++ validateChoices(config)

  /** Load and validate, failing on any error.
    *
    * There used to be a `loadOrDefault` that caught every failure here,
    * validation included, and started on a hard-coded fallback: no tier
    * profiles, token quota off, Kinesis off. So an unknown degradation mode or
    * an invalid profile, both documented as failing startup, instead started a
    * service that enforced defaults nobody configured. A config Gate cannot
    * honour now stops it.
    */
  def load[F[_]: Sync]: F[AppConfig] = loadFrom(ConfigSource.default)

  def loadFrom[F[_]: Sync](source: ConfigSource): F[AppConfig] = Sync[F]
    .delay(source.loadOrThrow[AppConfig]).flatMap { config =>
      val errors = validate(config)
      if errors.nonEmpty then
        Sync[F]
          .raiseError(new IllegalArgumentException(s"Invalid config: ${errors
              .mkString("; ")}"))
      else Sync[F].pure(config)
    }
