package security

import scala.concurrent.duration.*

import org.http4s.*
import org.http4s.circe.*
import org.http4s.dsl.io.*
import org.http4s.headers.Authorization
import org.http4s.server.AuthMiddleware
import org.typelevel.ci.*
import org.typelevel.log4cats.Logger

import cats.Applicative
import cats.data.{Kleisli, OptionT}
import cats.effect.*
import cats.syntax.all.*
import io.circe.Json
import io.circe.syntax.*

/** API key authentication for securing rate limiter endpoints.
  *
  * Provides:
  *   - API key validation from Authorization header
  *   - Rate limiting on the rate limiter itself (meta!)
  *   - Client identification for per-client rate limits
  */

/** Authenticated client information extracted from API key.
  */
case class AuthenticatedClient(
    apiKeyId: String,
    clientId: String,
    clientName: String,
    tier: ClientTier,
    permissions: Set[Permission],
)

/** Client tier determines rate limit profile.
  */
sealed trait ClientTier:
  def maxRequestsPerSecond: Int
  def maxBurstSize: Int

object ClientTier:
  case object Free extends ClientTier:
    val maxRequestsPerSecond = 10
    val maxBurstSize = 20

  case object Basic extends ClientTier:
    val maxRequestsPerSecond = 100
    val maxBurstSize = 200

  case object Premium extends ClientTier:
    val maxRequestsPerSecond = 1000
    val maxBurstSize = 2000

  case object Enterprise extends ClientTier:
    val maxRequestsPerSecond = 10000
    val maxBurstSize = 20000

  def fromString(s: String): Option[ClientTier] = s.toLowerCase match
    case "free" => Some(Free)
    case "basic" => Some(Basic)
    case "premium" => Some(Premium)
    case "enterprise" => Some(Enterprise)
    case _ => None

/** Permissions for API key holders.
  */
sealed trait Permission
object Permission:
  case object RateLimitCheck extends Permission
  case object RateLimitStatus extends Permission
  case object IdempotencyCheck extends Permission
  case object AdminMetrics extends Permission
  case object AdminConfig extends Permission

  val standard: Set[Permission] =
    Set(RateLimitCheck, RateLimitStatus, IdempotencyCheck)

  val admin: Set[Permission] = standard ++ Set(AdminMetrics, AdminConfig)

/** API key store trait for retrieving and validating keys.
  */
trait ApiKeyStore[F[_]]:
  /** Look up a client by API key */
  def findByKey(apiKey: String): F[Option[AuthenticatedClient]]

  /** Validate an API key is active */
  def isKeyValid(apiKey: String): F[Boolean]

object ApiKeyStore:
  /** In-memory API key store for development/testing.
    */
  def inMemory[F[_]: Sync](
      keys: Map[String, AuthenticatedClient],
  ): ApiKeyStore[F] = new ApiKeyStore[F]:
    override def findByKey(apiKey: String): F[Option[AuthenticatedClient]] =
      Sync[F].pure(keys.get(apiKey))

    override def isKeyValid(apiKey: String): F[Boolean] = Sync[F]
      .pure(keys.contains(apiKey))

  /** Default test keys for local development.
    */
  val testKeys: Map[String, AuthenticatedClient] = Map(
    "test-api-key" -> AuthenticatedClient(
      apiKeyId = "key_test_001",
      clientId = "client_test_001",
      clientName = "Test Client",
      tier = ClientTier.Premium,
      permissions = Permission.standard,
    ),
    "admin-api-key" -> AuthenticatedClient(
      apiKeyId = "key_admin_001",
      clientId = "client_admin_001",
      clientName = "Admin Client",
      tier = ClientTier.Enterprise,
      permissions = Permission.admin,
    ),
    "free-api-key" -> AuthenticatedClient(
      apiKeyId = "key_free_001",
      clientId = "client_free_001",
      clientName = "Free Tier Client",
      tier = ClientTier.Free,
      permissions = Permission.standard,
    ),
  )

/** Authentication error types.
  */
sealed trait AuthError extends RuntimeException
object AuthError:
  case object MissingApiKey extends AuthError:
    override def getMessage: String = "Missing API key in Authorization header"

  case object InvalidApiKey extends AuthError:
    override def getMessage: String = "Invalid API key"

  case class RateLimited(retryAfter: Int) extends AuthError:
    override def getMessage: String =
      s"Rate limited. Retry after $retryAfter seconds"

  case class InsufficientPermissions(required: Permission) extends AuthError:
    override def getMessage: String =
      s"Insufficient permissions: $required required"

/** API key authentication middleware.
  */
object ApiKeyAuth:

  private type AuthResult = Either[AuthError, AuthenticatedClient]

  /** Create authentication middleware.
    *
    * Every failure used to collapse to `None`, which http4s renders as a bare
    * 401. That put the auth-layer throttle -- a valid key sending too fast --
    * in the same bucket as a missing or invalid key, and the two demand
    * opposite client behaviour: fix your credentials versus back off and retry.
    * A correctness run against AWS reported thousands of `HTTP 401` and was
    * first diagnosed as capacity exhaustion; it was this throttle.
    *
    * The throttle now answers 429 with `Retry-After`. Missing and invalid keys
    * keep the bare 401 they always had.
    */
  def middleware[F[_]: Temporal: Logger](
      apiKeyStore: ApiKeyStore[F],
      authRateLimiter: Option[AuthRateLimiter[F]] = None,
  ): AuthMiddleware[F, AuthenticatedClient] =
    val logger = Logger[F]

    def fail(e: AuthError): AuthResult = Left(e)
    def ok(c: AuthenticatedClient): AuthResult = Right(c)

    val authUser: Kleisli[F, Request[F], AuthResult] = Kleisli { request =>
      extractApiKey(request).flatMap {
        case None => logger.debug("Request missing API key")
            .as(fail(AuthError.MissingApiKey))

        case Some(apiKey) => apiKeyStore.findByKey(apiKey).flatMap {
            case None => logger
                .warn(s"Invalid API key attempted: ${maskKey(apiKey)}")
                .as(fail(AuthError.InvalidApiKey))

            case Some(client) => authRateLimiter match
                case None => logger.debug(s"Authenticated client: ${client
                      .clientName}").as(ok(client))

                case Some(limiter) => limiter.checkLimit(client.apiKeyId)
                    .flatMap {
                      case AuthLimitDecision.Allowed => logger
                          .debug(s"Authenticated client: ${client.clientName}")
                          .as(ok(client))
                      case AuthLimitDecision.Throttled(retryAfter) =>
                        logger.warn(s"Client ${client.clientName} hit auth rate limit; retry after ${retryAfter}s")
                          .as(fail(AuthError.RateLimited(retryAfter)))
                    }
          }
      }
    }

    val onFailure: AuthedRoutes[AuthError, F] = Kleisli(authed =>
      OptionT.liftF(Temporal[F].pure(
        authed.context match
          case AuthError.RateLimited(retryAfter) =>
            Response[F](Status.TooManyRequests).withEntity(
              Json.obj("error" := "Rate limited", "retryAfter" := retryAfter),
            ).putHeaders(Header.Raw(ci"Retry-After", retryAfter.toString))
          case _ => Response[F](Status.Unauthorized),
      )),
    )

    AuthMiddleware(authUser, onFailure)

  /** Extract API key from Authorization header. Supports "Bearer <key>" and
    * "ApiKey <key>" formats.
    */
  private def extractApiKey[F[_]: Temporal](
      request: Request[F],
  ): F[Option[String]] = Temporal[F].pure(
    request.headers.get[Authorization].flatMap(auth =>
      auth.credentials match
        case Credentials.Token(scheme, token)
            if scheme.toString.equalsIgnoreCase("Bearer") ||
              scheme.toString.equalsIgnoreCase("ApiKey") => Some(token)
        case _ => None,
    ).orElse(
      // Also check X-Api-Key header
      request.headers.get(ci"X-Api-Key").map(_.head.value),
    ),
  )

  /** Mask API key for logging (show first/last 4 chars) */
  private def maskKey(key: String): String =
    if key.length > 8 then s"${key.take(4)}...${key.takeRight(4)}" else "****"

  /** Run `route` only when the client holds `permission`; answer 403 otherwise.
    *
    * The Kleisli this replaces was never wired into any route, so every
    * permission was decorative: `/metrics` sat public next to an unused
    * `AdminMetrics`. A 403, not a 404, so a key missing a grant is told why.
    */
  def requirePermission[F[_]: Applicative](
      client: AuthenticatedClient,
      permission: Permission,
  )(route: => F[Response[F]]): F[Response[F]] =
    if client.permissions.contains(permission) then route
    else
      Response[F](Status.Forbidden).withEntity(Json.obj(
        "error" := "forbidden",
        "message" := AuthError.InsufficientPermissions(permission).getMessage,
      )).pure[F]

/** Outcome of the auth-layer throttle. `Throttled` carries the seconds until
  * this client's per-minute window resets, so the response can say so.
  */
enum AuthLimitDecision:
  case Allowed
  case Throttled(retryAfterSeconds: Int)

/** Rate limiter for authentication attempts. Protects against brute-force API
  * key guessing.
  */
trait AuthRateLimiter[F[_]]:
  /** Check if a client can make a request */
  def checkLimit(clientId: String): F[AuthLimitDecision]

object AuthRateLimiter:
  import java.util.concurrent.atomic.AtomicInteger

  import com.github.benmanes.caffeine.cache.Caffeine

  private def toOption[A](o: java.util.Optional[A]): Option[A] =
    if o.isPresent then Some(o.get) else None

  /** Simple in-memory rate limiter for auth attempts.
    */
  def inMemory[F[_]: Temporal: Sync](
      maxRequestsPerMinute: Int = 100,
      maxFailedAttemptsPerMinute: Int = 10,
  ): F[AuthRateLimiter[F]] = Sync[F].delay {
    val window = java.time.Duration.ofMinutes(1)
    // Cache for tracking request counts per client. The window is
    // expireAfterWrite from the entry's creation: the counter is mutated in
    // place, never rewritten, so a client's minute starts at its first request.
    val requestCounts = Caffeine.newBuilder().expireAfterWrite(window)
      .build[String, AtomicInteger]()

    new AuthRateLimiter[F]:
      override def checkLimit(clientId: String): F[AuthLimitDecision] = Sync[F]
        .delay {
          val counter = requestCounts.get(clientId, _ => new AtomicInteger(0))
          val count = counter.incrementAndGet()
          if count <= maxRequestsPerMinute then AuthLimitDecision.Allowed
          else AuthLimitDecision.Throttled(secondsUntilReset(clientId))
        }

      // Caffeine reports the entry's age against the fixed expiry, which is
      // exactly the time left in the window. Whole window if it cannot -- the
      // entry expired between the increment and this read.
      private def secondsUntilReset(clientId: String): Int =
        val remaining =
          for
            policy <- toOption(requestCounts.policy().expireAfterWrite())
            age <- toOption(policy.ageOf(clientId))
          yield window.minus(age)
        remaining.map(d => math.max(1, math.ceil(d.toMillis / 1000.0).toInt))
          .getOrElse(window.toSeconds.toInt)
  }

/** Request context enriched with authentication info.
  */
case class AuthenticatedRequest[F[_]](
    client: AuthenticatedClient,
    request: Request[F],
    receivedAt: Long,
)
