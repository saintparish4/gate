package security

import org.http4s.*
import org.http4s.dsl.io.*
import org.http4s.implicits.*
import org.http4s.server.AuthMiddleware
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.typelevel.ci.*
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.noop.NoOpLogger

import cats.data.{Kleisli, OptionT}
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all.*

/** Unit tests for ApiKeyAuth middleware.
  *
  * Exercises the key → client resolution and per-client metadata that drive
  * tier-based rate limiting. HTTP-level assertions use Http4s ContextRoutes so
  * we can observe the full request → response pipeline without a running
  * server.
  */
class AuthMiddlewareSpec extends AsyncFreeSpec with AsyncIOSpec with Matchers:

  given Logger[IO] = NoOpLogger[IO]

  private val keyStore: ApiKeyStore[IO] = ApiKeyStore
    .inMemory[IO](ApiKeyStore.testKeys)

  private val middleware: AuthMiddleware[IO, AuthenticatedClient] = ApiKeyAuth
    .middleware[IO](keyStore, authRateLimiter = None)

  // A trivial authenticated route that echoes the client name so we can verify
  // which client was resolved.
  private val protectedRoutes: AuthedRoutes[AuthenticatedClient, IO] =
    AuthedRoutes.of[AuthenticatedClient, IO] { case ContextRequest(client, _) =>
      Ok(client.clientName)
    }

  private val routes: HttpRoutes[IO] = middleware(protectedRoutes)

  private def requestWithBearer(key: String): Request[IO] =
    Request[IO](Method.GET, uri"/check")
      .putHeaders(headers.Authorization(Credentials.Token(ci"Bearer", key)))

  private def requestWithApiKey(key: String): Request[IO] =
    Request[IO](Method.GET, uri"/check")
      .putHeaders(headers.Authorization(Credentials.Token(ci"ApiKey", key)))

  private def requestWithXApiKey(key: String): Request[IO] =
    Request[IO](Method.GET, uri"/check")
      .putHeaders(Header.Raw(ci"X-Api-Key", key))

  private def requestWithNoAuth: Request[IO] =
    Request[IO](Method.GET, uri"/check")

  "ApiKeyAuth middleware — HTTP routing" - {

    "valid Bearer token returns 200" in
      routes.run(requestWithBearer("test-api-key")).value.map(_.map(_.status))
        .asserting(_ shouldBe Some(Status.Ok))

    "invalid Bearer token returns 401" in
      routes.run(requestWithBearer("definitely-not-valid")).value
        .map(r => r.map(_.status).getOrElse(Status.Unauthorized))
        .asserting(_ shouldBe Status.Unauthorized)

    "missing Authorization header returns 401" in routes.run(requestWithNoAuth)
      .value.map(r => r.map(_.status).getOrElse(Status.Unauthorized))
      .asserting(_ shouldBe Status.Unauthorized)

    "ApiKey scheme is accepted as an alternative to Bearer" in
      routes.run(requestWithApiKey("test-api-key")).value.map(_.map(_.status))
        .asserting(_ shouldBe Some(Status.Ok))

    "X-Api-Key header is accepted as a fallback" in
      routes.run(requestWithXApiKey("test-api-key")).value.map(_.map(_.status))
        .asserting(_ shouldBe Some(Status.Ok))

    "empty string key is treated as invalid" in routes.run(requestWithBearer(""))
      .value.map(r => r.map(_.status).getOrElse(Status.Unauthorized))
      .asserting(_ shouldBe Status.Unauthorized)
  }

  "ApiKeyAuth middleware — client metadata" - {

    "test-api-key resolves to Premium tier with standard permissions" in
      keyStore.findByKey("test-api-key").asserting { maybeClient =>
        maybeClient shouldBe defined
        val client = maybeClient.get
        client.tier shouldBe ClientTier.Premium
        client.permissions should contain(Permission.RateLimitCheck)
        client.permissions should contain(Permission.RateLimitStatus)
        client.permissions should contain(Permission.IdempotencyCheck)
        client.permissions shouldNot contain(Permission.AdminMetrics)
        client.permissions shouldNot contain(Permission.AdminConfig)
      }

    "admin-api-key resolves to Enterprise tier with full admin permissions" in
      keyStore.findByKey("admin-api-key").asserting { maybeClient =>
        maybeClient shouldBe defined
        val client = maybeClient.get
        client.tier shouldBe ClientTier.Enterprise
        client.permissions should contain(Permission.AdminMetrics)
        client.permissions should contain(Permission.AdminConfig)
        client.permissions should contain(Permission.RateLimitCheck)
      }

    "free-api-key resolves to Free tier without admin permissions" in
      keyStore.findByKey("free-api-key").asserting { maybeClient =>
        maybeClient shouldBe defined
        val client = maybeClient.get
        client.tier shouldBe ClientTier.Free
        client.tier.maxRequestsPerSecond shouldBe 10
        client.tier.maxBurstSize shouldBe 20
        client.permissions shouldNot contain(Permission.AdminMetrics)
        client.permissions shouldNot contain(Permission.AdminConfig)
        client.permissions should contain(Permission.RateLimitCheck)
        client.permissions should contain(Permission.IdempotencyCheck)
      }

    "unknown key returns None from the store" in
      keyStore.findByKey("unknown-key-xyz").asserting(_ shouldBe None)

    "isKeyValid returns true for registered keys" in
      keyStore.isKeyValid("test-api-key").asserting(_ shouldBe true)

    "isKeyValid returns false for unknown keys" in
      keyStore.isKeyValid("ghost-key").asserting(_ shouldBe false)
  }

  "ClientTier" - {

    "Free tier limits are the lowest of all tiers" in IO.pure {
      ClientTier.Free.maxRequestsPerSecond should
        be < ClientTier.Basic.maxRequestsPerSecond
      ClientTier.Basic.maxRequestsPerSecond should
        be < ClientTier.Premium.maxRequestsPerSecond
      ClientTier.Premium.maxRequestsPerSecond should
        be < ClientTier.Enterprise.maxRequestsPerSecond
    }.asserting(_ => succeed)

    "fromString round-trips all known tier names" in IO.pure {
      ClientTier.fromString("free") shouldBe Some(ClientTier.Free)
      ClientTier.fromString("basic") shouldBe Some(ClientTier.Basic)
      ClientTier.fromString("premium") shouldBe Some(ClientTier.Premium)
      ClientTier.fromString("enterprise") shouldBe Some(ClientTier.Enterprise)
      ClientTier.fromString("unknown") shouldBe None
    }.asserting(_ => succeed)
  }

  "ApiKeyAuth middleware — auth-layer throttle" - {
    // The throttle is the one failure that is not an auth failure: a valid key
    // sending too fast. It must read as 429 with Retry-After, and it must not
    // change what a missing or invalid key gets.

    def throttled(limit: Int): IO[HttpRoutes[IO]] = AuthRateLimiter
      .inMemory[IO](maxRequestsPerMinute = limit)
      .map(l => ApiKeyAuth.middleware[IO](keyStore, Some(l))(protectedRoutes))

    def statusAndBody(
        r: HttpRoutes[IO],
        req: Request[IO],
    ): IO[Option[(Status, String)]] = r.run(req).value
      .flatMap(_.traverse(resp => resp.as[String].map(b => (resp.status, b))))

    "under the limit every request is 200" in throttled(3).flatMap(r =>
      List.fill(3)(requestWithBearer("test-api-key"))
        .traverse(req => r.run(req).value.map(_.map(_.status))),
    ).asserting(_ shouldBe List.fill(3)(Some(Status.Ok)))

    "over the limit is 429 with Retry-After and a body that says so, not 401" in
      throttled(2).flatMap { r =>
        val req = requestWithBearer("test-api-key")
        for
          _ <- r.run(req).value
          _ <- r.run(req).value
          third <- r.run(req).value
          body <- third.traverse(_.as[String])
        yield (
          third.map(_.status),
          third.flatMap(_.headers.get(ci"Retry-After").map(_.head.value)),
          body.getOrElse(""),
        )
      }.asserting { case (status, retryAfter, body) =>
        status shouldBe Some(Status.TooManyRequests)
        retryAfter.flatMap(_.toIntOption)
          .exists(n => n >= 1 && n <= 60) shouldBe true
        body should include("retryAfter")
        body should include("Rate limited")
      }

    "a missing key is still a bare 401" in throttled(2)
      .flatMap(r => statusAndBody(r, requestWithNoAuth))
      .asserting(_ shouldBe Some((Status.Unauthorized, "")))

    "an invalid key is still a bare 401" in throttled(2)
      .flatMap(r => statusAndBody(r, requestWithBearer("not-a-real-key")))
      .asserting(_ shouldBe Some((Status.Unauthorized, "")))

    "the throttle is per client: one key over the limit does not affect another" in
      throttled(1).flatMap(r =>
        for
          _ <- r.run(requestWithBearer("test-api-key")).value
          second <- r.run(requestWithBearer("test-api-key")).value
          other <- r.run(requestWithBearer("free-api-key")).value
        yield (second.map(_.status), other.map(_.status)),
      ).asserting(_ shouldBe (Some(Status.TooManyRequests), Some(Status.Ok)))
  }

  "AuthRateLimiter.inMemory" - {

    "allows up to the limit, then throttles with a retry-after inside the window" in
      AuthRateLimiter.inMemory[IO](maxRequestsPerMinute = 2)
        .flatMap(l => List.fill(3)("client-a").traverse(l.checkLimit)).asserting {
          ds =>
            ds.take(2) shouldBe
              List(AuthLimitDecision.Allowed, AuthLimitDecision.Allowed)
            ds(2) should matchPattern {
              case AuthLimitDecision.Throttled(n) if n >= 1 && n <= 60 =>
            }
        }
  }
