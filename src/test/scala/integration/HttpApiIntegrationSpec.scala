package integration

import scala.concurrent.duration.*

import org.http4s.*
import org.http4s.circe.*
import org.http4s.headers.`Retry-After`
import org.http4s.implicits.*
import org.scalatest.BeforeAndAfterEach
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.typelevel.ci.*
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.otel4s.trace.Tracer.Implicits.noop

import api.{
  RateLimitCheckResponse, RateLimitStatusResponse, Routes, TokenQuotaApi,
  TokenQuotaCheckRequest, TokenQuotaReconcileRequest,
}
import config.{
  BulkheadSettings, CircuitBreakerSettings, IdempotencyConfig, RateLimitConfig,
  RateLimitProfileConfig, ResilienceConfig, RetryConfig, RetrySettings,
  TimeoutSettings, TokenQuotaConfig,
}
import events.{EventPublisher, RateLimitEvent}
import observability.{MetricsPublisher, PrometheusMetrics}
import resilience.{AggregateHealth, GracefulDegradation, ResilientRateLimitStore}
import security.{
  ApiKeyAuth, ApiKeyStore, AuthRateLimiter, AuthenticatedClient, ClientTier,
  Permission,
}
import storage.{
  DynamoDBIdempotencyStore, DynamoDBRateLimitStore, DynamoDBTokenQuotaStore,
}
import cats.effect.IO
import cats.effect.std.Queue
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import io.circe.generic.auto.*
import io.circe.parser.*
import io.circe.syntax.*
import core.{IdempotencyStore, RateLimitStore, TokenQuotaService}

/** Integration tests for HTTP API endpoints.
  *
  * Tests the full request/response cycle through the HTTP layer. Requires
  * Docker (LocalStack). Without Docker, run unit tests only: sbt unitTest
  */
@Integration
class HttpApiIntegrationSpec
    extends AsyncFreeSpec
    with AsyncIOSpec
    with Matchers
    with LocalStackIntegrationSpec
    with BeforeAndAfterEach {

  implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  // Test configuration. `free` matches the defaults the older tests were
  // written against; `enterprise` is the profile a free key must not reach.
  lazy val testRateLimitConfig: RateLimitConfig = RateLimitConfig(
    defaultCapacity = 10,
    defaultRefillRatePerSecond = 0.1,
    defaultTtlSeconds = 3600,
    profiles = Map(
      "free" -> RateLimitProfileConfig(10, 0.1, 3600),
      "enterprise" -> RateLimitProfileConfig(10000, 1000.0, 3600),
    ),
  )

  // Create stores
  lazy val rateLimitStore: DynamoDBRateLimitStore[IO] =
    new DynamoDBRateLimitStore[IO](
      dynamoDbClient,
      testDynamoDBConfig.rateLimitTable,
      MetricsPublisher.noop[IO],
    )

  lazy val idempotencyStore: DynamoDBIdempotencyStore[IO] =
    new DynamoDBIdempotencyStore[IO](
      dynamoDbClient,
      testDynamoDBConfig.idempotencyTable,
    )

  // No-op event publisher for tests
  lazy val eventPublisher: EventPublisher[IO] = EventPublisher.noop[IO]

  // Mirrored into Prometheus, the way Main wires it, so /metrics shows what
  // the requests in these tests did.
  lazy val metricsPublisher: MetricsPublisher[IO] = PrometheusMetrics
    .dual(MetricsPublisher.noop[IO], prometheusMetrics)

  // Test API key store
  lazy val testApiKeyStore: ApiKeyStore[IO] = ApiKeyStore.inMemory[IO] {
    Map(
      "test-key" -> AuthenticatedClient(
        apiKeyId = "test-client-1",
        clientId = "test-client-1",
        clientName = "Test Client",
        tier = ClientTier.Free, // Use Free tier (capacity 10) to match test expectations
        permissions = Permission.standard,
      ),
      // A second standard tenant, for the isolation tests.
      "other-key" -> AuthenticatedClient(
        apiKeyId = "other-client-1",
        clientId = "other-client-1",
        clientName = "Other Client",
        tier = ClientTier.Free,
        permissions = Permission.standard,
      ),
      "admin-key" -> AuthenticatedClient(
        apiKeyId = "admin-client-1",
        clientId = "admin-client-1",
        clientName = "Admin Client",
        tier = ClientTier.Enterprise,
        permissions = Permission.admin,
      ),
    ) ++ Permission.standard.map(missing =>
      // Every standard permission but one, so a refusal can only come from
      // the route's own check.
      s"lacks-$missing" -> AuthenticatedClient(
        apiKeyId = s"lacks-$missing",
        clientId = s"lacks-$missing",
        clientName = s"Lacks $missing",
        tier = ClientTier.Free,
        permissions = Permission.standard - missing,
      ),
    )
  }

  // Auth middleware
  lazy val authMiddleware = ApiKeyAuth.middleware[IO](testApiKeyStore, None)

  lazy val prometheusMetrics: PrometheusMetrics[IO] = PrometheusMetrics[IO]
    .unsafeRunSync()

  val tokenQuotaTableName = "test-token-quotas"

  override protected def setupResources(): Unit = {
    super.setupResources()
    createDynamoDBTable(tokenQuotaTableName)
  }

  // Small limits and a short window so the HTTP tests hit the edges quickly.
  lazy val testTokenQuotaConfig: TokenQuotaConfig = TokenQuotaConfig(
    enabled = true,
    userLimit = 100,
    userWindowSeconds = 60,
    agentLimit = 80,
    agentWindowSeconds = 60,
    orgLimit = 1000,
    orgWindowSeconds = 60,
  )

  lazy val tokenQuotaApi: TokenQuotaApi[IO] = TokenQuotaApi[IO](
    TokenQuotaService[IO](
      DynamoDBTokenQuotaStore[IO](
        dynamoDbClient,
        tokenQuotaTableName,
        logger,
        metricsPublisher,
      ),
      testTokenQuotaConfig,
      metricsPublisher,
      logger,
    ),
    eventPublisher,
    metricsPublisher,
    logger,
    () => IO.pure("test-request-id"),
  )

  lazy val testIdempotencyConfig: IdempotencyConfig =
    IdempotencyConfig(defaultTtlSeconds = 86400, maxTtlSeconds = 86400)

  // Create routes
  lazy val routes: Routes[IO] = new Routes[IO](
    rateLimitStore,
    idempotencyStore,
    eventPublisher,
    metricsPublisher,
    authMiddleware,
    testRateLimitConfig,
    testIdempotencyConfig,
    logger,
    dashboardApi = None,
    tokenQuotaApi = Some(tokenQuotaApi),
    prometheusMetrics = Some(prometheusMetrics),
    healthCheck = IO.pure(AggregateHealth("ok", Nil)),
    getRequestId = () => IO.pure("test-request-id"),
  )

  lazy val httpApp: HttpApp[IO] = routes.httpApp

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    clearTable(testDynamoDBConfig.rateLimitTable)
    clearTable(testDynamoDBConfig.idempotencyTable)
    clearTable(tokenQuotaTableName)
  }

  "Tenant isolation" - {
    // Every key used to be global, so two clients naming the same key shared
    // one bucket, one idempotency record, and one quota counter (ADR-005). Each
    // route is driven here by two clients using the same visible key.

    def as(key: String)(request: Request[IO]): Request[IO] = request
      .putHeaders(headers.Authorization(Credentials.Token(ci"Bearer", key)))

    def post(path: Uri, body: String): Request[IO] =
      Request[IO](Method.POST, path).withEntity(body)
        .putHeaders(headers.`Content-Type`(MediaType.application.json))

    def json(response: Response[IO]): IO[io.circe.Json] = response.as[String]
      .flatMap(body => IO.fromEither(parse(body)))

    "POST /v1/ratelimit/check: draining a key leaves another client's bucket full" in {
      val check = post(uri"/v1/ratelimit/check", """{"key": "shared-rl"}""")
      for {
        _ <- List.fill(10)(check).traverse(r => httpApp.run(as("test-key")(r)))
        blocked <- httpApp.run(as("test-key")(check))
        other <- httpApp.run(as("other-key")(check))
        body <- json(other)
      } yield {
        blocked.status shouldBe Status.TooManyRequests
        other.status shouldBe Status.Ok
        body.hcursor.get[Int]("tokensRemaining").toOption shouldBe Some(9)
      }
    }

    "GET /v1/ratelimit/status/:key: a client sees only its own bucket" in {
      val status =
        Request[IO](Method.GET, uri"/v1/ratelimit/status/shared-status")
      for {
        _ <- httpApp.run(as("test-key")(post(
          uri"/v1/ratelimit/check",
          """{"key": "shared-status", "cost": 3}""",
        )))
        mine <- httpApp.run(as("test-key")(status)).flatMap(json)
        theirs <- httpApp.run(as("other-key")(status)).flatMap(json)
      } yield {
        mine.hcursor.get[Int]("tokensRemaining").toOption shouldBe Some(7)
        theirs.hcursor.get[Int]("tokensRemaining").toOption shouldBe Some(10)
      }
    }

    "POST /v1/idempotency/check: the same key is new once per client" in {
      val check =
        post(uri"/v1/idempotency/check", """{"idempotencyKey": "shared-idem"}""")
      for {
        mine <- httpApp.run(as("test-key")(check))
        mineBody <- json(mine)
        theirs <- httpApp.run(as("other-key")(check))
        theirsBody <- json(theirs)
      } yield {
        mineBody.hcursor.get[String]("status").toOption shouldBe Some("new")
        theirs.status shouldBe Status.Ok
        theirsBody.hcursor.get[String]("status").toOption shouldBe Some("new")
        // The caller's key comes back, never the scoped storage key.
        theirsBody.hcursor.get[String]("idempotencyKey").toOption shouldBe
          Some("shared-idem")
      }
    }

    "POST /v1/idempotency/:key/complete: a client cannot complete another's pending key" in {
      val check = post(
        uri"/v1/idempotency/check",
        """{"idempotencyKey": "shared-complete"}""",
      )
      val complete = post(
        uri"/v1/idempotency/shared-complete/complete",
        """{"statusCode": 200, "body": "forged"}""",
      )
      for {
        _ <- httpApp.run(as("test-key")(check))
        forged <- httpApp.run(as("other-key")(complete))
        after <- httpApp.run(as("test-key")(check))
        afterBody <- json(after)
      } yield {
        forged.status shouldBe Status.Conflict
        after.status shouldBe Status.Accepted
        afterBody.hcursor.get[String]("status").toOption shouldBe
          Some("in_progress")
      }
    }

    "POST /v1/idempotency/:key/fail: a client cannot release another's pending key" in {
      val check =
        post(uri"/v1/idempotency/check", """{"idempotencyKey": "shared-fail"}""")
      val fail = Request[IO](Method.POST, uri"/v1/idempotency/shared-fail/fail")
      for {
        _ <- httpApp.run(as("test-key")(check))
        forged <- httpApp.run(as("other-key")(fail))
        stillMine <- httpApp.run(as("test-key")(check)).flatMap(json)
        released <- httpApp.run(as("test-key")(fail))
        again <- httpApp.run(as("test-key")(check)).flatMap(json)
      } yield {
        forged.status shouldBe Status.Conflict
        stillMine.hcursor.get[String]("status").toOption shouldBe
          Some("in_progress")
        released.status shouldBe Status.Ok
        again.hcursor.get[String]("status").toOption shouldBe Some("new")
      }
    }

    "a completed response is replayed to its own client only" in {
      val check = post(
        uri"/v1/idempotency/check",
        """{"idempotencyKey": "shared-replay"}""",
      )
      val complete = post(
        uri"/v1/idempotency/shared-replay/complete",
        """{"statusCode": 201, "body": "secret"}""",
      )
      for {
        _ <- httpApp.run(as("test-key")(check))
        _ <- httpApp.run(as("test-key")(complete))
        theirs <- httpApp.run(as("other-key")(check)).flatMap(json)
        mine <- httpApp.run(as("test-key")(check)).flatMap(json)
      } yield {
        theirs.hcursor.get[String]("status").toOption shouldBe Some("new")
        (theirs.noSpaces should not).include("secret")
        mine.hcursor.get[String]("status").toOption shouldBe Some("duplicate")
        mine.hcursor.downField("originalResponse").get[String]("body")
          .toOption shouldBe Some("secret")
      }
    }

    "POST /v1/quota/check: two clients naming the same user meter separate quotas" in {
      def check(tokens: Int) = post(
        uri"/v1/quota/check",
        s"""{"userId": "shared-user", "estimatedInputTokens": $tokens}""",
      )
      for {
        filled <- httpApp.run(as("test-key")(check(100)))
        refused <- httpApp.run(as("test-key")(check(1)))
        theirs <- httpApp.run(as("other-key")(check(100)))
      } yield {
        filled.status shouldBe Status.Ok
        refused.status shouldBe Status.TooManyRequests
        theirs.status shouldBe Status.Ok
      }
    }

    "POST /v1/quota/reconcile: another client cannot reconcile a reservation, even knowing its ID" in {
      val fill = post(
        uri"/v1/quota/check",
        """{"userId": "shared-reconcile", "estimatedInputTokens": 100}""",
      )
      def erase(id: String) =
        post(uri"/v1/quota/reconcile", s"""{"reservationId": "$id", "actualInputTokens": 0, "actualOutputTokens": 0}""")
      val next = post(
        uri"/v1/quota/check",
        """{"userId": "shared-reconcile", "estimatedInputTokens": 1}""",
      )
      for {
        filled <- httpApp.run(as("test-key")(fill)).flatMap(json)
        id <- IO.fromEither(filled.hcursor.get[String]("reservationId"))
        stolen <- httpApp.run(as("other-key")(erase(id)))
        after <- httpApp.run(as("test-key")(next))
      } yield {
        stolen.status shouldBe Status.NotFound
        after.status shouldBe Status.TooManyRequests
      }
    }
  }

  "Token quota endpoints" - {

    def quotaCheck(userId: String, tokens: Long): Request[IO] = Request[IO](
      Method.POST,
      uri"/v1/quota/check",
    ).putHeaders(headers.Authorization(Credentials.Token(ci"Bearer", "test-key")))
      .withEntity(
        TokenQuotaCheckRequest(userId = userId, estimatedInputTokens = tokens)
          .asJson,
      )

    def quotaReconcile(reservationId: String, actual: Long): Request[IO] =
      Request[IO](Method.POST, uri"/v1/quota/reconcile").putHeaders(
        headers.Authorization(Credentials.Token(ci"Bearer", "test-key")),
      ).withEntity(
        TokenQuotaReconcileRequest(
          reservationId = reservationId,
          actualInputTokens = actual,
          actualOutputTokens = 0,
        ).asJson,
      )

    def reservationId(response: Response[IO]): IO[String] = response.as[String]
      .flatMap(body =>
        IO.fromEither(parse(body).flatMap(_.hcursor.get[String]("reservationId"))),
      )

    "POST /v1/quota/check admits under the limit, then rejects with Retry-After inside the window" in {
      for {
        first <- httpApp.run(quotaCheck("quota-user-1", 90))
        second <- httpApp.run(quotaCheck("quota-user-1", 50))
        body <- second.as[String]
      } yield {
        first.status shouldBe Status.Ok
        second.status shouldBe Status.TooManyRequests
        val retryAfter = second.headers.get(ci"Retry-After")
          .map(_.head.value.toInt)
        retryAfter.exists(r => r >= 1 && r <= 60) shouldBe true
        body should include("\"exceededLevel\":\"user\"")
      }
    }

    "POST /v1/quota/reconcile replaces the estimate so freed tokens can be reused" in {
      for {
        first <- httpApp.run(quotaCheck("quota-user-2", 60))
        id <- reservationId(first)
        reconciled <- httpApp.run(quotaReconcile(id, 30))
        second <- httpApp.run(quotaCheck("quota-user-2", 60))
      } yield {
        first.status shouldBe Status.Ok
        reconciled.status shouldBe Status.Ok
        // 30 actually used + 60 requested = 90, inside the 100-token limit.
        second.status shouldBe Status.Ok
      }
    }

    "POST /v1/quota/reconcile applies once: a second reconcile cannot free tokens again" in {
      for {
        first <- httpApp.run(quotaCheck("quota-user-3", 90))
        id <- reservationId(first)
        _ <- httpApp.run(quotaReconcile(id, 90))
        again <- httpApp.run(quotaReconcile(id, 0))
        next <- httpApp.run(quotaCheck("quota-user-3", 20))
      } yield {
        again.status shouldBe Status.Conflict
        // Still 90 used, so 20 more would pass the 100-token limit.
        next.status shouldBe Status.TooManyRequests
      }
    }

    "POST /v1/quota/reconcile ignores an estimate in the request" in {
      // The old contract took the estimate from the caller, which let
      // actual = 0 with a huge estimate zero the counter (finding B).
      val forged = Request[IO](Method.POST, uri"/v1/quota/reconcile")
        .putHeaders(headers.Authorization(
          Credentials.Token(ci"Bearer", "test-key"),
        ))
      for {
        first <- httpApp.run(quotaCheck("quota-user-4", 90))
        id <- reservationId(first)
        _ <- httpApp.run(
          forged.withEntity(s"""{"reservationId": "$id", "actualInputTokens": 90, "actualOutputTokens": 0, "estimatedInputTokens": 1000000}"""),
        )
        next <- httpApp.run(quotaCheck("quota-user-4", 20))
      } yield next.status shouldBe Status.TooManyRequests
    }
  }

  "Health endpoints" - {

    "GET /health should return 200" in {
      val request = Request[IO](Method.GET, uri"/health")

      httpApp.run(request)
        .asserting(response => response.status shouldBe Status.Ok)
    }

    "GET /health should return healthy status" in {
      val request = Request[IO](Method.GET, uri"/health")

      for {
        response <- httpApp.run(request)
        body <- response.as[String]
      } yield {
        body should include("healthy")
        // Asserted against the build so the literal cannot drift again.
        body should include(buildinfo.BuildInfo.version)
      }
    }

    "GET /ready should return 200 when dependencies are healthy" in {
      val request = Request[IO](Method.GET, uri"/ready")

      httpApp.run(request)
        .asserting(response => response.status shouldBe Status.Ok)
    }
  }

  "Rate limit endpoints" - {

    "POST /v1/ratelimit/check should allow requests within capacity" in {
      val body = """{"key": "user:123", "cost": 1}"""
      val request = Request[IO](Method.POST, uri"/v1/ratelimit/check")
        .withEntity(body).putHeaders(
          headers.`Content-Type`(MediaType.application.json),
          headers.Authorization(Credentials.Token(ci"Bearer", "test-key")),
        )

      for {
        response <- httpApp.run(request)
        body <- response.as[String]
        json <- IO.fromEither(parse(body))
      } yield {
        response.status shouldBe Status.Ok
        json.hcursor.get[Boolean]("allowed").toOption shouldBe Some(true)
        json.hcursor.get[Int]("tokensRemaining").toOption shouldBe Some(9)
      }
    }

    "POST /v1/ratelimit/check should return 429 when over capacity" in {
      // Exhaust all capacity in a single request to avoid token refill between requests
      val exhaustBody = """{"key": "exhaust-key", "cost": 10}"""
      val exhaustRequest = Request[IO](Method.POST, uri"/v1/ratelimit/check")
        .withEntity(exhaustBody).putHeaders(
          headers.`Content-Type`(MediaType.application.json),
          headers.Authorization(Credentials.Token(ci"Bearer", "test-key")),
        )

      val nextBody = """{"key": "exhaust-key", "cost": 1}"""
      val nextRequest = Request[IO](Method.POST, uri"/v1/ratelimit/check")
        .withEntity(nextBody).putHeaders(
          headers.`Content-Type`(MediaType.application.json),
          headers.Authorization(Credentials.Token(ci"Bearer", "test-key")),
        )

      val test = for {
        // Exhaust all capacity in one request
        _ <- httpApp.run(exhaustRequest).flatMap(_.body.compile.drain)

        // This should be rejected
        response <- httpApp.run(nextRequest)
      } yield response

      test.asserting { response =>
        response.status shouldBe Status.TooManyRequests
        response.headers.get[`Retry-After`] shouldBe defined
      }
    }

    "POST /v1/ratelimit/check should include Retry-After header on rejection" in {
      val body = """{"key": "retry-test-key", "cost": 10}"""

      val request1 = Request[IO](Method.POST, uri"/v1/ratelimit/check")
        .withEntity(body).putHeaders(
          headers.`Content-Type`(MediaType.application.json),
          headers.Authorization(Credentials.Token(ci"Bearer", "test-key")),
        )
      val request2 = Request[IO](Method.POST, uri"/v1/ratelimit/check")
        .withEntity("""{"key": "retry-test-key", "cost": 1}""").putHeaders(
          headers.`Content-Type`(MediaType.application.json),
          headers.Authorization(Credentials.Token(ci"Bearer", "test-key")),
        )

      val test = for {
        // Exhaust capacity - consume response body to ensure request completes
        _ <- httpApp.run(request1).flatMap(_.body.compile.drain)

        // Get rejection with Retry-After
        response <- httpApp.run(request2)
        bodyText <- response.as[String]
      } yield (response, bodyText)

      test.asserting { case (response, body) =>
        response.status shouldBe Status.TooManyRequests
        body should include("retryAfter")
        body should include("Rate limit exceeded")
      }
    }

    "GET /v1/ratelimit/status/:key should return current status" in {
      // First consume some tokens
      val checkBody = """{"key": "status-key", "cost": 3}"""
      val checkRequest = Request[IO](Method.POST, uri"/v1/ratelimit/check")
        .withEntity(checkBody).putHeaders(
          headers.`Content-Type`(MediaType.application.json),
          headers.Authorization(Credentials.Token(ci"Bearer", "test-key")),
        )

      val test = for {
        _ <- httpApp.run(checkRequest)

        statusRequest =
          Request[IO](Method.GET, uri"/v1/ratelimit/status/status-key")
            .putHeaders(headers.Authorization(
              Credentials.Token(ci"Bearer", "test-key"),
            ))
        response <- httpApp.run(statusRequest)
        body <- response.as[String]
        json <- IO.fromEither(parse(body))
      } yield (response, json)

      test.asserting { case (response, json) =>
        response.status shouldBe Status.Ok
        json.hcursor.get[String]("key").toOption shouldBe Some("status-key")
        json.hcursor.get[Int]("tokensRemaining").toOption shouldBe Some(7)
        json.hcursor.get[Int]("limit").toOption shouldBe Some(10)
      }
    }

    "GET /v1/ratelimit/status/:key should return full capacity for unknown key" in {
      val request =
        Request[IO](Method.GET, uri"/v1/ratelimit/status/unknown-key")
          .putHeaders(headers.Authorization(
            Credentials.Token(ci"Bearer", "test-key"),
          ))

      for {
        response <- httpApp.run(request)
        body <- response.as[String]
        json <- IO.fromEither(parse(body))
      } yield {
        response.status shouldBe Status.Ok
        json.hcursor.get[Int]("tokensRemaining").toOption shouldBe Some(10)
      }
    }
  }

  "Idempotency endpoints" - {

    "POST /v1/idempotency/check should return 'new' for first request" in {
      val body = """{"idempotencyKey": "payment:abc-123"}"""
      val request = Request[IO](Method.POST, uri"/v1/idempotency/check")
        .withEntity(body).putHeaders(
          headers.`Content-Type`(MediaType.application.json),
          headers.Authorization(Credentials.Token(ci"Bearer", "test-key")),
        )

      for {
        response <- httpApp.run(request)
        bodyText <- response.as[String]
        json <- IO.fromEither(parse(bodyText))
      } yield {
        response.status shouldBe Status.Ok
        json.hcursor.get[String]("status").toOption shouldBe Some("new")
        json.hcursor.get[String]("idempotencyKey").toOption shouldBe
          Some("payment:abc-123")
      }
    }

    "POST /v1/idempotency/check should return 'in_progress' for second request" in {
      val body = """{"idempotencyKey": "payment:dup-456"}"""
      val request = Request[IO](Method.POST, uri"/v1/idempotency/check")
        .withEntity(body).putHeaders(
          headers.`Content-Type`(MediaType.application.json),
          headers.Authorization(Credentials.Token(ci"Bearer", "test-key")),
        )

      val test = for {
        _ <- httpApp.run(request)
        response <- httpApp.run(request)
        bodyText <- response.as[String]
        json <- IO.fromEither(parse(bodyText))
      } yield (response, json)

      test.asserting { case (response, json) =>
        response.status shouldBe Status.Accepted
        json.hcursor.get[String]("status").toOption shouldBe Some("in_progress")
      }
    }

    "POST /v1/idempotency/:key/complete should store response" in {
      val checkBody = """{"idempotencyKey": "store-test-key"}"""
      val storeBody =
        """{
        "statusCode": 201,
        "body": "{\"orderId\": \"order-789\"}",
        "headers": {"X-Request-Id": "req-123"}
      }"""

      val checkRequest = Request[IO](Method.POST, uri"/v1/idempotency/check")
        .withEntity(checkBody).putHeaders(
          headers.`Content-Type`(MediaType.application.json),
          headers.Authorization(Credentials.Token(ci"Bearer", "test-key")),
        )
      val storeRequest =
        Request[IO](Method.POST, uri"/v1/idempotency/store-test-key/complete")
          .withEntity(storeBody).putHeaders(
            headers.`Content-Type`(MediaType.application.json),
            headers.Authorization(Credentials.Token(ci"Bearer", "test-key")),
          )

      val test = for {
        // Create idempotency key
        _ <- httpApp.run(checkRequest)

        // Store response
        storeResponse <- httpApp.run(storeRequest)

        // Check again - should get cached response
        checkResponse <- httpApp.run(checkRequest)
        responseBody <- checkResponse.as[String]
      } yield (storeResponse, responseBody)

      test.asserting { case (storeResponse, body) =>
        storeResponse.status shouldBe Status.Ok
        body should include("duplicate")
        body should include("originalResponse")
      }
    }
  }

  "Request body contract" - {

    def post(path: Uri, body: String): Request[IO] =
      Request[IO](Method.POST, path).withEntity(body).putHeaders(
        headers.`Content-Type`(MediaType.application.json),
        headers.Authorization(Credentials.Token(ci"Bearer", "test-key")),
      )

    "POST /v1/ratelimit/check accepts a body that omits the optional cost" in {
      val request = post(uri"/v1/ratelimit/check", """{"key": "default-cost"}""")
      for {
        response <- httpApp.run(request)
        body <- response.as[String]
        json <- IO.fromEither(parse(body))
      } yield {
        response.status shouldBe Status.Ok
        // capacity 10, default cost 1
        json.hcursor.get[Int]("tokensRemaining").toOption shouldBe Some(9)
      }
    }

    "POST /v1/quota/check accepts a body that omits estimatedOutputTokens" in {
      val request = post(
        uri"/v1/quota/check",
        """{"userId": "default-output", "estimatedInputTokens": 10}""",
      )
      httpApp.run(request).asserting(_.status shouldBe Status.Ok)
    }

    "an undecodable body is a 400, not a 500" in
      httpApp.run(post(uri"/v1/ratelimit/check", "not json"))
        .asserting(_.status shouldBe Status.BadRequest)

    "a body missing a required field is a 422, not a 500" in
      httpApp.run(post(uri"/v1/ratelimit/check", """{"cost": 1}"""))
        .asserting(_.status shouldBe Status.UnprocessableEntity)
  }

  "Authorization" - {

    def withKey(request: Request[IO], key: String): Request[IO] = request
      .putHeaders(headers.Authorization(Credentials.Token(ci"Bearer", key)))

    def checkNaming(profile: String, key: String): Request[IO] = withKey(
      Request[IO](Method.POST, uri"/v1/ratelimit/check").withEntity(
        s"""{"key": "profile-bypass", "cost": 1, "profile": "$profile"}""",
      ).putHeaders(headers.`Content-Type`(MediaType.application.json)),
      key,
    )

    def metrics(key: Option[String]): IO[Response[IO]] =
      val request = Request[IO](Method.GET, uri"/metrics")
      httpApp.run(key.fold(request)(withKey(request, _)))

    // Routes.apply is the wiring Main uses: the dashboard exists only when it
    // is handed the decision queue.
    def appWithDashboardQueue(
        queue: Option[Queue[IO, RateLimitEvent]],
    ): IO[HttpApp[IO]] = Routes[IO](
      rateLimitStore,
      idempotencyStore,
      eventPublisher,
      metricsPublisher,
      authMiddleware,
      testRateLimitConfig,
      testIdempotencyConfig,
      logger,
      dashboardEventQueue = queue,
      healthCheck = IO.pure(AggregateHealth("ok", Nil)),
      getRequestId = () => IO.pure("test-request-id"),
    ).map(_.httpApp)

    "a free key naming the enterprise profile is 403 and consumes nothing" in {
      val status = withKey(
        Request[IO](Method.GET, uri"/v1/ratelimit/status/profile-bypass"),
        "test-key",
      )
      for {
        refused <- httpApp.run(checkNaming("enterprise", "test-key"))
        body <- refused.as[String]
        after <- httpApp.run(status).flatMap(_.as[String])
        json <- IO.fromEither(parse(after))
      } yield {
        refused.status shouldBe Status.Forbidden
        body should include("profile_not_permitted")
        json.hcursor.get[Int]("tokensRemaining").toOption shouldBe Some(10)
      }
    }

    "an enterprise key may name the enterprise profile" in
      httpApp.run(checkNaming("enterprise", "admin-key"))
        .asserting(_.status shouldBe Status.Ok)

    "an unknown profile is 400" in httpApp.run(checkNaming("gold", "test-key"))
      .asserting(_.status shouldBe Status.BadRequest)

    // Only /metrics used to check a permission, so a key's grants decided
    // nothing on these routes, and quota and complete had no permission to
    // grant. Each route must refuse exactly the key that lacks its own.
    def jsonPost(path: Uri, body: String): Request[IO] =
      Request[IO](Method.POST, path).withEntity(body)
        .putHeaders(headers.`Content-Type`(MediaType.application.json))

    val guarded: List[(Permission, Request[IO])] = List(
      Permission.RateLimitCheck ->
        jsonPost(uri"/v1/ratelimit/check", """{"key": "perm-guard"}"""),
      Permission.RateLimitStatus ->
        Request[IO](Method.GET, uri"/v1/ratelimit/status/perm-guard"),
      Permission.IdempotencyCheck -> jsonPost(
        uri"/v1/idempotency/check",
        """{"idempotencyKey": "perm-guard"}""",
      ),
      Permission.IdempotencyComplete -> jsonPost(
        uri"/v1/idempotency/perm-guard/complete",
        """{"statusCode": 200, "body": "{}"}""",
      ),
      Permission.IdempotencyComplete ->
        Request[IO](Method.POST, uri"/v1/idempotency/perm-guard/fail"),
      Permission.QuotaCheck -> jsonPost(
        uri"/v1/quota/check",
        """{"userId": "perm-guard", "estimatedInputTokens": 1}""",
      ),
      Permission.QuotaReconcile -> jsonPost(
        uri"/v1/quota/reconcile",
        """{"userId": "perm-guard", "actualInputTokens": 1, "actualOutputTokens": 0, "estimatedInputTokens": 1, "estimatedOutputTokens": 0}""",
      ),
    )

    "every standard permission guards a route" in
      IO(guarded.map(_._1).toSet shouldBe Permission.standard)

    guarded.foreach { case (permission, request) =>
      s"${request.method} ${request
          .uri} is 403 without $permission and served with it" in {
        for {
          refused <- httpApp.run(withKey(request, s"lacks-$permission"))
          body <- refused.as[String]
          served <- httpApp.run(withKey(request, "test-key"))
        } yield {
          refused.status shouldBe Status.Forbidden
          body should startWith("""{"error":"forbidden","message":""")
          body should include(permission.toString)
          served.status should not be Status.Forbidden
        }
      }
    }

    "GET /metrics without a key is 401" in metrics(None)
      .asserting(_.status shouldBe Status.Unauthorized)

    "GET /metrics with a key lacking AdminMetrics is 403" in
      metrics(Some("test-key")).asserting(_.status shouldBe Status.Forbidden)

    "GET /metrics with an admin key is the Prometheus exposition" in
      metrics(Some("admin-key")).flatMap(r => r.as[String].map((r.status, _)))
        .asserting { case (status, body) =>
          status shouldBe Status.Ok
          body should include("gate_requests_total")
        }

    "without the dashboard queue, dashboard routes are not served" in {
      val config = Request[IO](Method.POST, uri"/dashboard/api/config")
        .withEntity(
          """{"capacity": 1, "refillRatePerSecond": 1, "ttlSeconds": 1}""",
        )
      for {
        app <- appWithDashboardQueue(None)
        anonymous <- app.run(config)
        authed <- app.run(withKey(config, "test-key"))
      } yield {
        anonymous.status shouldBe Status.Unauthorized
        authed.status shouldBe Status.NotFound
      }
    }

    "GET /metrics reflects the traffic that went through the API" in {
      def post(path: Uri, body: String): Request[IO] = withKey(
        Request[IO](Method.POST, path).withEntity(body)
          .putHeaders(headers.`Content-Type`(MediaType.application.json)),
        "test-key",
      )
      // simpleclient writes `name{a="x",b="y",} 3.0`; the series is identified
      // by everything before the value.
      def value(body: String, series: String): Double = body.linesIterator
        .find(_.startsWith(series)).flatMap(_.split(' ').lastOption)
        .map(_.toDouble).getOrElse(0.0)
      val series = List(
        "gate_requests_total{tier=\"free\",result=\"allowed\",}",
        "gate_idempotency_total{result=\"new\",}",
        "gate_quota_tokens_admitted_total{level=\"user\",}",
      )
      def snapshot: IO[List[Double]] = metrics(Some("admin-key"))
        .flatMap(_.as[String]).map(body => series.map(value(body, _)))
      for {
        before <- snapshot
        _ <- httpApp
          .run(post(uri"/v1/ratelimit/check", """{"key": "metrics-e2e"}"""))
        _ <- httpApp.run(post(
          uri"/v1/idempotency/check",
          """{"idempotencyKey": "metrics-e2e"}""",
        ))
        _ <- httpApp.run(post(
          uri"/v1/quota/check",
          """{"userId": "metrics-e2e", "estimatedInputTokens": 10, "estimatedOutputTokens": 5}""",
        ))
        after <- snapshot
      } yield after.zip(before).map(_ - _) shouldBe List(1.0, 1.0, 15.0)
    }

    "with the dashboard queue, dashboard routes are served" in {
      for {
        queue <- Queue.bounded[IO, RateLimitEvent](8)
        app <- appWithDashboardQueue(Some(queue))
        response <- app.run(Request[IO](Method.GET, uri"/dashboard/api/config"))
      } yield response.status shouldBe Status.Ok
    }
  }

  "Error contract" - {
    // Every answer that is not a 2xx carries {"error": code, "message": text}.
    // There were five shapes, including an empty 401 and plain text for an
    // undecodable body, so a client had to know which route refused it before
    // it could read why. Each route is driven to each status it can answer.

    def withKey(request: Request[IO], key: String = "test-key"): Request[IO] =
      request.putHeaders(headers.Authorization(Credentials.Token(ci"Bearer", key)))

    def post(path: Uri, body: String): Request[IO] =
      Request[IO](Method.POST, path).withEntity(body)
        .putHeaders(headers.`Content-Type`(MediaType.application.json))

    def get(path: Uri): Request[IO] = Request[IO](Method.GET, path)

    // Stores pointed at a table that does not exist: DynamoDB answers, but
    // never with data, which is a store failure the routes must report.
    lazy val brokenRateLimitStore = new DynamoDBRateLimitStore[IO](
      dynamoDbClient,
      "no-such-table",
      MetricsPublisher.noop[IO],
    )
    lazy val brokenIdempotencyStore =
      new DynamoDBIdempotencyStore[IO](dynamoDbClient, "no-such-table")
    lazy val brokenQuotaApi = TokenQuotaApi[IO](
      TokenQuotaService[IO](
        DynamoDBTokenQuotaStore[IO](
          dynamoDbClient,
          "no-such-table",
          logger,
          metricsPublisher,
        ),
        testTokenQuotaConfig,
        metricsPublisher,
        logger,
      ),
      eventPublisher,
      metricsPublisher,
      logger,
      () => IO.pure("test-request-id"),
    )

    def app(
        rateLimit: RateLimitStore[IO] = rateLimitStore,
        idempotency: IdempotencyStore[IO] = idempotencyStore,
        quota: Option[TokenQuotaApi[IO]] = Some(tokenQuotaApi),
        prometheus: Option[PrometheusMetrics[IO]] = Some(prometheusMetrics),
        auth: org.http4s.server.AuthMiddleware[IO, AuthenticatedClient] =
          authMiddleware,
    ): HttpApp[IO] = new Routes[IO](
      rateLimit,
      idempotency,
      eventPublisher,
      metricsPublisher,
      auth,
      testRateLimitConfig,
      testIdempotencyConfig,
      logger,
      dashboardApi = None,
      tokenQuotaApi = quota,
      prometheusMetrics = prometheus,
      healthCheck = IO.pure(AggregateHealth("ok", Nil)),
      getRequestId = () => IO.pure("test-request-id"),
    ).httpApp

    /** The status, and a body that is exactly the error contract: a JSON object
      * whose `error` is `code` and whose `message` is non-empty text.
      */
    def assertError(
        response: Response[IO],
        status: Status,
        code: String,
    ): IO[org.scalatest.Assertion] = response.as[String].map { body =>
      val cursor = parse(body).toOption.map(_.hcursor)
      withClue(s"${response.status} $body: ") {
        response.status shouldBe status
        response.contentType.map(_.mediaType) shouldBe
          Some(MediaType.application.json)
        cursor.flatMap(_.get[String]("error").toOption) shouldBe Some(code)
        cursor.flatMap(_.get[String]("message").toOption)
          .exists(_.nonEmpty) shouldBe true
      }
    }

    def expect(name: String, status: Status, code: String)(
        response: => IO[Response[IO]],
    ): Unit = s"$name is ${status.code} $code" in
      response.flatMap(assertError(_, status, code))

    val jsonRoutes: List[Uri] = List(
      uri"/v1/ratelimit/check",
      uri"/v1/idempotency/check",
      uri"/v1/idempotency/contract-key/complete",
      uri"/v1/quota/check",
      uri"/v1/quota/reconcile",
    )

    val everyRoute: List[Request[IO]] = jsonRoutes.map(post(_, "{}")) ++ List(
      Request[IO](Method.POST, uri"/v1/idempotency/contract-key/fail"),
      get(uri"/v1/ratelimit/status/contract-key"),
      get(uri"/metrics"),
    )

    "authentication" - {
      everyRoute.foreach { request =>
        expect(
          s"${request.method} ${request.uri} without a key",
          Status.Unauthorized,
          "unauthorized",
        )(httpApp.run(request))
        expect(
          s"${request.method} ${request.uri} with an unknown key",
          Status.Unauthorized,
          "unauthorized",
        )(httpApp.run(withKey(request, "no-such-key")))
      }

      "the auth throttle is 429 rate_limited, with retryAfter beside the two fields" in {
        val request = withKey(get(uri"/v1/ratelimit/status/throttled"))
        for {
          limiter <- AuthRateLimiter.inMemory[IO](1)
          throttled =
            app(auth = ApiKeyAuth.middleware[IO](testApiKeyStore, Some(limiter)))
          _ <- throttled.run(request)
          second <- throttled.run(request)
          retryAfter = second.headers.get(ci"Retry-After").map(_.head.value)
          body <- second.as[String].flatMap(b => IO.fromEither(parse(b)))
        } yield {
          second.status shouldBe Status.TooManyRequests
          body.hcursor.get[String]("error").toOption shouldBe
            Some("rate_limited")
          body.hcursor.get[String]("message").toOption
            .exists(_.nonEmpty) shouldBe true
          body.hcursor.get[Int]("retryAfter").toOption.map(_.toString) shouldBe
            retryAfter
        }
      }

      expect("a route without its permission", Status.Forbidden, "forbidden")(
        httpApp.run(withKey(get(uri"/metrics"))),
      )
    }

    "paths and bodies" - {
      expect("an unknown path", Status.NotFound, "not_found")(httpApp.run(
        withKey(get(uri"/v1/no-such-route")),
      ))

      jsonRoutes.foreach { path =>
        expect(
          s"POST $path with a body that is not JSON",
          Status.BadRequest,
          "invalid_request",
        )(httpApp.run(withKey(post(path, "not json"))))
        expect(
          s"POST $path with a body missing its fields",
          Status.UnprocessableEntity,
          "invalid_request",
        )(httpApp.run(withKey(post(path, "{}"))))
      }

      "a body with a field of the wrong type names the field, not its value" in
        httpApp.run(withKey(post(
          uri"/v1/ratelimit/check",
          """{"key": "typed", "cost": "sekrit"}""",
        ))).flatMap(_.as[String]).asserting { body =>
          body should include(".cost")
          (body should not).include("sekrit")
        }

      expect(
        "an unhandled failure",
        Status.InternalServerError,
        "internal_error",
      )(
        // The raw store, with no resilient wrapper to turn it into a degraded
        // decision.
        app(rateLimit = brokenRateLimitStore)
          .run(withKey(post(uri"/v1/ratelimit/check", """{"key": "boom"}"""))),
      )
    }

    "POST /v1/ratelimit/check" - {
      expect("a cost of zero", Status.BadRequest, "validation_error")(
        httpApp.run(withKey(
          post(uri"/v1/ratelimit/check", """{"key": "c", "cost": 0}"""),
        )),
      )
      expect(
        "a profile above the tier",
        Status.Forbidden,
        "profile_not_permitted",
      )(httpApp.run(withKey(post(
        uri"/v1/ratelimit/check",
        """{"key": "c", "profile": "enterprise"}""",
      ))))

      "an empty bucket is 429 rate_limit_exceeded, still with allowed and no degraded header" in {
        val drain = withKey(
          post(uri"/v1/ratelimit/check", """{"key": "drained", "cost": 10}"""),
        )
        for {
          first <- httpApp.run(drain)
          second <- httpApp.run(drain)
          body <- second.as[String].flatMap(b => IO.fromEither(parse(b)))
          _ <- httpApp.run(drain).flatMap(
            assertError(_, Status.TooManyRequests, "rate_limit_exceeded"),
          )
        } yield {
          first.headers.get(ci"X-Gate-Degraded") shouldBe None
          second.headers.get(ci"X-Gate-Degraded") shouldBe None
          body.hcursor.get[Boolean]("allowed").toOption shouldBe Some(false)
        }
      }
    }

    "GET /v1/ratelimit/status/:key" - expect(
      "an unreadable store",
      Status.ServiceUnavailable,
      "storage_unavailable",
    )(app(rateLimit = brokenRateLimitStore).run(withKey(
      get(uri"/v1/ratelimit/status/any"),
    )))

    "POST /v1/idempotency/check" - {
      expect("a ttl of zero", Status.BadRequest, "validation_error")(httpApp.run(
        withKey(post(
          uri"/v1/idempotency/check",
          """{"idempotencyKey": "k", "ttl": 0}""",
        )),
      ))
      expect(
        "a different body for a claimed key",
        Status.Conflict,
        "idempotency_conflict",
      )(
        httpApp.run(withKey(post(
          uri"/v1/idempotency/check",
          """{"idempotencyKey": "mismatch", "requestBody": "a"}""",
        ))) *> httpApp.run(withKey(post(
          uri"/v1/idempotency/check",
          """{"idempotencyKey": "mismatch", "requestBody": "b"}""",
        ))),
      )
      expect("a failing store", Status.ServiceUnavailable, "storage_unavailable")(
        app(idempotency = brokenIdempotencyStore).run(withKey(
          post(uri"/v1/idempotency/check", """{"idempotencyKey": "k"}"""),
        )),
      )
    }

    "POST /v1/idempotency/:key/complete" - {
      val done = """{"statusCode": 200, "body": "{}"}"""
      expect("a key that is not pending", Status.Conflict, "not_pending")(
        httpApp
          .run(withKey(post(uri"/v1/idempotency/never-claimed/complete", done))),
      )
      expect(
        "a response over the stored limit",
        Status.PayloadTooLarge,
        "response_too_large",
      )(httpApp.run(withKey(post(
        uri"/v1/idempotency/too-large/complete",
        s"""{"statusCode": 200, "body": "${"x" * 360000}"}""",
      ))))
      expect("a failing store", Status.ServiceUnavailable, "storage_unavailable")(
        app(idempotency = brokenIdempotencyStore)
          .run(withKey(post(uri"/v1/idempotency/any/complete", done))),
      )
    }

    "POST /v1/idempotency/:key/fail" - {
      val fail =
        Request[IO](Method.POST, uri"/v1/idempotency/never-claimed/fail")
      expect("a key that is not pending", Status.Conflict, "not_pending")(
        httpApp.run(withKey(fail)),
      )
      expect("a failing store", Status.ServiceUnavailable, "storage_unavailable")(
        app(idempotency = brokenIdempotencyStore).run(withKey(fail)),
      )
    }

    "POST /v1/quota/check" - {
      def check(user: String, tokens: Long) = withKey(post(
        uri"/v1/quota/check",
        s"""{"userId": "$user", "estimatedInputTokens": $tokens}""",
      ))
      expect("a negative estimate", Status.BadRequest, "validation_error")(
        httpApp.run(check("contract-neg", -1)),
      )
      expect(
        "an estimate above the limit",
        Status.BadRequest,
        "validation_error",
      )(httpApp.run(check("contract-big", 101)))
      expect("a spent quota", Status.TooManyRequests, "quota_exceeded")(
        httpApp.run(check("contract-spent", 100)) *>
          httpApp.run(check("contract-spent", 1)),
      )
      expect("a failing store", Status.ServiceUnavailable, "storage_unavailable")(
        app(quota = Some(brokenQuotaApi)).run(check("contract-down", 1)),
      )
      expect("quotas disabled", Status.NotFound, "not_found")(
        app(quota = None).run(check("contract-off", 1)),
      )
    }

    "POST /v1/quota/reconcile" - {
      def reconcile(id: String, actual: Long) =
        withKey(post(uri"/v1/quota/reconcile", s"""{"reservationId": "$id", "actualInputTokens": $actual, "actualOutputTokens": 0}"""))
      expect("a negative count", Status.BadRequest, "validation_error")(
        httpApp.run(reconcile("any", -1)),
      )
      expect("an unknown reservation", Status.NotFound, "reservation_not_found")(
        httpApp.run(reconcile("no-such-reservation", 1)),
      )
      expect(
        "a second reconcile with different usage",
        Status.Conflict,
        "already_reconciled",
      )(
        for {
          reserved <- httpApp.run(withKey(post(
            uri"/v1/quota/check",
            """{"userId": "contract-twice", "estimatedInputTokens": 50}""",
          ))).flatMap(_.as[String])
          id <- IO.fromEither(
            parse(reserved).flatMap(_.hcursor.get[String]("reservationId")),
          )
          _ <- httpApp.run(reconcile(id, 50))
          again <- httpApp.run(reconcile(id, 10))
        } yield again,
      )
      expect("a failing store", Status.ServiceUnavailable, "storage_unavailable")(
        app(quota = Some(brokenQuotaApi)).run(reconcile("any", 1)),
      )
      expect("quotas disabled", Status.NotFound, "not_found")(
        app(quota = None).run(reconcile("any", 1)),
      )
    }

    "GET /metrics" - expect("Prometheus disabled", Status.NotFound, "not_found")(
      app(prometheus = None).run(withKey(get(uri"/metrics"), "admin-key")),
    )

    "POST /dashboard/api/config" -
      expect("a capacity of zero", Status.BadRequest, "validation_error") {
        for {
          queue <- Queue.bounded[IO, RateLimitEvent](8)
          dashboard <- Routes[IO](
            rateLimitStore,
            idempotencyStore,
            eventPublisher,
            metricsPublisher,
            authMiddleware,
            testRateLimitConfig,
            testIdempotencyConfig,
            logger,
            dashboardEventQueue = Some(queue),
            healthCheck = IO.pure(AggregateHealth("ok", Nil)),
            getRequestId = () => IO.pure("test-request-id"),
          )
          response <- dashboard.httpApp.run(post(
            uri"/dashboard/api/config",
            """{"capacity": 0, "refillRatePerSecond": 1, "ttlSeconds": 1}""",
          ))
        } yield response
      }
  }

  "Degraded answers" - {
    // When the store cannot answer, the degradation mode does. A reject-all
    // 429 used to be indistinguishable from an empty bucket, and an allow-all
    // 200 reported 100 tokens left whatever the profile's capacity.

    // No retries, breaker, or bulkhead, so the first failure degrades.
    val noPatterns: ResilienceConfig = ResilienceConfig(
      circuitBreaker = CircuitBreakerSettings(
        enabled = false,
        dynamodb = config.CircuitBreakerConfig(),
      ),
      retry = RetrySettings(dynamodb =
        RetryConfig(
          maxRetries = 0,
          baseDelay = 1.millis,
          maxDelay = 1.millis,
          multiplier = 1.0,
        ),
      ),
      bulkhead = BulkheadSettings(enabled = false),
      timeout = TimeoutSettings(rateLimitCheck = 5.seconds),
    )

    val check = Request[IO](Method.POST, uri"/v1/ratelimit/check")
      .withEntity("""{"key": "degraded"}""").putHeaders(
        headers.`Content-Type`(MediaType.application.json),
        headers.Authorization(Credentials.Token(ci"Bearer", "test-key")),
      )

    def degradedCheck(
        mode: GracefulDegradation.DegradationMode,
    ): IO[(Response[IO], io.circe.Json)] = ResilientRateLimitStore[IO](
      new DynamoDBRateLimitStore[IO](
        dynamoDbClient,
        "no-such-table",
        MetricsPublisher.noop[IO],
      ),
      noPatterns,
      MetricsPublisher.noop[IO],
      mode,
    ).use { store =>
      val app = new Routes[IO](
        store,
        idempotencyStore,
        eventPublisher,
        metricsPublisher,
        authMiddleware,
        testRateLimitConfig,
        testIdempotencyConfig,
        logger,
        dashboardApi = None,
        tokenQuotaApi = None,
        prometheusMetrics = None,
        healthCheck = IO.pure(AggregateHealth("ok", Nil)),
        getRequestId = () => IO.pure("test-request-id"),
      ).httpApp
      for {
        response <- app.run(check)
        body <- response.as[String].flatMap(b => IO.fromEither(parse(b)))
      } yield (response, body)
    }

    def header(response: Response[IO], name: String): Option[String] = response
      .headers.get(org.typelevel.ci.CIString(name)).map(_.head.value)

    "reject-all is a 429 that says it is degraded, in a header and in the error" in
      degradedCheck(GracefulDegradation.DegradationMode.RejectAll)
        .asserting { case (response, body) =>
          response.status shouldBe Status.TooManyRequests
          header(response, "X-Gate-Degraded") shouldBe Some("true")
          header(response, "Retry-After") shouldBe Some("60")
          body.hcursor.get[String]("error").toOption shouldBe Some("degraded")
          body.hcursor.get[String]("message").toOption
            .exists(_.nonEmpty) shouldBe true
          body.hcursor.get[Boolean]("allowed").toOption shouldBe Some(false)
          body.hcursor.get[Int]("limit").toOption shouldBe Some(10)
        }

    "allow-all is a 200 with the header and the profile's capacity, not 100" in
      degradedCheck(GracefulDegradation.DegradationMode.AllowAll)
        .asserting { case (response, body) =>
          response.status shouldBe Status.Ok
          header(response, "X-Gate-Degraded") shouldBe Some("true")
          body.hcursor.get[Boolean]("allowed").toOption shouldBe Some(true)
          body.hcursor.get[Int]("tokensRemaining").toOption shouldBe Some(10)
          body.hcursor.get[Int]("limit").toOption shouldBe Some(10)
        }
  }
}
