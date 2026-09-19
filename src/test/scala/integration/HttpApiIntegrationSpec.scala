package integration

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
  IdempotencyConfig, RateLimitConfig, RateLimitProfileConfig, TokenQuotaConfig,
}
import events.{EventPublisher, RateLimitEvent}
import observability.{MetricsPublisher, PrometheusMetrics}
import resilience.AggregateHealth
import security.{
  ApiKeyAuth, ApiKeyStore, AuthenticatedClient, ClientTier, Permission,
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
import core.TokenQuotaService

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
      "admin-key" -> AuthenticatedClient(
        apiKeyId = "admin-client-1",
        clientId = "admin-client-1",
        clientName = "Admin Client",
        tier = ClientTier.Enterprise,
        permissions = Permission.admin,
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

  "Token quota endpoints" - {

    def quotaCheck(userId: String, tokens: Long): Request[IO] = Request[IO](
      Method.POST,
      uri"/v1/quota/check",
    ).putHeaders(headers.Authorization(Credentials.Token(ci"Bearer", "test-key")))
      .withEntity(
        TokenQuotaCheckRequest(userId = userId, estimatedInputTokens = tokens)
          .asJson,
      )

    def quotaReconcile(
        userId: String,
        actual: Long,
        estimated: Long,
    ): Request[IO] = Request[IO](Method.POST, uri"/v1/quota/reconcile")
      .putHeaders(headers.Authorization(Credentials.Token(ci"Bearer", "test-key")))
      .withEntity(
        TokenQuotaReconcileRequest(
          userId = userId,
          actualInputTokens = actual,
          actualOutputTokens = 0,
          estimatedInputTokens = estimated,
          estimatedOutputTokens = 0,
        ).asJson,
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
        reconciled <- httpApp.run(quotaReconcile("quota-user-2", 30, 60))
        second <- httpApp.run(quotaCheck("quota-user-2", 60))
      } yield {
        first.status shouldBe Status.Ok
        reconciled.status shouldBe Status.Ok
        // 30 actually used + 60 requested = 90, inside the 100-token limit.
        second.status shouldBe Status.Ok
      }
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
}
