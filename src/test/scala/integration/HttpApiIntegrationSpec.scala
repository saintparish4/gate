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
  DashboardApi, RateLimitCheckResponse, RateLimitStatusResponse, Routes,
}
import config.{IdempotencyConfig, RateLimitConfig}
import events.EventPublisher
import observability.MetricsPublisher
import resilience.AggregateHealth
import security.{
  ApiKeyAuth, ApiKeyStore, AuthenticatedClient, ClientTier, Permission,
}
import storage.{DynamoDBIdempotencyStore, DynamoDBRateLimitStore}
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import io.circe.generic.auto.*
import io.circe.parser.*

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

  // Test configuration
  lazy val testRateLimitConfig: RateLimitConfig = RateLimitConfig(
    defaultCapacity = 10,
    defaultRefillRatePerSecond = 0.1,
    defaultTtlSeconds = 3600,
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

  // No-op metrics publisher for tests
  lazy val metricsPublisher: MetricsPublisher[IO] = MetricsPublisher.noop[IO]

  // Test API key store
  lazy val testApiKeyStore: ApiKeyStore[IO] = ApiKeyStore.inMemory[IO](Map(
    "test-key" -> AuthenticatedClient(
      apiKeyId = "test-client-1",
      clientId = "test-client-1",
      clientName = "Test Client",
      tier = ClientTier.Free, // Use Free tier (capacity 10) to match test expectations
      permissions = Permission.standard,
    ),
  ))

  // Auth middleware
  lazy val authMiddleware = ApiKeyAuth.middleware[IO](testApiKeyStore, None)

  // Dashboard API (no SSE queue for integration tests)
  lazy val dashboardApi: DashboardApi[IO] = DashboardApi
    .apply[IO](rateLimitStore, testRateLimitConfig, logger, None, eventPublisher)
    .unsafeRunSync()

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
    dashboardApi,
    tokenQuotaApi = None,
    prometheusMetrics = None,
    healthCheck = IO.pure(AggregateHealth("ok", Nil)),
    getRequestId = () => IO.pure("test-request-id"),
  )

  lazy val httpApp: HttpApp[IO] = routes.httpApp

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    clearTable(testDynamoDBConfig.rateLimitTable)
    clearTable(testDynamoDBConfig.idempotencyTable)
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
        body should include("0.2.0")
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
}
