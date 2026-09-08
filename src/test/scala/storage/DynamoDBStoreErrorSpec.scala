package storage

import java.lang.reflect.{InvocationHandler, Proxy}
import java.util.concurrent.CompletableFuture

import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

import org.http4s.*
import org.http4s.circe.*
import org.http4s.implicits.*
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.typelevel.ci.*
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.noop.NoOpLogger
import org.typelevel.otel4s.trace.Tracer.Implicits.noop

import config.*
import core.*
import events.EventPublisher
import observability.MetricsPublisher
import resilience.*
import security.{AuthenticatedClient, ClientTier, Permission}
import api.IdempotencyApi
import testutil.*
import cats.effect.*
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all.*
import io.circe.parser.*
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.*

/** Tests for error handling in DynamoDB-backed stores and the resilient
  * wrapper.
  *
  * Tests are grouped into three areas:
  *
  *   1. `ResilientRateLimitStore` degrades gracefully when the underlying store
  *      raises DynamoDB-style exceptions (fails open in AllowAll mode, logs the
  *      error, increments the `RateLimitDegraded` metric).
  *   2. `DynamoDBRateLimitStore` with a stubbed DynamoDB client that returns
  *      corrupt data → fails open on the corrupt key (grants full capacity),
  *      logs an ERROR, and increments the `CorruptStateRead` metric.
  *   3. `IdempotencyApi` maps `CorruptIdempotencyRecordException` to a 503 with
  *      a structured `storage_corruption` error body.
  */
class DynamoDBStoreErrorSpec
    extends AsyncFreeSpec with AsyncIOSpec with Matchers:

  // ── helpers ────────────────────────────────────────────────────────────────

  given Logger[IO] = NoOpLogger[IO]

  /** ResilienceConfig tuned for fast unit tests: no retries, no circuit
    * breaker, no bulkhead, generous timeout so the failure is never a timeout.
    */
  private val fastResilienceConfig: ResilienceConfig = ResilienceConfig(
    circuitBreaker = CircuitBreakerSettings(
      enabled = false,
      dynamodb = config.CircuitBreakerConfig(),
      kinesis = config.CircuitBreakerConfig(),
    ),
    retry = RetrySettings(
      dynamodb = RetryConfig(
        maxRetries = 0,
        baseDelay = 1.millis,
        maxDelay = 1.millis,
        multiplier = 1.0,
      ),
      kinesis = RetryConfig(),
    ),
    bulkhead = BulkheadSettings(enabled = false),
    timeout = TimeoutSettings(rateLimitCheck = 5.seconds),
  )

  /** Build a DynamoDbAsyncClient proxy that returns a fixed GetItemResponse.
    * putItem and updateItem return empty success responses. All other calls
    * fail immediately — if a test inadvertently calls them the test will fail.
    */
  private def stubClient(getItemResp: GetItemResponse): DynamoDbAsyncClient =
    Proxy.newProxyInstance(
      classOf[DynamoDbAsyncClient].getClassLoader,
      Array(classOf[DynamoDbAsyncClient]),
      new InvocationHandler:
        override def invoke(
            proxy: Object,
            method: java.lang.reflect.Method,
            args: Array[Object],
        ): Object = method.getName match
          case "getItem" => CompletableFuture.completedFuture(getItemResp)
          case "putItem" => CompletableFuture
              .completedFuture(PutItemResponse.builder().build())
          case "updateItem" => CompletableFuture
              .completedFuture(UpdateItemResponse.builder().build())
          case "describeTable" => CompletableFuture
              .completedFuture(DescribeTableResponse.builder().build())
          case "serviceName" => "DynamoDB"
          case "close" => null // void
          case other => CompletableFuture
              .failedFuture[Object](new UnsupportedOperationException(
                s"Stub does not implement: $other",
              )),
    ).asInstanceOf[DynamoDbAsyncClient]

  /** A GetItemResponse that contains an existing rate-limit item with a
    * non-numeric "tokens" attribute value — this triggers the corrupt-state
    * path in parseState.
    *
    * NOTE: "NaN".toDouble succeeds in Scala/Java (returns Double.NaN). We must
    * use a string that actually throws NumberFormatException so that parseState
    * returns Left and the store falls back to full-capacity (fails open).
    */
  private def corruptTokensResponse(key: String): GetItemResponse =
    val item = Map(
      "pk" -> AttributeValue.builder().s(s"ratelimit#$key").build(),
      "tokens" -> AttributeValue.builder().n("not_a_number").build(), // throws NFE
      "lastRefillMs" -> AttributeValue.builder().n("0").build(),
      "version" -> AttributeValue.builder().n("1").build(),
    ).asJava
    GetItemResponse.builder().item(item).build()

  // ── 5.4a: ResilientRateLimitStore degradation ──────────────────────────────

  "ResilientRateLimitStore — AllowAll degradation mode" - {

    "fails open (returns Allowed) when the underlying store raises an exception" in {
      val ex = new RuntimeException("simulated DynamoDB failure")

      ResilientRateLimitStore(
        failingStore(ex),
        fastResilienceConfig,
        MetricsPublisher.noop[IO],
        EventPublisher.noop[IO],
        GracefulDegradation.DegradationMode.AllowAll,
      ).use(resilientStore =>
        resilientStore.checkAndConsume("test-key", cost = 1, testProfile)
          .asserting { decision =>
            decision shouldBe a[RateLimitDecision.Allowed]
            // GracefulDegradation.AllowAll grants 100 tokens as a sentinel value
            decision.asInstanceOf[RateLimitDecision.Allowed]
              .tokensRemaining shouldBe 100
          },
      )
    }

    "fails open when the underlying store raises ProvisionedThroughputExceededException" in {
      val ex = software.amazon.awssdk.services.dynamodb.model
        .ProvisionedThroughputExceededException.builder()
        .message("Throughput exceeded").build()

      ResilientRateLimitStore(
        failingStore(ex),
        fastResilienceConfig,
        MetricsPublisher.noop[IO],
        EventPublisher.noop[IO],
        GracefulDegradation.DegradationMode.AllowAll,
      ).use(resilientStore =>
        resilientStore.checkAndConsume("throttled-key", cost = 1, testProfile)
          .asserting(decision => decision shouldBe a[RateLimitDecision.Allowed]),
      )
    }

    "increments RateLimitDegraded metric on any underlying failure" in {
      val ex = new java.io.IOException("simulated network timeout")

      (for
        metricNames <- Ref.of[IO, List[String]](Nil)
        metrics = capturingMetrics(metricNames)

        _ <- ResilientRateLimitStore(
          failingStore(ex),
          fastResilienceConfig,
          metrics,
          EventPublisher.noop[IO],
          GracefulDegradation.DegradationMode.AllowAll,
        ).use(resilientStore =>
          resilientStore.checkAndConsume("metric-key", cost = 1, testProfile),
        )

        recorded <- metricNames.get
      yield recorded)
        .asserting(metrics => metrics should contain("RateLimitDegraded"))
    }

    "rejects all requests in RejectAll degradation mode" in {
      val ex = new RuntimeException("store down")

      ResilientRateLimitStore(
        failingStore(ex),
        fastResilienceConfig,
        MetricsPublisher.noop[IO],
        EventPublisher.noop[IO],
        GracefulDegradation.DegradationMode.RejectAll,
      ).use(resilientStore =>
        resilientStore.checkAndConsume("reject-key", cost = 1, testProfile)
          .asserting(decision => decision shouldBe a[RateLimitDecision.Rejected]),
      )
    }
  }

  // ──  DynamoDBRateLimitStore corrupt-state path ────────────────────────

  "DynamoDBRateLimitStore — corrupt stored state" - {

    "fails open (grants full capacity) when stored tokens attribute is malformed" in {
      // The stub returns a GetItemResponse with tokens = "NaN" which cannot be
      // parsed as a Double.  DynamoDBRateLimitStore.getOrInitState detects the
      // Left parse result, logs an ERROR, increments CorruptStateRead, and
      // falls back to a fresh full-capacity bucket — i.e. allows the request.
      val key = "corrupt-state-key"
      val client = stubClient(corruptTokensResponse(key))

      (for
        errorLogs <- Ref.of[IO, List[String]](Nil)
        metricNames <- Ref.of[IO, List[String]](Nil)
        metricsPublisher = capturingMetrics(metricNames)
        given Logger[IO] = capturingLogger(errorLogs)
        store =
          DynamoDBRateLimitStore[IO](client, "test-table", metricsPublisher)
        decision <- store.checkAndConsume(key, cost = 1, testProfile)

        logs <- errorLogs.get
        recorded <- metricNames.get
      yield (decision, logs, recorded)).asserting {
        case (decision, logs, recorded) =>
          // 1. Fails open → request is allowed
          decision shouldBe a[RateLimitDecision.Allowed]

          // 2. An ERROR is logged mentioning the key and the corrupt-state reason
          logs
            .exists(msg =>
              msg.contains(key) && msg.contains("Corrupt"),
            ) shouldBe true

          // 3. CorruptStateRead metric is incremented
          recorded should contain("CorruptStateRead")
      }
    }
  }

  // ── IdempotencyApi 503 on corrupt record ─────────────────────────────

  "IdempotencyApi — corrupt idempotency record" - {

    "returns 503 with storage_corruption error when store raises CorruptIdempotencyRecordException" in {
      val corruptKey = "corrupt-idempotency-key"
      val detail = "unknown status value: 'INVALID_STATUS'"

      val corruptStore: IdempotencyStore[IO] = new IdempotencyStore[IO]:
        def check(
            key: String,
            clientId: String,
            ttlSeconds: Long,
            requestHash: Option[String] = None,
        ): IO[IdempotencyResult] = IO
          .raiseError(new CorruptIdempotencyRecordException(key, detail))
        def storeResponse(key: String, response: StoredResponse): IO[Boolean] =
          IO.pure(false)
        def markFailed(key: String): IO[Boolean] = IO.pure(false)
        def get(key: String): IO[Option[IdempotencyRecord]] = IO.pure(None)
        def healthCheck: IO[Either[String, Unit]] = IO.pure(Right(()))

      val idempotencyConfig =
        IdempotencyConfig(defaultTtlSeconds = 3600L, maxTtlSeconds = 3600L)

      val api = IdempotencyApi[IO](
        corruptStore,
        idempotencyConfig,
        EventPublisher.noop[IO],
        MetricsPublisher.noop[IO],
        summon[Logger[IO]],
        () => IO.pure("test-request-id"),
      )

      val body = s"""{"idempotencyKey": "$corruptKey"}"""
      val req = Request[IO](Method.POST, uri"/v1/idempotency/check")
        .withEntity(body).putHeaders(
          headers.`Content-Type`(MediaType.application.json),
          headers.Authorization(Credentials.Token(ci"Bearer", "test-key")),
        )

      (for
        response <- api.check(req, testClient)
        bodyText <- response.body.compile.toVector.map(_.map(_.toChar).mkString)
      yield (response.status, parse(bodyText).toOption))
        .asserting { case (status, maybeJson) =>
          status shouldBe Status.ServiceUnavailable
          val cursor = maybeJson.get.hcursor
          cursor.downField("error").as[String].toOption shouldBe
            Some("storage_corruption")
          cursor.downField("message").as[String].toOption
            .exists(_.contains(detail)) shouldBe true
        }
    }
  }

  // ── DynamoDBRateLimitStore: non-OCC failures ───────────────────────────────

  /** A client whose getItem fails with `ex`; every other call is unexpected. */
  private def failingGetItemClient(ex: Throwable): DynamoDbAsyncClient = Proxy
    .newProxyInstance(
      classOf[DynamoDbAsyncClient].getClassLoader,
      Array(classOf[DynamoDbAsyncClient]),
      new InvocationHandler:
        override def invoke(
            proxy: Object,
            method: java.lang.reflect.Method,
            args: Array[Object],
        ): Object = method.getName match
          case "getItem" => CompletableFuture.failedFuture[Object](ex)
          case "serviceName" => "DynamoDB"
          case "close" => null
          case other => CompletableFuture
              .failedFuture[Object](new UnsupportedOperationException(
                s"Stub does not implement: $other",
              )),
    ).asInstanceOf[DynamoDbAsyncClient]

  "DynamoDBRateLimitStore — non-OCC failures" - {
    "propagates an SDK client failure as itself, not as a MatchError" in {
      val ex = software.amazon.awssdk.core.exception.SdkClientException.builder()
        .message("Maximum pending connection acquisitions exceeded").build()
      val store = new DynamoDBRateLimitStore[IO](
        failingGetItemClient(ex),
        "rate-limits",
        MetricsPublisher.noop[IO],
      )

      store.checkAndConsume("k", 1, testProfile).attempt
        .asserting(_ shouldBe Left(ex))
    }
  }
