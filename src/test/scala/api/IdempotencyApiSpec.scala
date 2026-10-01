package api

import java.time.Instant

import org.http4s.*
import org.http4s.circe.*
import org.http4s.implicits.*
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.typelevel.ci.*
import org.typelevel.log4cats.Logger
import org.typelevel.otel4s.trace.Tracer.Implicits.noop

import config.IdempotencyConfig
import core.*
import events.EventPublisher
import observability.MetricsPublisher
import security.AuthenticatedClient
import testutil.*
import cats.effect.*
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all.*
import io.circe.parser.*

/** Unit tests for IdempotencyApi: TTL capping and warning when client TTL
  * exceeds max.
  */
class IdempotencyApiSpec extends AsyncFreeSpec with AsyncIOSpec with Matchers:

  "IdempotencyApi TTL capping" - {

    "should pass capped TTL to store when client requests TTL above max" in {
      val maxTtl = 3600L
      val requestedTtl = 100000L
      val config =
        IdempotencyConfig(defaultTtlSeconds = 86400, maxTtlSeconds = maxTtl)

      val test =
        for
          capturedTtl <- Ref[IO].of(Option.empty[Long])
          store = new IdempotencyStore[IO]:
            override def check(
                idempotencyKey: String,
                clientId: String,
                ttlSeconds: Long,
                requestHash: Option[String] = None,
            ): IO[IdempotencyResult] = capturedTtl.set(Some(ttlSeconds)) *>
              Clock[IO].realTime.map(d =>
                IdempotencyResult.New(
                  idempotencyKey,
                  Instant.ofEpochMilli(d.toMillis),
                  "claim-1",
                ),
              )
            override def storeResponse(
                idempotencyKey: String,
                clientId: String,
                response: StoredResponse,
                claimId: Option[String],
            ): IO[Boolean] = IO.pure(false)
            override def markFailed(
                idempotencyKey: String,
                clientId: String,
                claimId: Option[String],
            ): IO[Boolean] = IO.pure(false)
            override def get(
                idempotencyKey: String,
            ): IO[Option[IdempotencyRecord]] = IO.pure(None)
            override def healthCheck: IO[Either[String, Unit]] = IO.pure(Right(()))

          logger <- Ref[IO].of(List.empty[String])
            .map(ref => capturingLogger(ref))
          api = IdempotencyApi[IO](
            store,
            config,
            EventPublisher.noop[IO],
            MetricsPublisher.noop[IO],
            logger,
            () => IO.pure("test-request-id"),
          )
          body = s"""{"idempotencyKey": "cap-test", "ttl": $requestedTtl}"""
          req = Request[IO](Method.POST, uri"/v1/idempotency/check")
            .withEntity(body).putHeaders(
              headers.`Content-Type`(MediaType.application.json),
              headers.Authorization(Credentials.Token(ci"Bearer", "test-key")),
            )
          _ <- api.check(req, testClient)
          ttl <- capturedTtl.get
        yield ttl

      test.asserting(ttl => ttl shouldBe Some(maxTtl))
    }
  }

  "IdempotencyApi TTL validation" - {

    // A ttl of zero or less was accepted and wrote an already-expired record.
    "a ttl of zero or less is 400, and claims nothing" in {
      def check(api: IdempotencyApi[IO], ttl: Long) = api.check(
        Request[IO](Method.POST, uri"/v1/idempotency/check")
          .withEntity(s"""{"idempotencyKey": "ttl-test", "ttl": $ttl}""")
          .putHeaders(headers.`Content-Type`(MediaType.application.json)),
        testClient,
      )
      val test =
        for
          store <- IdempotencyStore.inMemory[IO]
          logger <- Ref[IO].of(List.empty[String]).map(capturingLogger)
          api = IdempotencyApi[IO](
            store,
            IdempotencyConfig(),
            EventPublisher.noop[IO],
            MetricsPublisher.noop[IO],
            logger,
            () => IO.pure("test-request-id"),
          )
          zero <- check(api, 0)
          negative <- check(api, -60)
          message <- negative.bodyText.compile.string
          valid <- check(api, 60)
        yield (zero.status, negative.status, message, valid.status)

      test.asserting { case (zero, negative, message, valid) =>
        zero shouldBe Status.BadRequest
        negative shouldBe Status.BadRequest
        message should include("ttl must be positive, got -60")
        // Still unclaimed: the first valid check is new.
        valid shouldBe Status.Ok
      }
    }
  }

  "IdempotencyApi TTL warning" - {

    "should log a warning when client TTL exceeds max" in {
      val maxTtl = 3600L
      val requestedTtl = 99999L
      val config =
        IdempotencyConfig(defaultTtlSeconds = 86400, maxTtlSeconds = maxTtl)

      val test =
        for
          warnLogs <- Ref[IO].of(List.empty[String])
          logger = capturingLogger(warnLogs)
          store = new IdempotencyStore[IO]:
            override def check(
                idempotencyKey: String,
                clientId: String,
                ttlSeconds: Long,
                requestHash: Option[String] = None,
            ): IO[IdempotencyResult] = Clock[IO].realTime.map(d =>
              IdempotencyResult
                .New(idempotencyKey, Instant.ofEpochMilli(d.toMillis), "claim-1"),
            )
            override def storeResponse(
                idempotencyKey: String,
                clientId: String,
                response: StoredResponse,
                claimId: Option[String],
            ): IO[Boolean] = IO.pure(false)
            override def markFailed(
                idempotencyKey: String,
                clientId: String,
                claimId: Option[String],
            ): IO[Boolean] = IO.pure(false)
            override def get(
                idempotencyKey: String,
            ): IO[Option[IdempotencyRecord]] = IO.pure(None)
            override def healthCheck: IO[Either[String, Unit]] = IO.pure(Right(()))

          api = IdempotencyApi[IO](
            store,
            config,
            EventPublisher.noop[IO],
            MetricsPublisher.noop[IO],
            logger,
            () => IO.pure("test-request-id"),
          )
          body = s"""{"idempotencyKey": "warn-test", "ttl": $requestedTtl}"""
          req = Request[IO](Method.POST, uri"/v1/idempotency/check")
            .withEntity(body).putHeaders(
              headers.`Content-Type`(MediaType.application.json),
              headers.Authorization(Credentials.Token(ci"Bearer", "test-key")),
            )
          _ <- api.check(req, testClient)
          logs <- warnLogs.get
        yield logs

      test.asserting { logs =>
        logs should have size 1
        logs.head should include("Idempotency TTL capped")
        logs.head should include("requested=99999")
        logs.head should include("max=3600")
        logs.head should include("warn-test")
      }
    }

    "IdempotencyApi — store failures" - {

      def failingStore(ex: Throwable): IdempotencyStore[IO] =
        new IdempotencyStore[IO]:
          override def check(
              idempotencyKey: String,
              clientId: String,
              ttlSeconds: Long,
              requestHash: Option[String],
          ): IO[IdempotencyResult] = IO.raiseError(ex)
          override def storeResponse(
              idempotencyKey: String,
              clientId: String,
              response: StoredResponse,
              claimId: Option[String],
          ): IO[Boolean] = IO.raiseError(ex)
          override def markFailed(
              idempotencyKey: String,
              clientId: String,
              claimId: Option[String],
          ): IO[Boolean] = IO.raiseError(ex)
          override def get(
              idempotencyKey: String,
          ): IO[Option[IdempotencyRecord]] = IO.pure(None)
          override def healthCheck: IO[Either[String, Unit]] = IO.pure(Right(()))

      def apiWith(store: IdempotencyStore[IO]): IdempotencyApi[IO] =
        IdempotencyApi[IO](
          store,
          IdempotencyConfig(defaultTtlSeconds = 3600, maxTtlSeconds = 86400),
          EventPublisher.noop[IO],
          MetricsPublisher.noop[IO],
          org.typelevel.log4cats.noop.NoOpLogger[IO],
          () => IO.pure("test-request-id"),
        )

      def checkRequest(body: String): Request[IO] =
        Request[IO](Method.POST, uri"/v1/idempotency/check").withEntity(body)
          .putHeaders(headers.`Content-Type`(MediaType.application.json))

      "maps an unexpected store failure to a structured 503, not a bare 500" in {
        val api = apiWith(failingStore(new RuntimeException("pool exhausted")))
        for
          resp <- api
            .check(checkRequest("""{"idempotencyKey": "k-1"}"""), testClient)
          json <- resp.as[String].map(parse(_).toOption)
        yield
          resp.status shouldBe Status.ServiceUnavailable
          json
            .flatMap(_.hcursor.downField("error").as[String].toOption) shouldBe
            Some("storage_unavailable")
      }

      "maps a store failure on fail the same way" in {
        val api = apiWith(failingStore(new RuntimeException("pool exhausted")))
        api.fail("k-1", testClient)
          .asserting(_.status shouldBe Status.ServiceUnavailable)
      }

      "maps a store failure on complete the same way" in {
        val api = apiWith(failingStore(new RuntimeException("pool exhausted")))
        val req = Request[IO](Method.POST, uri"/v1/idempotency/k-1/complete")
          .withEntity("""{"statusCode": 200, "body": "{}"}""")
          .putHeaders(headers.`Content-Type`(MediaType.application.json))
        api.complete("k-1", req, testClient)
          .asserting(_.status shouldBe Status.ServiceUnavailable)
      }

      "counts each check by result, including a store failure as error" in {
        given Logger[IO] = org.typelevel.log4cats.noop.NoOpLogger[IO]
        def countOf(prom: observability.PrometheusMetrics[IO], result: String) =
          prom.registry.getSampleValue(
            "gate_idempotency_total",
            Array("result"),
            Array(result),
          ).doubleValue
        def api(
            store: IdempotencyStore[IO],
            prom: observability.PrometheusMetrics[IO],
        ) = IdempotencyApi[IO](
          store,
          IdempotencyConfig(defaultTtlSeconds = 3600, maxTtlSeconds = 86400),
          EventPublisher.noop[IO],
          observability.PrometheusMetrics.dual(MetricsPublisher.noop[IO], prom),
          org.typelevel.log4cats.noop.NoOpLogger[IO],
          () => IO.pure("test-request-id"),
        )
        val body = """{"idempotencyKey": "k-count"}"""
        for
          prom <- observability.PrometheusMetrics[IO]
          healthy <- IdempotencyStore.inMemory[IO]
          _ <- api(healthy, prom).check(checkRequest(body), testClient)
          _ <- api(healthy, prom).check(checkRequest(body), testClient)
          broken = failingStore(new RuntimeException("pool exhausted"))
          _ <- api(broken, prom).check(checkRequest(body), testClient)
        yield List("new", "in_progress", "error").map(countOf(prom, _)) shouldBe
          List(1.0, 1.0, 1.0)
      }

      "lets a malformed body propagate so http4s can answer 4xx" in {
        val api = apiWith(failingStore(new RuntimeException("unused")))
        api.check(checkRequest("not json"), testClient).attempt.asserting {
          case Left(_: MessageFailure) => succeed
          case other => fail(s"expected a MessageFailure, got $other")
        }
      }
    }
  }

  "IdempotencyApi — fail" - {

    val other = testClient.copy(apiKeyId = "other-client", clientId = "other")

    def apiWith(store: IdempotencyStore[IO]): IdempotencyApi[IO] =
      IdempotencyApi[IO](
        store,
        IdempotencyConfig(defaultTtlSeconds = 3600, maxTtlSeconds = 86400),
        EventPublisher.noop[IO],
        MetricsPublisher.noop[IO],
        org.typelevel.log4cats.noop.NoOpLogger[IO],
        () => IO.pure("test-request-id"),
      )

    def check(
        api: IdempotencyApi[IO],
        client: AuthenticatedClient,
        key: String,
    ) = api.check(
      Request[IO](Method.POST, uri"/v1/idempotency/check")
        .withEntity(s"""{"idempotencyKey": "$key"}""")
        .putHeaders(headers.`Content-Type`(MediaType.application.json)),
      client,
    ).flatMap(_.as[String])
      .map(body => parse(body).flatMap(_.hcursor.get[String]("status")).toOption)

    def complete(api: IdempotencyApi[IO], key: String, body: String) = api
      .complete(
        key,
        Request[IO](Method.POST, uri"/v1/idempotency/k/complete").withEntity(
          io.circe.Json.obj(
            "statusCode" -> io.circe.Json.fromInt(200),
            "body" -> io.circe.Json.fromString(body),
          ).noSpaces,
        ).putHeaders(headers.`Content-Type`(MediaType.application.json)),
        testClient,
      )

    "releases a pending key, and the next check claims it again" in {
      for
        api <- IdempotencyStore.inMemory[IO].map(apiWith)
        first <- check(api, testClient, "fail-1")
        failed <- api.fail("fail-1", testClient)
        retried <- check(api, testClient, "fail-1")
      yield
        first shouldBe Some("new")
        failed.status shouldBe Status.Ok
        retried shouldBe Some("new")
    }

    "refuses a key another client claimed, which stays in progress" in {
      for
        api <- IdempotencyStore.inMemory[IO].map(apiWith)
        _ <- check(api, testClient, "fail-2")
        forged <- api.fail("fail-2", other)
        after <- check(api, testClient, "fail-2")
      yield
        forged.status shouldBe Status.Conflict
        after shouldBe Some("in_progress")
    }

    "never reopens a completed key" in {
      for
        api <- IdempotencyStore.inMemory[IO].map(apiWith)
        _ <- check(api, testClient, "fail-3")
        _ <- complete(api, "fail-3", "done")
        refused <- api.fail("fail-3", testClient)
        after <- check(api, testClient, "fail-3")
      yield
        refused.status shouldBe Status.Conflict
        after shouldBe Some("duplicate")
    }
  }

  "IdempotencyApi — stored response cap" - {

    def apiWith(store: IdempotencyStore[IO]): IdempotencyApi[IO] =
      IdempotencyApi[IO](
        store,
        IdempotencyConfig(defaultTtlSeconds = 3600, maxTtlSeconds = 86400),
        EventPublisher.noop[IO],
        MetricsPublisher.noop[IO],
        org.typelevel.log4cats.noop.NoOpLogger[IO],
        () => IO.pure("test-request-id"),
      )

    def completeWith(api: IdempotencyApi[IO], key: String, body: String) = api
      .complete(
        key,
        Request[IO](Method.POST, uri"/v1/idempotency/k/complete").withEntity(
          io.circe.Json.obj(
            "statusCode" -> io.circe.Json.fromInt(200),
            "body" -> io.circe.Json.fromString(body),
          ).noSpaces,
        ).putHeaders(headers.`Content-Type`(MediaType.application.json)),
        testClient,
      )

    def claim(store: IdempotencyStore[IO], key: String) = store
      .check(TenantKey(testClient.clientId, key), testClient.clientId, 3600)

    "the size is the encoded JSON, so escaping counts" in IO {
      val at = Instant.EPOCH
      val plain = StoredResponse(200, "a" * 1000, Map.empty, at)
      val quoted = StoredResponse(200, "\"" * 1000, Map.empty, at)
      IdempotencyApi.storedSize(quoted) - IdempotencyApi.storedSize(
        plain,
      ) shouldBe 1000
    }

    "refuses a response over the cap with 413 and leaves the key pending" in {
      // 200,000 quotes: under the cap as characters, twice it once escaped.
      val body = "\"" * 200000
      for
        store <- IdempotencyStore.inMemory[IO]
        _ <- claim(store, "big")
        resp <- completeWith(apiWith(store), "big", body)
        error <- resp.as[String]
          .map(parse(_).flatMap(_.hcursor.get[String]("error")).toOption)
        record <- store.get(TenantKey(testClient.clientId, "big"))
      yield
        resp.status shouldBe Status.PayloadTooLarge
        error shouldBe Some("response_too_large")
        record.map(_.status) shouldBe Some(IdempotencyStatus.Pending)
    }

    "stores a response under the cap" in {
      val body = "a" * (IdempotencyApi.MaxStoredResponseBytes - 1024)
      for
        store <- IdempotencyStore.inMemory[IO]
        _ <- claim(store, "fits")
        resp <- completeWith(apiWith(store), "fits", body)
        record <- store.get(TenantKey(testClient.clientId, "fits"))
      yield
        resp.status shouldBe Status.Ok
        record.flatMap(_.response).map(_.body) shouldBe Some(body)
    }
  }
