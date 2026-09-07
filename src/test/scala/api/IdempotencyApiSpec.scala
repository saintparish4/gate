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
                IdempotencyResult
                  .New(idempotencyKey, Instant.ofEpochMilli(d.toMillis)),
              )
            override def storeResponse(
                idempotencyKey: String,
                response: StoredResponse,
            ): IO[Boolean] = IO.pure(false)
            override def markFailed(idempotencyKey: String): IO[Boolean] = IO
              .pure(false)
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
                .New(idempotencyKey, Instant.ofEpochMilli(d.toMillis)),
            )
            override def storeResponse(
                idempotencyKey: String,
                response: StoredResponse,
            ): IO[Boolean] = IO.pure(false)
            override def markFailed(idempotencyKey: String): IO[Boolean] = IO
              .pure(false)
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
  }
