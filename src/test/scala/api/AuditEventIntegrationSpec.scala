package api

import java.time.Instant
import java.util.concurrent.atomic.AtomicReference

import scala.concurrent.duration.*

import org.http4s.*
import org.http4s.circe.CirceEntityDecoder.*
import org.http4s.circe.CirceEntityEncoder.*
import org.http4s.implicits.*
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.noop.NoOpLogger
import org.typelevel.otel4s.trace.Tracer

import cats.effect.*
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all.*
import io.circe.generic.auto.*
import io.circe.syntax.*
import config.*
import core.*
import events.*
import observability.MetricsPublisher
import security.*

class AuditEventIntegrationSpec
    extends AsyncFreeSpec with AsyncIOSpec with Matchers:

  given Logger[IO] = NoOpLogger[IO]
  given Tracer[IO] = Tracer.noop[IO]

  "RateLimitApi" - {

    "emit an AuditEvent when a request is rejected (429)" in {
      val captured = new AtomicReference[List[RateLimitEvent]](Nil)

      val capturingPublisher = new EventPublisher[IO]:
        def publish(event: RateLimitEvent): IO[Unit] = IO.delay {
          captured.updateAndGet(event :: _)
          ()
        }
        def publishBatch(events: List[RateLimitEvent]): IO[Unit] = events
          .traverse_(publish)
        def healthCheck: IO[Either[String, Unit]] = IO.pure(Right(()))

      val config = RateLimitConfig(
        defaultCapacity = 1,
        defaultRefillRatePerSecond = 0.001, // effectively no refill during test duration
        defaultTtlSeconds = 3600,
      )

      for
        store <- RateLimitStore.inMemory[IO]
        api = RateLimitApi[IO](
          store,
          capturingPublisher,
          MetricsPublisher.noop[IO],
          config,
          summon[Logger[IO]],
          () => IO.pure("test-request-id"),
        )
        client = AuthenticatedClient(
          apiKeyId = "test-key",
          clientId = "test-client",
          clientName = "Test Client",
          tier = ClientTier.Free,
          permissions = Set.empty,
        )

        // First request drains the single token
        body1 = RateLimitCheckRequest(key = "test-key", cost = 1)
        req1 = Request[IO](Method.POST, uri"/v1/ratelimit/check")
          .withEntity(body1.asJson)
        resp1 <- api.check(req1, client)
        _ = resp1.status shouldBe Status.Ok

        // Allow fire-and-forget fibers to complete
        _ <- IO.sleep(100.millis)

        // Second request should be rejected
        body2 = RateLimitCheckRequest(key = "test-key", cost = 1)
        req2 = Request[IO](Method.POST, uri"/v1/ratelimit/check")
          .withEntity(body2.asJson)
        resp2 <- api.check(req2, client)
        _ = resp2.status shouldBe Status.TooManyRequests

        // Allow fire-and-forget fibers to complete
        _ <- IO.sleep(200.millis)

        events = captured.get()
        auditEvents = events.collect { case e: RateLimitEvent.AuditEvent => e }
      yield
        auditEvents should have size 1
        auditEvents.head.decision shouldBe "rejected"
        auditEvents.head.clientId shouldBe "test-client"
        auditEvents.head.reason shouldBe "Rate limit exceeded"
    }
  }
