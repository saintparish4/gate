package api

import scala.concurrent.duration.DurationInt

import org.http4s.*
import org.http4s.circe.*
import org.http4s.circe.CirceEntityDecoder.*
import org.http4s.circe.CirceEntityEncoder.*
import org.http4s.implicits.*
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.typelevel.ci.*
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.noop.NoOpLogger
import org.typelevel.otel4s.trace.Tracer

import cats.effect.*
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all.*
import io.circe.generic.auto.*
import io.circe.syntax.*
import config.{RateLimitConfig, RateLimitProfileConfig}
import core.*
import events.*
import observability.MetricsPublisher
import security.*
import storage.InMemoryRateLimitStore
import testutil.*

class RateLimitApiSpec extends AsyncFreeSpec with AsyncIOSpec with Matchers:

  given Logger[IO] = NoOpLogger[IO]
  given Tracer[IO] = Tracer.noop[IO]

  val configWithProfiles: RateLimitConfig = RateLimitConfig(
    defaultCapacity = 50,
    defaultRefillRatePerSecond = 5.0,
    defaultTtlSeconds = 3600,
    algorithm = "token-bucket",
    profiles = Map(
      "free" -> RateLimitProfileConfig(20, 2.0, 3600),
      "custom" -> RateLimitProfileConfig(500, 50.0, 7200),
    ),
  )

  /** Build an in-memory-backed RateLimitApi. */
  def makeApi(
      config: RateLimitConfig = configWithProfiles,
      events: EventPublisher[IO] = EventPublisher.noop[IO],
      metrics: MetricsPublisher[IO] = MetricsPublisher.noop[IO],
  ): IO[RateLimitApi[IO]] = InMemoryRateLimitStore.create[IO].map(store =>
    RateLimitApi[IO](
      store,
      events,
      metrics,
      config,
      Logger[IO],
      () => IO.pure("test-request-id"),
    ),
  )

  def postCheckRequest(
      key: String,
      cost: Int,
      profile: Option[String] = None,
  ): Request[IO] =
    Request[IO](method = Method.POST, uri = uri"/v1/ratelimit/check").withEntity(
      RateLimitCheckRequest(key = key, cost = cost, profile = profile).asJson,
    )

  "RateLimitApi" - {

    "rejects cost=0 with 400 BadRequest" in makeApi()
      .flatMap(api => api.check(postCheckRequest("k1", 0), testClient))
      .asserting((r: Response[IO]) => r.status.shouldBe(Status.BadRequest))

    "rejects negative cost with 400 BadRequest" in makeApi()
      .flatMap(api => api.check(postCheckRequest("k1", -5), testClient))
      .asserting((r: Response[IO]) => r.status.shouldBe(Status.BadRequest))

    "resolves profile by explicit name in request body" in
      // "custom" profile has capacity=500; default is 50.
      // A cost of 100 should succeed with the custom profile but fail with default.
      makeApi().flatMap(api =>
        for
          // drain default-profile bucket (capacity=50)
          _ <- (1 to 50).toList
            .traverse_(_ => api.check(postCheckRequest("k2", 1), testClient))
          // explicit custom profile — separate logical bucket, 500 capacity
          r <- api.check(
            postCheckRequest("k2-custom", 100, profile = Some("custom")),
            testClient,
          )
        yield r,
      ).asserting((r: Response[IO]) => r.status.shouldBe(Status.Ok))

    "resolves profile by tier name from config when no explicit profile given" in {
      // Free-tier client: config.profiles("free") = capacity 20
      // An AdminTier client falls back to defaults (capacity 50).
      // Drain the free-tier bucket to 0, then verify next call is rejected.
      val freeClient = testClient.copy(tier = ClientTier.Free)
      makeApi().flatMap(api =>
        for
          _ <- (1 to 20).toList.traverse_(_ =>
            api.check(postCheckRequest("tier-key", 1), freeClient),
          )
          r <- api.check(postCheckRequest("tier-key", 1), freeClient)
        yield r,
      ).asserting((r: Response[IO]) => r.status.shouldBe(Status.TooManyRequests))
    }

    "falls back to config defaults when tier has no named profile" in {
      // Premium tier has no named profile in configWithProfiles; uses defaults (50 cap).
      val premiumClient = testClient.copy(tier = ClientTier.Premium)
      makeApi().flatMap(api =>
        api.check(postCheckRequest("admin-key", 1), premiumClient),
      ).asserting((r: Response[IO]) => r.status.shouldBe(Status.Ok))
    }

    "Allowed response carries X-RateLimit-* headers" in makeApi()
      .flatMap(api => api.check(postCheckRequest("hdr-key", 1), testClient))
      .asserting { (response: Response[IO]) =>
        response.status.shouldBe(Status.Ok)
        response.headers.get(ci"X-RateLimit-Limit").shouldBe(defined)
        response.headers.get(ci"X-RateLimit-Remaining").shouldBe(defined)
        response.headers.get(ci"X-RateLimit-Reset").shouldBe(defined)
      }

    "Rejected response carries Retry-After header" in makeApi().flatMap(api =>
      for
        // exhaust the bucket
        _ <- (1 to 50).toList
          .traverse_(_ => api.check(postCheckRequest("rjt-key", 1), testClient))
        r <- api.check(postCheckRequest("rjt-key", 1), testClient)
      yield r,
    ).asserting { (response: Response[IO]) =>
      response.status.shouldBe(Status.TooManyRequests)
      response.headers.get(ci"Retry-After").shouldBe(defined)
    }

    "audit event is published asynchronously on rejection" in
      {
        for
          eventsRef <- Ref.of[IO, List[RateLimitEvent]](Nil)
          publisher = new EventPublisher[IO]:
            def publish(e: RateLimitEvent): IO[Unit] = eventsRef.update(_ :+ e)
            def publishBatch(es: List[RateLimitEvent]): IO[Unit] = eventsRef
              .update(_ ++ es)
            def healthCheck: IO[Either[String, Unit]] = IO.pure(Right(()))
          api <- makeApi(events = publisher)
          // Exhaust tokens
          _ <- (1 to 50).toList.traverse_(_ =>
            api.check(postCheckRequest("audit-key", 1), testClient),
          )
          _ <- api.check(postCheckRequest("audit-key", 1), testClient)
          // The publish is fire-and-forget (`.start`). Give the fiber time to run.
          _ <- IO.sleep(50.millis)
          events <- eventsRef.get
        yield events.exists {
          case _: RateLimitEvent.AuditEvent => true
          case _ => false
        }
      }.asserting(_ shouldBe true)
  }
