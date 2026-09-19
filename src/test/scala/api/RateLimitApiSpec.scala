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
      "tiny" -> RateLimitProfileConfig(5, 0.5, 3600),
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

  "RateLimitApi.selectProfile" - {

    // The ladder from application.conf, plus two profiles that each beat
    // `basic` on one dimension only.
    val ladder = RateLimitConfig(
      defaultCapacity = 100,
      defaultRefillRatePerSecond = 10.0,
      defaultTtlSeconds = 3600,
      profiles = Map(
        "free" -> RateLimitProfileConfig(20, 2.0, 3600),
        "basic" -> RateLimitProfileConfig(100, 10.0, 3600),
        "premium" -> RateLimitProfileConfig(1000, 100.0, 3600),
        "enterprise" -> RateLimitProfileConfig(10000, 1000.0, 3600),
        "burst" -> RateLimitProfileConfig(200, 1.0, 3600),
        "fast" -> RateLimitProfileConfig(10, 50.0, 3600),
      ),
    )
    val tiers = List(
      ClientTier.Free,
      ClientTier.Basic,
      ClientTier.Premium,
      ClientTier.Enterprise,
    )
    def capacityOf(name: String): Int = ladder.profiles(name).capacity

    "every tier may name its own profile or one below it, never one above" in IO {
      for
        (tier, tierRank) <- tiers.zipWithIndex
        (asked, askedRank) <- tiers.zipWithIndex
      do
        val name = asked.toString.toLowerCase
        val result = RateLimitApi.selectProfile(ladder, tier, Some(name))
        if askedRank <= tierRank then
          result.map(_.capacity) shouldBe Right(capacityOf(name))
        else result shouldBe Left(ProfileRefusal.AboveTier(name, tier))
    }.asserting(_ => succeed)

    "no named profile means the tier's own" in IO(tiers.foreach(tier =>
      RateLimitApi.selectProfile(ladder, tier, None).map(_.capacity) shouldBe
        Right(capacityOf(tier.toString.toLowerCase)),
    )).asserting(_ => succeed)

    "beating the tier on either dimension alone is above it" in IO {
      RateLimitApi
        .selectProfile(ladder, ClientTier.Basic, Some("burst")) shouldBe
        Left(ProfileRefusal.AboveTier("burst", ClientTier.Basic))
      RateLimitApi
        .selectProfile(ladder, ClientTier.Basic, Some("fast")) shouldBe
        Left(ProfileRefusal.AboveTier("fast", ClientTier.Basic))
    }.asserting(_ => succeed)

    "an unknown name is refused as unknown" in
      IO(RateLimitApi.selectProfile(ladder, ClientTier.Enterprise, Some("gold")))
        .asserting(_ shouldBe Left(ProfileRefusal.Unknown("gold")))
  }

  "RateLimitApi" - {

    "rejects cost=0 with 400 BadRequest" in makeApi()
      .flatMap(api => api.check(postCheckRequest("k1", 0), testClient))
      .asserting((r: Response[IO]) => r.status.shouldBe(Status.BadRequest))

    "rejects negative cost with 400 BadRequest" in makeApi()
      .flatMap(api => api.check(postCheckRequest("k1", -5), testClient))
      .asserting((r: Response[IO]) => r.status.shouldBe(Status.BadRequest))

    "a named profile narrower than the tier is used" in makeApi().flatMap(api =>
      api.check(
        postCheckRequest("k2-tiny", 1, profile = Some("tiny")),
        testClient,
      ).flatMap(r => r.as[RateLimitCheckResponse].map(b => (r.status, b.limit))),
    ).asserting(_ shouldBe (Status.Ok, 5))

    "a named profile above the tier is 403 and consumes nothing" in {
      // The bypass: a free key naming a bigger profile used to get it.
      val freeClient = testClient.copy(tier = ClientTier.Free)
      makeApi().flatMap(api =>
        for
          r <- api.check(
            postCheckRequest("k2-custom", 1, profile = Some("custom")),
            freeClient,
          )
          body <- r.as[io.circe.Json]
          status <- api.status("k2-custom", freeClient)
          remaining <- status.as[RateLimitStatusResponse].map(_.tokensRemaining)
        yield (r.status, body.hcursor.get[String]("error").toOption, remaining),
      ).asserting(
        _ shouldBe (Status.Forbidden, Some("profile_not_permitted"), 20),
      )
    }

    "an unknown profile name is 400, not a silent fallback to the tier" in
      makeApi().flatMap(api =>
        api.check(postCheckRequest("k2", 1, profile = Some("nope")), testClient),
      ).asserting((r: Response[IO]) => r.status.shouldBe(Status.BadRequest))

    "resolves profile by tier name from config when no explicit profile given" in {
      // Free-tier client: config.profiles("free") = capacity 20
      // An AdminTier client falls back to defaults (capacity 50).
      // Drain the free-tier bucket to 0, then verify next call is rejected.
      // A near-zero refill: at 2/s, a loaded host took long enough over the 20
      // checks to refill one and the 21st was allowed.
      val freeClient = testClient.copy(tier = ClientTier.Free)
      val slowRefill = configWithProfiles.copy(profiles =
        configWithProfiles.profiles
          .updated("free", RateLimitProfileConfig(20, 0.001, 3600)),
      )
      makeApi(config = slowRefill).flatMap(api =>
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
