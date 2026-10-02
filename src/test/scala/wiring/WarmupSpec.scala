package wiring

import scala.concurrent.duration.*

import org.http4s.*
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

import cats.effect.*
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.testkit.TestControl
import cats.syntax.all.*
import config.{IdempotencyConfig, RateLimitConfig, TokenQuotaConfig}
import core.*
import security.AuthenticatedClient

/** The warm-up runs every request path before the server binds, and can never
  * hold the port closed or fail start-up.
  *
  * It used to be five calls on the rate-limit store. A task that took load
  * seconds after starting then met the idempotency and quota paths cold, and
  * missed the 2-second store timeout on them.
  */
class WarmupSpec extends AsyncFreeSpec with AsyncIOSpec with Matchers:

  private val rateLimitConfig = RateLimitConfig(
    defaultCapacity = 10,
    defaultRefillRatePerSecond = 1.0,
    defaultTtlSeconds = 3600,
  )
  private val idempotencyConfig = IdempotencyConfig()
  private val quotaConfig = TokenQuotaConfig(enabled = true)

  /** An app that records each request's method and path, with the path's
    * variable segment replaced, and answers what `respond` says.
    */
  private def recordingApp(
      seen: Ref[IO, List[String]],
      respond: Request[IO] => IO[Response[IO]] = _ => IO.pure(Response[IO]()),
  ): HttpApp[IO] = HttpApp[IO](request =>
    seen.update(_ :+ s"${request.method} ${request.uri.path.renderString
        .replaceAll("warmup-[0-9]+", "*")}") *> respond(request),
  )

  private def reserving(request: Request[IO]): IO[Response[IO]] = IO.pure(
    if request.uri.path.renderString == "/v1/quota/check" then
      Response[IO]().withEntity("""{"reservationId":"r-1"}""")
    else Response[IO](),
  )

  private val everyPath = List(
    "POST /v1/ratelimit/check",
    "GET /v1/ratelimit/status/*",
    "POST /v1/idempotency/check",
    "POST /v1/idempotency/check",
    "POST /v1/idempotency/*/complete",
    "POST /v1/idempotency/check",
    "POST /v1/quota/check",
    "POST /v1/quota/reconcile",
  )

  "Warmup.requestPaths" - {

    "runs every path once per round, in order" in
      Ref.of[IO, List[String]](Nil).flatMap(seen =>
        Warmup.requestPaths(
          recordingApp(seen, reserving),
          quota = true,
          Warmup.Settings(rounds = 1, parallelism = 1),
        ).flatMap(report => seen.get.map((report, _))),
      ).asserting { case (report, seen) =>
        seen shouldBe everyPath
        (report.completed, report.failed) shouldBe (1, 0)
      }

    "leaves the quota paths out when quotas are off" in
      Ref.of[IO, List[String]](Nil).flatMap(seen =>
        Warmup.requestPaths(
          recordingApp(seen),
          quota = false,
          Warmup.Settings(rounds = 3, parallelism = 2),
        ) *> seen.get,
      ).asserting { seen =>
        seen should have size 18
        seen.exists(_.contains("quota")) shouldBe false
      }

    "runs the requested number of rounds" in
      Ref.of[IO, List[String]](Nil).flatMap(seen =>
        Warmup.requestPaths(
          recordingApp(seen, reserving),
          quota = true,
          Warmup.Settings(rounds = 25, parallelism = 4),
        ).flatMap(report => seen.get.map(s => (report.completed, s.size))),
      ).asserting(_ shouldBe (25, 25 * everyPath.size))

    "never fails start-up when every request raises" in Warmup.requestPaths(
      HttpApp[IO](_ => IO.raiseError(new RuntimeException("cold"))),
      quota = true,
      Warmup.Settings(rounds = 4, parallelism = 2),
    ).asserting(report => (report.completed, report.failed) shouldBe (0, 4))

    "stops at its budget when the store hangs, so the port still opens" in
      TestControl.executeEmbed(Warmup.requestPaths(
        HttpApp[IO](_ => IO.never),
        quota = true,
        Warmup.Settings(rounds = 100, parallelism = 8, budget = 20.seconds),
      )).asserting { report =>
        report.completed shouldBe 0
        report.elapsed shouldBe 20.seconds
      }
  }

  "Warmup.run" - {

    def stores = (
      RateLimitStore.inMemory[IO],
      IdempotencyStore.inMemory[IO],
      TokenQuotaStore.inMemory[IO],
    ).tupled

    "drives the real routes over the stores with no failed round" in
      Ref.of[IO, List[String]](Nil).flatMap(logged =>
        stores.flatMap { case (rateLimit, idempotency, quota) =>
          Warmup.run[IO](
            rateLimit,
            idempotency,
            Some(quota),
            rateLimitConfig,
            idempotencyConfig,
            quotaConfig,
            infoLogger(logged),
            Warmup.Settings(rounds = 40, parallelism = 4),
          ).flatMap(report => logged.get.map((report, _)))
        },
      ).asserting { case (report, logged) =>
        (report.completed, report.failed) shouldBe (40, 0)
        logged.exists(_.contains("40 of 40 rounds")) shouldBe true
      }

    "completes each key it claims, so the claim ID round-trips" in
      stores.flatMap { case (rateLimit, idempotency, quota) =>
        // Every record the warm-up wrote, whatever its random client ID.
        val spy = new SpyIdempotencyStore(idempotency)
        Warmup.run[IO](
          rateLimit,
          spy,
          Some(quota),
          rateLimitConfig,
          idempotencyConfig,
          quotaConfig,
          infoLogger(Ref.unsafe(Nil)),
          Warmup.Settings(rounds = 6, parallelism = 2),
        ) *> spy.outcomes
      }.asserting { outcomes =>
        outcomes.count(_ == "claimed") shouldBe 6
        outcomes.count(_ == "completed with its claim ID") shouldBe 6
      }

    "leaves a real client's state untouched" in
      stores.flatMap { case (rateLimit, idempotency, quota) =>
        val client = AuthenticatedClient(
          "real",
          "real",
          "Real",
          security.ClientTier.Free,
          security.Permission.standard,
        )
        val profile = RateLimitProfile(10, 1.0, 3600)
        Warmup.run[IO](
          rateLimit,
          idempotency,
          Some(quota),
          rateLimitConfig,
          idempotencyConfig,
          quotaConfig,
          infoLogger(Ref.unsafe(Nil)),
          Warmup.Settings(rounds = 20, parallelism = 4),
        ) *>
          (
            rateLimit.getStatus(TenantKey(client.clientId, "warmup-0"), profile),
            idempotency.get(TenantKey(client.clientId, "warmup-0")),
          ).tupled
      }.asserting(_ shouldBe (None, None))
  }

  private def infoLogger(
      ref: Ref[IO, List[String]],
  ): org.typelevel.log4cats.Logger[IO] = new org.typelevel.log4cats.Logger[IO]:
    def error(t: Throwable)(msg: => String): IO[Unit] = IO.unit
    def error(msg: => String): IO[Unit] = IO.unit
    def warn(t: Throwable)(msg: => String): IO[Unit] = IO.unit
    def warn(msg: => String): IO[Unit] = IO.unit
    def info(t: Throwable)(msg: => String): IO[Unit] = ref.update(_ :+ msg)
    def info(msg: => String): IO[Unit] = ref.update(_ :+ msg)
    def debug(t: Throwable)(msg: => String): IO[Unit] = IO.unit
    def debug(msg: => String): IO[Unit] = IO.unit
    def trace(t: Throwable)(msg: => String): IO[Unit] = IO.unit
    def trace(msg: => String): IO[Unit] = IO.unit

  private class SpyIdempotencyStore(underlying: IdempotencyStore[IO])
      extends IdempotencyStore[IO]:
    private val seen = Ref.unsafe[IO, List[String]](Nil)
    private val claims = Ref.unsafe[IO, Map[String, String]](Map.empty)
    def outcomes: IO[List[String]] = seen.get

    override def check(
        idempotencyKey: String,
        clientId: String,
        ttlSeconds: Long,
        requestHash: Option[String],
    ): IO[IdempotencyResult] = underlying
      .check(idempotencyKey, clientId, ttlSeconds, requestHash).flatTap {
        case IdempotencyResult.New(key, _, claimId) => claims
            .update(_ + (key -> claimId)) *> seen.update(_ :+ "claimed")
        case _ => IO.unit
      }
    override def storeResponse(
        idempotencyKey: String,
        clientId: String,
        response: StoredResponse,
        claimId: Option[String],
    ): IO[Boolean] = underlying
      .storeResponse(idempotencyKey, clientId, response, claimId).flatTap(
        stored =>
          claims.get.flatMap(issued =>
            seen.update(
              _ :+
                (if stored && claimId.isDefined &&
                   claimId == issued.get(idempotencyKey)
                 then "completed with its claim ID"
                 else "not completed"),
            ),
          ),
      )
    override def markFailed(
        idempotencyKey: String,
        clientId: String,
        claimId: Option[String],
    ): IO[Boolean] = underlying.markFailed(idempotencyKey, clientId, claimId)
    override def get(idempotencyKey: String): IO[Option[IdempotencyRecord]] =
      underlying.get(idempotencyKey)
    override def healthCheck: IO[Either[String, Unit]] = underlying.healthCheck
