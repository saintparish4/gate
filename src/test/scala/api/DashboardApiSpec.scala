package api

import java.time.Instant

import scala.concurrent.duration.*

import org.http4s.*
import org.http4s.implicits.*
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.typelevel.log4cats.noop.NoOpLogger

import cats.effect.*
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.testkit.TestControl
import io.circe.parser.parse
import config.RateLimitConfig
import core.*
import events.EventPublisher

/** `GET /dashboard/api/status` reports the demo bucket's reset.
  *
  * `resetAt` was always the empty string. It is now the store's own value, as
  * on `/v1/ratelimit/status`, and the current time for a bucket not yet
  * created.
  */
class DashboardApiSpec extends AsyncFreeSpec with AsyncIOSpec with Matchers:

  // 10 tokens, 1 per second: the demo profile is built from the defaults.
  private val config = RateLimitConfig(
    defaultCapacity = 10,
    defaultRefillRatePerSecond = 1.0,
    defaultTtlSeconds = 3600,
  )
  private val profile = RateLimitProfile(10, 1.0, 3600)

  private def statusResetAt(app: HttpApp[IO]): IO[String] =
    for
      response <- app.run(Request[IO](Method.GET, uri"/dashboard/api/status"))
      body <- response.bodyText.compile.string
      resetAt <- IO
        .fromEither(parse(body).flatMap(_.hcursor.get[String]("resetAt")))
    yield resetAt

  "GET /dashboard/api/status resetAt" - {

    "is the current time for a bucket not yet created, then the store's own value" in
      TestControl.executeEmbed {
        for
          store <- RateLimitStore.inMemory[IO]
          dashboard <- DashboardApi[IO](
            store,
            config,
            NoOpLogger[IO],
            None,
            EventPublisher.noop[IO],
          )
          app = dashboard.routes.orNotFound
          _ <- IO.sleep(5.seconds)
          untouched <- statusResetAt(app)
          _ <- app.run(Request[IO](Method.POST, uri"/dashboard/api/check"))
          _ <- IO.sleep(200.millis)
          own <- store.getStatus("dashboard-demo", profile)
          afterCheck <- statusResetAt(app)
        yield (untouched, own.map(_.resetAt.toString), afterCheck)
      }.asserting { case (untouched, own, afterCheck) =>
        untouched shouldBe Instant.ofEpochMilli(5000).toString
        own shouldBe defined
        Some(afterCheck) shouldBe own
        afterCheck should not be untouched
      }
  }
