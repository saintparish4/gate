package wiring

import java.time.Instant

import scala.concurrent.duration.*

import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.noop.NoOpLogger

import cats.effect.*
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.testkit.TestControl
import core.{RateLimitDecision, RateLimitProfile, RateLimitStore}

class WarmupSpec extends AsyncFreeSpec with AsyncIOSpec with Matchers:

  given Logger[IO] = NoOpLogger[IO]

  val profile: RateLimitProfile = RateLimitProfile(100, 10.0, 3600)

  def storeWith(check: IO[RateLimitDecision]): RateLimitStore[IO] =
    new RateLimitStore[IO]:
      def checkAndConsume(
          k: String,
          c: Int,
          p: RateLimitProfile,
      ): IO[RateLimitDecision] = check
      def getStatus(
          k: String,
          p: RateLimitProfile,
      ): IO[Option[RateLimitDecision.Allowed]] = IO.pure(None)
      def healthCheck: IO[Either[String, Unit]] = IO.pure(Right(()))

  "Warmup.rateLimitPath" - {

    "exercises the store the requested number of times" in
      Ref.of[IO, Int](0).flatMap { calls =>
        val store = storeWith(
          calls.update(_ + 1).as(RateLimitDecision.Allowed(99, Instant.EPOCH)),
        )
        Warmup.rateLimitPath[IO](store, profile, rounds = 5) *> calls.get
      }.asserting(_ shouldBe 5)

    "never fails startup when the store fails" in {
      val store = storeWith(IO.raiseError(new RuntimeException("cold")))
      Warmup.rateLimitPath[IO](store, profile, rounds = 3).attempt
        .asserting(_ shouldBe Right(()))
    }

    "bounds a hanging call by the per-call timeout" in {
      val store = storeWith(IO.never)
      val program = Warmup
        .rateLimitPath[IO](store, profile, rounds = 2, perCallTimeout = 1.second)
        .attempt
      TestControl.executeEmbed(program).asserting(_ shouldBe Right(()))
    }
  }
