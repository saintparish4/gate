package resilience

import scala.concurrent.duration.*

import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.noop.NoOpLogger

import cats.effect.*
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.testkit.TestControl
import cats.syntax.all.*
import core.GateError

/** The bulkhead bounds how long a caller waits for a permit; it must never
  * bound how long the admitted work takes.
  */
class BulkheadSpec extends AsyncFreeSpec with AsyncIOSpec with Matchers:

  given Logger[IO] = NoOpLogger[IO]

  def bulkhead(maxConcurrent: Int, maxWait: FiniteDuration): IO[Bulkhead[IO]] =
    Bulkhead[IO]("test", BulkheadConfig(maxConcurrent, maxWait))

  "Bulkhead" - {

    "does not cut off a slow operation once it holds a permit" in {
      val program =
        for
          bh <- bulkhead(maxConcurrent = 1, maxWait = 10.millis)
          result <- bh.execute(IO.sleep(5.seconds).as(42))
          inFlight <- bh.inFlight
        yield (result, inFlight)

      TestControl.executeEmbed(program).asserting(_ shouldBe (42, 0))
    }

    "rejects a caller that cannot get a permit within maxWait" in {
      val program =
        for
          bh <- bulkhead(maxConcurrent = 1, maxWait = 10.millis)
          holder <- bh.execute(IO.sleep(1.second).as("held")).start
          _ <- IO.sleep(1.milli)
          rejected <- bh.execute(IO.pure("late")).attempt
          held <- holder.joinWithNever
          queued <- bh.queued
          inFlight <- bh.inFlight
        yield (rejected, held, queued, inFlight)

      TestControl.executeEmbed(program).asserting {
        case (rejected, held, queued, inFlight) =>
          rejected shouldBe Left(GateError.BulkheadFull("test"))
          held shouldBe "held"
          queued shouldBe 0
          inFlight shouldBe 0
      }
    }

    "releases the permit when the operation fails" in {
      val boom = new RuntimeException("boom")
      val program =
        for
          bh <- bulkhead(maxConcurrent = 1, maxWait = 10.millis)
          failed <- bh.execute(IO.raiseError[Int](boom)).attempt
          next <- bh.execute(IO.pure(1))
          inFlight <- bh.inFlight
        yield (failed, next, inFlight)

      TestControl.executeEmbed(program).asserting {
        case (failed, next, inFlight) =>
          failed shouldBe Left(boom)
          next shouldBe 1
          inFlight shouldBe 0
      }
    }

    "admits waiters as permits free up" in {
      val program =
        for
          bh <- bulkhead(maxConcurrent = 2, maxWait = 10.seconds)
          results <- (1 to 6).toList
            .parTraverse(i => bh.execute(IO.sleep(100.millis).as(i)))
          inFlight <- bh.inFlight
        yield (results.sorted, inFlight)

      TestControl.executeEmbed(program).asserting(_ shouldBe ((1 to 6).toList, 0))
    }
  }
