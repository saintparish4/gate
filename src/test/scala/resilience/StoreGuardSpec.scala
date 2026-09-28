package resilience

import scala.concurrent.duration.*

import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.noop.NoOpLogger

import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.testkit.TestControl
import cats.effect.{IO, Ref}
import config.BulkheadSettings
import core.GateError

/** The idempotency and quota stores had no timeout and no bulkhead (finding G):
  * a slow DynamoDB held each request for the SDK's 10 s and let callers pile
  * up.
  */
class StoreGuardSpec extends AsyncFreeSpec with AsyncIOSpec with Matchers:

  given Logger[IO] = NoOpLogger[IO]

  private val noBulkhead = BulkheadSettings(enabled = false)

  "a call that hangs becomes a StoreTimeout after the bound, once" in
    TestControl.executeEmbed(
      StoreGuard.resource[IO]("quota", 5.seconds, noBulkhead).use(guard =>
        for
          calls <- Ref.of[IO, Int](0)
          started <- IO.monotonic
          result <- guard(calls.update(_ + 1) *> IO.never[Unit]).attempt
          ended <- IO.monotonic
          n <- calls.get
        yield (result, ended - started, n),
      ),
    ).asserting { case (result, took, calls) =>
      result.left.toOption.collect { case e: GateError.StoreTimeout =>
        e.operation
      } shouldBe Some("quota")
      took shouldBe 5.seconds
      calls shouldBe 1
    }

  "a full bulkhead refuses the next caller instead of queueing it forever" in
    TestControl.executeEmbed(
      StoreGuard.resource[IO](
        "idempotency",
        1.minute,
        BulkheadSettings(enabled = true, maxConcurrent = 1, maxWait = 100.millis),
      ).use(guard =>
        for
          holder <- guard(IO.sleep(10.seconds)).start
          _ <- IO.sleep(1.second)
          second <- guard(IO.unit).attempt
          _ <- holder.join
        yield second,
      ),
    ).asserting(
      _.left.toOption.map(_.getClass) shouldBe
        Some(classOf[GateError.BulkheadFull]),
    )
