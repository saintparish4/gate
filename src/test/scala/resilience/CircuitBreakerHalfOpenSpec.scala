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

/** `halfOpenMaxCalls` bounds the probes in flight while half-open.
  *
  * A single-permit semaphore used to guard the half-open path, so one probe ran
  * at a time whatever the setting said: 3, the default, behaved as 1. The
  * probes here block on a gate, so every call the breaker admits is in flight
  * at once and can be counted.
  */
class CircuitBreakerHalfOpenSpec
    extends AsyncFreeSpec with AsyncIOSpec with Matchers:

  given Logger[IO] = NoOpLogger[IO]

  private val resetTimeout = 30.seconds
  private val callers = 10

  private def openBreaker(halfOpenMaxCalls: Int): IO[CircuitBreaker[IO]] =
    for
      cb <- CircuitBreaker[IO](
        "half-open-test",
        CircuitBreakerConfig(
          maxFailures = 1,
          resetTimeout = resetTimeout,
          halfOpenMaxCalls = halfOpenMaxCalls,
        ),
      )
      _ <- cb.protect(IO.raiseError(new RuntimeException("down"))).attempt
    yield cb

  /** Send `callers` calls at a breaker whose timeout has passed. Returns how
    * many ran, how many were refused while those were in flight, and the state
    * after `finish` let the probes complete.
    */
  private def probe(halfOpenMaxCalls: Int)(
      finish: Deferred[IO, Either[Throwable, Unit]] => IO[Unit],
  ): IO[(Int, Int, CircuitState, CircuitState)] = TestControl.executeEmbed {
    for
      cb <- openBreaker(halfOpenMaxCalls)
      _ <- IO.sleep(resetTimeout)
      started <- Ref.of[IO, Int](0)
      gate <- Deferred[IO, Either[Throwable, Unit]]
      fibers <- List.fill(callers)(
        cb.protect(started.update(_ + 1) *> gate.get.rethrow).attempt.start,
      ).sequence
      // Long enough for every fiber to reach the gate or be refused, and
      // shorter than the reset timeout.
      _ <- IO.sleep(1.second)
      running <- started.get
      during <- cb.state
      refused <- cb.metrics.map(_.rejectedCount.toInt)
      _ <- finish(gate)
      _ <- fibers.traverse_(_.join)
      after <- cb.state
    yield (running, refused, during, after)
  }

  "a half-open breaker" - {

    List(1, 3, 5).foreach(limit =>
      s"admits exactly $limit of $callers concurrent probes when halfOpenMaxCalls = $limit" in
        probe(limit)(_.complete(Right(())).void)
          .asserting { case (running, refused, during, after) =>
            running shouldBe limit
            refused shouldBe callers - limit
            during shouldBe CircuitState.HalfOpen
            after shouldBe CircuitState.Closed
          },
    )

    "reopens when its probes fail, however many were in flight" in
      probe(3)(_.complete(Left(new RuntimeException("still down"))).void)
        .asserting { case (running, _, _, after) =>
          running shouldBe 3
          after shouldBe CircuitState.Open
        }

    "gets a cancelled probe's slot back" in TestControl.executeEmbed(
      for
        cb <- openBreaker(halfOpenMaxCalls = 1)
        _ <- IO.sleep(resetTimeout)
        stuck <- cb.protect(IO.never[Unit]).start
        _ <- IO.sleep(1.second)
        refused <- cb.protect(IO.unit).attempt
        _ <- stuck.cancel
        next <- cb.protect(IO.pure(42)).attempt
        state <- cb.state
      yield (refused.isLeft, next, state),
    ).asserting(_ shouldBe (true, Right(42), CircuitState.Closed))
  }
