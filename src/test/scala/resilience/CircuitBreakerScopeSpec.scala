package resilience

import scala.concurrent.duration.*

import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.noop.NoOpLogger

import cats.effect.*
import cats.effect.testing.scalatest.AsyncIOSpec

/** The rate-limit circuit breaker is process-wide: one instance guards every
  * key. These cover which failures are allowed to open it, because counting a
  * caller-scoped failure lets one contended key apply degradation mode to every
  * tenant.
  */
class CircuitBreakerScopeSpec
    extends AsyncFreeSpec
    with AsyncIOSpec
    with Matchers:

  given Logger[IO] = NoOpLogger[IO]

  private val tight = CircuitBreakerConfig(
    maxFailures = 2,
    resetTimeout = 100.millis,
    halfOpenMaxCalls = 1,
  )

  private def breaker: IO[CircuitBreaker[IO]] = CircuitBreaker[IO](
    "scope-test",
    tight,
    countsAsFailure = ResilientRateLimitStore.dependencyFailure,
  )

  "the shared breaker" - {

    "stays closed under unbounded per-key OCC contention" in breaker.flatMap {
      cb =>
        val conflict = core.GateError.OCCConflict("hot-key", 3)
        for
          _ <- List.fill(50)(cb.protect(IO.raiseError(conflict)).attempt)
            .sequence
          state <- cb.state
          m <- cb.metrics
        yield
          state shouldBe CircuitState.Closed
          m.failureCount shouldBe 0
    }

    "still surfaces the original error to the caller when it does not count" in
      breaker.flatMap { cb =>
        val conflict = core.GateError.OCCConflict("hot-key", 3)
        cb.protect(IO.raiseError(conflict)).attempt.map { result =>
          result shouldBe Left(conflict)
        }
      }

    "opens on store timeouts, which mean no answer came back" in breaker
      .flatMap { cb =>
        val timeout = core.GateError.StoreTimeout("checkAndConsume", 2.seconds)
        for
          _ <- cb.protect(IO.raiseError(timeout)).attempt
          _ <- cb.protect(IO.raiseError(timeout)).attempt
          state <- cb.state
        yield state shouldBe CircuitState.Open
      }

    "does not let already-shed load feed itself" in breaker.flatMap { cb =>
      for
        _ <- List
          .fill(10)(cb.protect(IO.raiseError(core.GateError.CircuitOpen("x"))).attempt)
          .sequence
        _ <- List
          .fill(10)(cb.protect(IO.raiseError(core.GateError.BulkheadFull("y"))).attempt)
          .sequence
        state <- cb.state
      yield state shouldBe CircuitState.Closed
    }

    "counts a dependency failure even when contention is interleaved with it" in
      breaker.flatMap { cb =>
        val conflict = core.GateError.OCCConflict("hot-key", 1)
        val timeout = core.GateError.StoreTimeout("checkAndConsume", 2.seconds)
        for
          _ <- cb.protect(IO.raiseError(timeout)).attempt
          _ <- cb.protect(IO.raiseError(conflict)).attempt
          mid <- cb.state
          _ <- cb.protect(IO.raiseError(timeout)).attempt
          after <- cb.state
        yield
          // The conflict must neither open the breaker nor reset the count that
          // the timeouts are building up.
          mid shouldBe CircuitState.Closed
          after shouldBe CircuitState.Open
      }

    "recovers after a half-open probe fails with a non-counting error" in
      breaker.flatMap { cb =>
        val timeout = core.GateError.StoreTimeout("checkAndConsume", 2.seconds)
        val conflict = core.GateError.OCCConflict("hot-key", 1)
        for
          _ <- cb.protect(IO.raiseError(timeout)).attempt
          _ <- cb.protect(IO.raiseError(timeout)).attempt
          opened <- cb.state
          _ <- IO.sleep(150.millis)
          // Probe hits per-key contention: proves nothing about DynamoDB, and
          // must hand back the probe slot rather than stranding the breaker in
          // half-open with none left.
          _ <- cb.protect(IO.raiseError(conflict)).attempt
          _ <- IO.sleep(150.millis)
          recovered <- cb.protect(IO.pure(42)).attempt
          state <- cb.state
        yield
          opened shouldBe CircuitState.Open
          recovered shouldBe Right(42)
          state shouldBe CircuitState.Closed
      }
  }

  "dependencyFailure classification" - {

    "treats DynamoDB answering us as evidence it is alive" in IO {
      ResilientRateLimitStore
        .dependencyFailure(core.GateError.OCCConflict("k", 1)) shouldBe false
      ResilientRateLimitStore
        .dependencyFailure(core.GateError.CorruptState("k", "bad")) shouldBe
        false
      ResilientRateLimitStore.dependencyFailure(
        software.amazon.awssdk.services.dynamodb.model
          .ConditionalCheckFailedException.builder().build(),
      ) shouldBe false
    }

    "treats no-answer and transport faults as dependency failures" in IO {
      ResilientRateLimitStore.dependencyFailure(
        core.GateError.StoreTimeout("checkAndConsume", 2.seconds),
      ) shouldBe true
      ResilientRateLimitStore
        .dependencyFailure(new java.io.IOException("connection reset")) shouldBe
        true
      ResilientRateLimitStore.dependencyFailure(
        software.amazon.awssdk.services.dynamodb.model
          .ProvisionedThroughputExceededException.builder().build(),
      ) shouldBe true
    }

    "does not count load it already shed, or a misconfiguration" in IO {
      ResilientRateLimitStore
        .dependencyFailure(core.GateError.CircuitOpen("cb")) shouldBe false
      ResilientRateLimitStore
        .dependencyFailure(core.GateError.BulkheadFull("bh")) shouldBe false
      ResilientRateLimitStore
        .dependencyFailure(core.GateError.ConfigError("bad")) shouldBe false
    }
  }
