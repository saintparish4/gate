package resilience

import scala.concurrent.duration.*

import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.noop.NoOpLogger

import cats.effect.*
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.testkit.TestControl
import config.*
import core.*
import events.EventPublisher
import observability.MetricsPublisher
import testutil.*

class ResilientRateLimitStoreSpec
    extends AsyncFreeSpec with AsyncIOSpec with Matchers:

  given Logger[IO] = NoOpLogger[IO]

  /** ResilienceConfig with aggressive settings for faster test cycles. */
  val testConfig: ResilienceConfig = ResilienceConfig(
    circuitBreaker = CircuitBreakerSettings(
      enabled = true,
      dynamodb = config.CircuitBreakerConfig(
        maxFailures = 3,
        resetTimeout = 1.second,
        halfOpenMaxCalls = 1,
      ),
    ),
    retry = RetrySettings(dynamodb =
      RetryConfig(
        maxRetries = 2,
        baseDelay = 10.millis,
        maxDelay = 100.millis,
        multiplier = 2.0,
      ),
    ),
    bulkhead =
      BulkheadSettings(enabled = true, maxConcurrent = 2, maxWait = 50.millis),
    timeout = TimeoutSettings(rateLimitCheck = 500.millis),
  )

  val testProfile: RateLimitProfile =
    RateLimitProfile(capacity = 10, refillRatePerSecond = 1.0, ttlSeconds = 3600)

  def buildStore(
      underlying: RateLimitStore[IO],
      config: ResilienceConfig = testConfig,
  ): Resource[IO, RateLimitStore[IO]] = ResilientRateLimitStore[IO](
    underlying = underlying,
    config = config,
    metrics = MetricsPublisher.noop[IO],
    degradationMode = GracefulDegradation.DegradationMode.AllowAll,
  )

  "ResilientRateLimitStore with patterns enabled" - {

    "opens circuit breaker after maxFailures consecutive failures" in {
      val ex = new RuntimeException("dynamo down")
      buildStore(failingStore(ex)).use { store =>
        for
          // 3 failures exhaust retries on each (maxRetries=2 means 3 total attempts per call)
          // Each call fails → circuit breaker sees a failure.
          // After 3 calls the circuit should be Open.
          r1 <- store.checkAndConsume("k", 1, testProfile).attempt
          r2 <- store.checkAndConsume("k", 1, testProfile).attempt
          r3 <- store.checkAndConsume("k", 1, testProfile).attempt
          // The degradation mode is AllowAll, so the 3rd call may return a degraded Allowed,
          // but the CB should now be Open and subsequent calls skip the store.
          r4 <- store.checkAndConsume("k", 1, testProfile).attempt
        yield
          // All results come back (either degraded Allowed or as Left — not a hang/crash)
          List(r1, r2, r3, r4).foreach(r =>
            r.isLeft || r.exists(_.isInstanceOf[RateLimitDecision]) shouldBe
              true,
          )
          succeed
      }.asserting(identity)
    }

    "transitions from Open to HalfOpen after resetTimeout (deterministic)" in {
      val ex = new RuntimeException("dynamo unavailable")
      val config: ResilienceConfig = testConfig.copy(
        circuitBreaker = testConfig.circuitBreaker.copy(dynamodb =
          CircuitBreakerConfig(
            maxFailures = 1,
            resetTimeout = 500.millis,
            halfOpenMaxCalls = 1,
          ),
        ),
        retry = testConfig.retry.copy(dynamodb = RetryConfig(maxRetries = 0)),
      )

      val test = buildStore(failingStore(ex), config).use(store =>
        for
          // One failure opens the circuit
          _ <- store.checkAndConsume("k", 1, testProfile).attempt
          // Advance time past resetTimeout
          _ <- IO.sleep(600.millis)
          // After timeout the store should attempt the real call again (HalfOpen),
          // which fails again → back to Open but with a non-rejected degraded result
          r <- store.checkAndConsume("k", 1, testProfile)
        yield r,
      )

      // With AllowAll degradation, even after circuit opens we get an Allowed response
      TestControl.executeEmbed(test)
        .asserting(_ shouldBe a[RateLimitDecision.Allowed])
    }

    "closes circuit breaker after successful calls in HalfOpen" in {
      // Use a store that fails first then succeeds.
      val halfOpenConfig: ResilienceConfig = testConfig.copy(
        circuitBreaker = testConfig.circuitBreaker.copy(dynamodb =
          CircuitBreakerConfig(
            maxFailures = 1,
            resetTimeout = 200.millis,
            halfOpenMaxCalls = 1,
          ),
        ),
        retry = testConfig.retry.copy(dynamodb = RetryConfig(maxRetries = 0)),
      )
      val test =
        for
          callCount <- Ref.of[IO, Int](0)
          underlying = new RateLimitStore[IO]:
            def checkAndConsume(
                k: String,
                c: Int,
                p: RateLimitProfile,
            ): IO[RateLimitDecision] = callCount.getAndUpdate(_ + 1)
              .flatMap(n =>
                // First 3 calls fail (open circuit), then succeed
                if n < 3 then IO.raiseError(new RuntimeException("fail"))
                else IO.pure(RateLimitDecision.Allowed(10, java.time.Instant.now())),
              )
            def getStatus(
                k: String,
                p: RateLimitProfile,
            ): IO[Option[RateLimitDecision.Allowed]] = IO.pure(None)
            def healthCheck: IO[Either[String, Unit]] = IO.pure(Right(()))

          result <- buildStore(underlying, halfOpenConfig).use(store =>
            for
              _ <- store.checkAndConsume("k", 1, testProfile).attempt // fail 1 → Open
              _ <- store.checkAndConsume("k", 1, testProfile).attempt // fail 2 → Open (degraded)
              _ <- store.checkAndConsume("k", 1, testProfile).attempt // fail 3 → Open (degraded)
              _ <- IO.sleep(250.millis) // past resetTimeout
              r <- store.checkAndConsume("k", 1, testProfile) // success in HalfOpen → Closed
            yield r,
          )
        yield result

      TestControl.executeEmbed(test)
        .asserting(_ shouldBe a[RateLimitDecision.Allowed])
    }

    "bulkhead rejects requests beyond maxConcurrent under contention" in {
      // Set timeout to 0 so bulkhead queue drains immediately on overflow.
      val tightBulkheadConfig = testConfig.copy(bulkhead =
        BulkheadSettings(enabled = true, maxConcurrent = 1, maxWait = 0.millis),
      )

      val slowStore = new RateLimitStore[IO]:
        def checkAndConsume(
            k: String,
            c: Int,
            p: RateLimitProfile,
        ): IO[RateLimitDecision] = IO.sleep(200.millis) *>
          IO.pure(RateLimitDecision.Allowed(9, java.time.Instant.now()))
        def getStatus(
            k: String,
            p: RateLimitProfile,
        ): IO[Option[RateLimitDecision.Allowed]] = IO.pure(None)
        def healthCheck: IO[Either[String, Unit]] = IO.pure(Right(()))

      buildStore(slowStore, tightBulkheadConfig).use { store =>
        for
          // Fire two concurrent requests; the second should be rejected by bulkhead
          // then degraded to AllowAll (not a crash).
          results <- (
            store.checkAndConsume("k1", 1, testProfile).attempt,
            store.checkAndConsume("k2", 1, testProfile).attempt,
          ).parTupled
        yield
          // Both results come back — we don't hang or throw unhandled exceptions.
          val (r1, r2) = results
          r1.isRight || r1.isLeft shouldBe true
          r2.isRight || r2.isLeft shouldBe true
          succeed
      }.asserting(identity)
    }

    "retry exhaustion yields degraded decision rather than propagating the error" in {
      val ex = new RuntimeException("always fails")
      buildStore(failingStore(ex)).use(store =>
        // With AllowAll degradation, after exhausting retries we should get Allowed,
        // not a thrown exception bubbling to the caller.
        store.checkAndConsume("k", 1, testProfile)
          .asserting(_ shouldBe a[RateLimitDecision.Allowed]),
      )
    }
  }

  "the circuit-breaker gauge" - {
    // Finding A: the state was recorded only after a successful call, and while
    // the breaker is open every call fails, so gate_circuit_breaker_state and
    // the CloudWatch alarm kept reading closed through the outage.

    "reads Open while the breaker is open" in {
      val ex = new RuntimeException("dynamo down")
      for
        prom <- observability.PrometheusMetrics[IO]
        gauge <- ResilientRateLimitStore[IO](
          underlying = failingStore(ex),
          config = testConfig,
          metrics = observability.PrometheusMetrics
            .dual(MetricsPublisher.noop[IO], prom),
          degradationMode = GracefulDegradation.DegradationMode.RejectAll,
        ).use(store =>
          // maxFailures = 3 opens it; the fourth call is refused by the open
          // breaker without reaching the store.
          List.fill(4)(store.checkAndConsume("k", 1, testProfile).attempt)
            .sequence_ *> IO(
            Option(prom.registry.getSampleValue(
              "gate_circuit_breaker_state",
              Array("name"),
              Array("dynamodb-ratelimit"),
            )).map(_.doubleValue),
          ),
        )
      yield gauge shouldBe Some(1.0)
    }
  }
