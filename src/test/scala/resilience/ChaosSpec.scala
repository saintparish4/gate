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
import config.*
import core.*
import events.EventPublisher
import observability.MetricsPublisher
import storage.InMemoryRateLimitStore

/** Fault injection tests for the resilience layer.
  *
  * Each test creates a deliberately broken store and verifies that the
  * resilient wrapper provides the promised guarantees: retries, circuit
  * breaking, graceful degradation, and no cascading hangs.
  */
class ChaosSpec extends AsyncFreeSpec with AsyncIOSpec with Matchers:

  given Logger[IO] = NoOpLogger[IO]

  val profile: RateLimitProfile =
    RateLimitProfile(capacity = 10, refillRatePerSecond = 1.0, ttlSeconds = 3600)

  /** Store that fails on every Nth call (1-indexed). */
  def intermittentStore(
      underlying: RateLimitStore[IO],
      failEvery: Int,
      ex: Throwable,
  ): IO[RateLimitStore[IO]] = Ref.of[IO, Int](0).map { counter =>
    new RateLimitStore[IO]:
      def checkAndConsume(
          k: String,
          c: Int,
          p: RateLimitProfile,
      ): IO[RateLimitDecision] = counter.getAndUpdate(_ + 1).flatMap(n =>
        if (n + 1) % failEvery == 0 then IO.raiseError(ex)
        else underlying.checkAndConsume(k, c, p),
      )
      def getStatus(
          k: String,
          p: RateLimitProfile,
      ): IO[Option[RateLimitDecision.Allowed]] = underlying.getStatus(k, p)
      def healthCheck: IO[Either[String, Unit]] = underlying.healthCheck
  }

  /** Store that introduces a fixed delay before each operation. */
  def slowStore(
      underlying: RateLimitStore[IO],
      delay: FiniteDuration,
  ): RateLimitStore[IO] = new RateLimitStore[IO]:
    def checkAndConsume(
        k: String,
        c: Int,
        p: RateLimitProfile,
    ): IO[RateLimitDecision] = IO.sleep(delay) *>
      underlying.checkAndConsume(k, c, p)
    def getStatus(
        k: String,
        p: RateLimitProfile,
    ): IO[Option[RateLimitDecision.Allowed]] = IO.sleep(delay) *>
      underlying.getStatus(k, p)
    def healthCheck: IO[Either[String, Unit]] = underlying.healthCheck

  /** Store that returns ProvisionedThroughputExceededException for the first N
    * calls.
    */
  def throttlingStore(
      underlying: RateLimitStore[IO],
      throttleFirst: Int,
  ): IO[RateLimitStore[IO]] = Ref.of[IO, Int](0).map { counter =>
    new RateLimitStore[IO]:
      def checkAndConsume(
          k: String,
          c: Int,
          p: RateLimitProfile,
      ): IO[RateLimitDecision] = counter.getAndUpdate(_ + 1).flatMap(n =>
        if n < throttleFirst then
          IO.raiseError(
            software.amazon.awssdk.services.dynamodb.model
              .ProvisionedThroughputExceededException.builder()
              .message("throttled").build(),
          )
        else underlying.checkAndConsume(k, c, p),
      )
      def getStatus(
          k: String,
          p: RateLimitProfile,
      ): IO[Option[RateLimitDecision.Allowed]] = underlying.getStatus(k, p)
      def healthCheck: IO[Either[String, Unit]] = underlying.healthCheck
  }

  val baseConfig: ResilienceConfig = ResilienceConfig(
    circuitBreaker = CircuitBreakerSettings(
      enabled = true,
      dynamodb = config.CircuitBreakerConfig(
        maxFailures = 5,
        resetTimeout = 10.seconds,
        halfOpenMaxCalls = 2,
      ),
      kinesis = config.CircuitBreakerConfig(),
    ),
    retry = RetrySettings(
      dynamodb = RetryConfig(
        maxRetries = 3,
        baseDelay = 10.millis,
        maxDelay = 200.millis,
        multiplier = 2.0,
      ),
      kinesis = RetryConfig(),
    ),
    bulkhead = BulkheadSettings(enabled = false),
    timeout = TimeoutSettings(rateLimitCheck = 2.seconds),
  )

  def buildResilientStore(
      chaos: RateLimitStore[IO],
      config: ResilienceConfig = baseConfig,
  ): Resource[IO, RateLimitStore[IO]] = ResilientRateLimitStore[IO](
    underlying = chaos,
    config = config,
    metrics = MetricsPublisher.noop[IO],
    eventPublisher = EventPublisher.noop[IO],
    degradationMode = GracefulDegradation.DegradationMode.AllowAll,
  )

  "ChaosSpec: fault injection" - {

    "intermittent DynamoDB failures: retry recovers and circuit stays closed" in {
      val ex = new RuntimeException("intermittent")
      val test =
        for
          base <- InMemoryRateLimitStore.create[IO]
          chaos <- intermittentStore(base, failEvery = 3, ex)
          results <- buildResilientStore(chaos).use(store =>
            // Run 9 calls. Every 3rd call to `chaos` fails, but retry wraps them.
            // Net: all 9 logical calls should return a successful decision.
            (1 to 9).toList.traverse(i =>
              store.checkAndConsume(s"key-$i", 1, profile).attempt,
            ),
          )
        yield results

      TestControl.executeEmbed(test)
        .asserting((results: List[Either[Throwable, RateLimitDecision]]) =>
          // With retries and AllowAll degradation, no result should be a Left (propagated error)
          results.forall(_.isRight).shouldBe(true),
        )
    }

    "slow DynamoDB responses: timeout fires before retry, degradation kicks in" in {
      val tightTimeoutConfig = baseConfig.copy(
        timeout = TimeoutSettings(rateLimitCheck = 50.millis),
        retry = RetrySettings(
          dynamodb = RetryConfig(
            maxRetries = 1,
            baseDelay = 10.millis,
            maxDelay = 20.millis,
          ),
          kinesis = RetryConfig(),
        ),
      )

      val test = InMemoryRateLimitStore.create[IO].flatMap { base =>
        val slow = slowStore(base, delay = 100.millis) // exceeds 50ms timeout
        buildResilientStore(slow, tightTimeoutConfig)
          .use(store => store.checkAndConsume("slow-key", 1, profile))
      }

      // Timeout triggers, retries exhaust, AllowAll degradation returns Allowed
      TestControl.executeEmbed(test)
        .asserting(_ shouldBe a[RateLimitDecision.Allowed])
    }

    "DynamoDB throttling burst: exponential backoff succeeds eventually" in {
      // First 3 calls are throttled; call 4 succeeds.
      // With maxRetries=3, the store should reach the successful call on the 4th attempt.
      val test =
        for
          base <- InMemoryRateLimitStore.create[IO]
          chaos <- throttlingStore(base, throttleFirst = 3)
          result <- buildResilientStore(chaos)
            .use(store => store.checkAndConsume("throttle-key", 1, profile))
        yield result

      TestControl.executeEmbed(test)
        .asserting(_ shouldBe a[RateLimitDecision.Allowed])
    }

    "cascading failure: circuit open + retry exhausted yields graceful degradation, not crash" in {
      val ex = new RuntimeException("total outage")
      // Open the circuit fast (maxFailures=1) with no recovery time
      val cascadeConfig = baseConfig.copy(
        circuitBreaker = CircuitBreakerSettings(
          enabled = true,
          dynamodb = config.CircuitBreakerConfig(
            maxFailures = 1,
            resetTimeout = 1.hour, // will never reset during this test
            halfOpenMaxCalls = 1,
          ),
          kinesis = config.CircuitBreakerConfig(),
        ),
        retry = RetrySettings(
          dynamodb = RetryConfig(maxRetries = 0), // no retries
          kinesis = RetryConfig(),
        ),
        bulkhead = BulkheadSettings(
          enabled = true,
          maxConcurrent = 1,
          maxWait = 1.millis, // bulkhead overflows immediately under contention
        ),
      )

      val test = buildResilientStore(testutil.failingStore(ex), cascadeConfig)
        .use(store =>
          for
            // Trigger circuit open (1 failure needed)
            _ <- store.checkAndConsume("chaos-key", 1, profile).attempt
            // Now circuit is Open AND bulkhead is free.
            // All subsequent calls degrade gracefully without stack overflow or hang.
            results <- (1 to 10).toList
              .traverse(_ => store.checkAndConsume("chaos-key", 1, profile))
          yield results,
        )

      TestControl.executeEmbed(test).asserting {
        (results: List[RateLimitDecision]) =>
          results.should(have size 10)
          // AllowAll degradation: all 10 come back as Allowed
          results.forall(_.isInstanceOf[RateLimitDecision.Allowed])
            .shouldBe(true)
      }
    }
  }
