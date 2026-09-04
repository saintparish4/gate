package resilience

import scala.concurrent.duration.*

import org.scalacheck.{Gen, Shrink}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class RetryPolicyPropertySpec
    extends AnyFreeSpec with ScalaCheckPropertyChecks with Matchers:

  // Shrink toward 1 so we never try base=0 (RetryPolicy requires baseDelay > 0)
  private given Shrink[Int] =
    Shrink(n => if n <= 1 then Stream.empty else Stream(n / 2).filter(_ >= 1))

  val genBaseDelayMs: Gen[Int] = Gen.choose(1, 1000)
  val genAttempt: Gen[Int] = Gen.choose(1, 50)

  // Build a valid policy with jitterFactor=0 so delays are deterministic.
  def genPolicy(baseMs: Int): Gen[RetryPolicy] =
    for
      maxMs <- Gen.choose(baseMs, 60_000)
      multiplier <- Gen.choose(10, 40).map(_.toDouble / 10.0) // 1.0 – 4.0
      maxRetries <- Gen.choose(0, 10)
    yield RetryPolicy(
      maxRetries = maxRetries,
      baseDelay = baseMs.millis,
      maxDelay = maxMs.millis,
      multiplier = multiplier,
      jitterFactor = 0.0,
    )

  "RetryPolicy properties" - {

    // Relies on Retry.calculateDelay being private[resilience] (see change above)
    "delay is bounded by maxDelay for all attempt numbers" in
      forAll(genBaseDelayMs)(base =>
        forAll(genPolicy(base), genAttempt) { (policy, attempt) =>
          val delay = Retry.calculateDelay(policy, attempt)
          delay should be <= policy.maxDelay
        },
      )

    "delay is monotonically non-decreasing with attempt number (jitter=0)" in
      forAll(genBaseDelayMs)(base =>
        forAll(genPolicy(base))(policy =>
          forAll(Gen.choose(1, 30)) { n =>
            val d1 = Retry.calculateDelay(policy, n)
            val d2 = Retry.calculateDelay(policy, n + 1)
            // d2 >= d1 until capped at maxDelay
            d2 should be >= d1
          },
        ),
      )

    "delay is at least baseDelay for attempt 1" in
      forAll(genBaseDelayMs)(base =>
        forAll(genPolicy(base)) { policy =>
          val delay = Retry.calculateDelay(policy, 1)
          delay should be >= policy.baseDelay
        },
      )

    "RetryPolicy rejects invalid configurations" - {

      "maxDelay < baseDelay throws" in forAll(Gen.choose(2, 100))(base =>
        whenever(base >= 2)(forAll(Gen.choose(1, base - 1))(tooSmall =>
          an[IllegalArgumentException] should be thrownBy
            RetryPolicy(baseDelay = base.millis, maxDelay = tooSmall.millis),
        )),
      )

      "multiplier < 1.0 throws" in
        forAll(Gen.choose(1, 99).map(_.toDouble / 100.0))(bad =>
          an[IllegalArgumentException] should be thrownBy
            RetryPolicy(multiplier = bad),
        )

      "jitterFactor > 1.0 throws" in
        forAll(Gen.choose(101, 200).map(_.toDouble / 100.0))(bad =>
          an[IllegalArgumentException] should be thrownBy
            RetryPolicy(jitterFactor = bad),
        )

      "negative maxRetries throws" in forAll(Gen.choose(-50, -1))(bad =>
        an[IllegalArgumentException] should be thrownBy
          RetryPolicy(maxRetries = bad),
      )
    }
  }
