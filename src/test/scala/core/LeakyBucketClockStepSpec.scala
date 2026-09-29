package core

import org.scalacheck.Gen
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

/** Clock corrections against the leaky bucket, the companion of
  * TokenBucketOccBoundSpec.
  *
  * `attempt` mirrors LeakyBucketRateLimitStore.singleAttempt -- leak, then
  * pour, with the write conditioned on the version read -- so a serial fold
  * over committed attempts is the history DynamoDB would hold. Each attempt
  * reads its own clock, which is how a backward step (or a task whose clock
  * trails another's) reaches the store.
  */
class LeakyBucketClockStepSpec
    extends AnyFreeSpec with ScalaCheckPropertyChecks with Matchers:

  private val profile =
    RateLimitProfile(capacity = 10, refillRatePerSecond = 1.0, ttlSeconds = 3600)

  private def attempt(
      state: Option[LeakyBucketState],
      cost: Int,
      nowMs: Long,
  ): Option[LeakyBucketState] =
    val cur = state.getOrElse(LeakyBucketState(0.0, nowMs, 0L))
    LeakyBucket.pour(LeakyBucket.leak(cur, nowMs, profile), cost, nowMs, profile)

  "a backward step does not deny a pour that fits with no time elapsed" in {
    val poured = attempt(None, 5, nowMs = 10_000L)
    // The clock steps back 3 s. The bucket holds 5 of 10, so 5 more fits even
    // if nothing drained. Unclamped, elapsed was -3 s, the level rose to 8, and
    // this was refused.
    val afterStep = attempt(poured, 5, nowMs = 7_000L)

    afterStep.map(_.level) shouldBe Some(10.0)
    afterStep.map(_.lastLeakMs) shouldBe Some(10_000L)
  }

  "a backward step does not drain the same interval twice" in {
    val half = attempt(None, 5, nowMs = 10_000L)
    // A task 3 s behind fills the bucket. Nothing has drained by either clock.
    val filled = attempt(half, 5, nowMs = 7_000L)
    filled.map(_.level) shouldBe Some(10.0)
    // Had that pour moved lastLeakMs back to 7 000, the leading task would now
    // drain 3 s that never passed on its clock, and admit this.
    attempt(filled, 1, nowMs = 10_000L) shouldBe None
  }

  "two skewed clocks on one key never admit more than capacity plus the leak" in {
    val gen =
      for
        skew <- Gen.choose(0L, 10_000L)
        n <- Gen.choose(20, 300)
        steps <- Gen.listOfN(
          n,
          Gen.zip(
            Gen.oneOf(true, false),
            Gen.frequency(3 -> Gen.const(0L), 7 -> Gen.choose(1L, 400L)),
          ),
        )
      yield (skew, steps)

    forAll(gen, minSuccessful(500)) { case (skew, steps) =>
      val start = 1_000_000L
      val (_, state, admitted, firstNow, lastNow) = steps.foldLeft(
        (start, Option.empty[LeakyBucketState], 0L, Option.empty[Long], 0L),
      ) { case ((t0, state, admitted, first, last), (leading, advance)) =>
        val t = t0 + advance
        val nowMs = if leading then t else t - skew
        attempt(state, 1, nowMs) match
          case Some(next) => (
              t,
              Some(next),
              admitted + 1,
              first.orElse(Some(nowMs)),
              last.max(nowMs),
            )
          case None => (t, state, admitted, first, last)
      }
      // Every admission raised the level by one, and the level only fell by
      // leaking. The leak can have run only between the first committed clock
      // reading and the latest one, whichever task read them.
      val leakWindowSec = firstNow.map(f => (lastNow - f) / 1000.0)
        .getOrElse(0.0)
      val ceiling = profile.capacity +
        profile.refillRatePerSecond * leakWindowSec
      withClue(s"skew=${skew}ms admitted=$admitted state=$state: ")(
        admitted.toDouble should be <= ceiling,
      )
    }
  }
