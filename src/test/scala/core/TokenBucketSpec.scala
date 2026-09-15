package core

import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

/** Unit tests for the pure TokenBucket module.
  *
  * These tests exercise the algorithm directly, without any IO or storage
  * layer. They cover edge cases that are difficult to observe through the store
  * API (e.g. zero elapsed time, near-zero refill rates) and document the
  * invariants stated in the TokenBucket scaladoc.
  */
class TokenBucketSpec extends AnyFreeSpec with Matchers:

  val profile: RateLimitProfile =
    RateLimitProfile(capacity = 10, refillRatePerSecond = 1.0, ttlSeconds = 3600)

  "TokenBucket.refill" - {

    "should add zero tokens when elapsed time is 0ms" in {
      // elapsed = (nowMs - lastRefillMs) / 1000 = 0 → tokensToAdd = 0
      val state =
        TokenBucketState(tokens = 5.0, lastRefillMs = 1000L, version = 1L)
      val result = TokenBucket.refill(state, nowMs = 1000L, profile)
      result.tokens shouldBe 5.0
    }

    "should add tokens proportional to elapsed time" in {
      // 3 seconds elapsed, rate = 1/s → adds 3 tokens (5 + 3 = 8)
      val state = TokenBucketState(tokens = 5.0, lastRefillMs = 0L, version = 1L)
      val result = TokenBucket.refill(state, nowMs = 3000L, profile)
      result.tokens shouldBe 8.0 +- 0.001
    }

    "should cap tokens at capacity, never exceed it" in {
      // 9 tokens + 5 seconds × 1 token/s = 14 → capped at 10
      val state = TokenBucketState(tokens = 9.0, lastRefillMs = 0L, version = 1L)
      val result = TokenBucket.refill(state, nowMs = 5000L, profile)
      result.tokens shouldBe 10.0
    }

    "should not refill beyond capacity even for a very large elapsed window" in {
      val state = TokenBucketState(tokens = 0.0, lastRefillMs = 0L, version = 1L)
      val result = TokenBucket.refill(state, nowMs = 999_999_000L, profile)
      result.tokens shouldBe 10.0
    }

    "should not modify version on refill" in {
      val state = TokenBucketState(tokens = 5.0, lastRefillMs = 0L, version = 7L)
      val result = TokenBucket.refill(state, nowMs = 1000L, profile)
      result.version shouldBe 7L
    }

    "should not modify lastRefillMs on refill (only consume does)" in {
      val state = TokenBucketState(tokens = 5.0, lastRefillMs = 0L, version = 1L)
      val result = TokenBucket.refill(state, nowMs = 1000L, profile)
      result.lastRefillMs shouldBe 0L
    }

    "should produce negligible refill for a near-zero refill rate" in {
      // refillRatePerSecond is bounded below at > 0 by RateLimitProfile.require, so
      // we use the smallest practical value (0.001 tokens/s) to confirm the
      // formula behaves correctly at low rates.
      val slowProfile = RateLimitProfile(
        capacity = 10,
        refillRatePerSecond = 0.001,
        ttlSeconds = 3600,
      )
      val state = TokenBucketState(tokens = 0.0, lastRefillMs = 0L, version = 1L)
      val result = TokenBucket.refill(state, nowMs = 100L, slowProfile) // 0.1 s × 0.001/s = 0.0001
      result.tokens should be < 0.001
    }

    "RateLimitProfile should reject a zero refill rate at construction" in {
      an[IllegalArgumentException] should be thrownBy RateLimitProfile(
        capacity = 10,
        refillRatePerSecond = 0.0,
        ttlSeconds = 3600,
      )
    }

    "RateLimitProfile should reject a negative refill rate at construction" in {
      an[IllegalArgumentException] should be thrownBy RateLimitProfile(
        capacity = 10,
        refillRatePerSecond = -1.0,
        ttlSeconds = 3600,
      )
    }

    "RateLimitProfile should reject zero capacity at construction" in {
      an[IllegalArgumentException] should be thrownBy RateLimitProfile(
        capacity = 0,
        refillRatePerSecond = 1.0,
        ttlSeconds = 3600,
      )
    }
  }

  "TokenBucket.consume" - {

    "should succeed and deduct exactly cost tokens" in {
      val state =
        TokenBucketState(tokens = 10.0, lastRefillMs = 0L, version = 1L)
      val result = TokenBucket.consume(state, cost = 3, nowMs = 1000L)
      result shouldBe defined
      result.get.tokens shouldBe 7.0
    }

    "should succeed when cost equals available tokens (boundary)" in {
      val state = TokenBucketState(tokens = 5.0, lastRefillMs = 0L, version = 1L)
      val result = TokenBucket.consume(state, cost = 5, nowMs = 1000L)
      result shouldBe defined
      result.get.tokens shouldBe 0.0
    }

    "should return None when cost exceeds available tokens" in {
      val state = TokenBucketState(tokens = 3.0, lastRefillMs = 0L, version = 1L)
      val result = TokenBucket.consume(state, cost = 4, nowMs = 1000L)
      result shouldBe None
    }

    "should leave original state unchanged on rejection (tokens never go negative)" in {
      val state = TokenBucketState(tokens = 2.0, lastRefillMs = 0L, version = 1L)
      TokenBucket.consume(state, cost = 5, nowMs = 1000L)
      // state is immutable — verify it is unchanged
      state.tokens shouldBe 2.0
      state.version shouldBe 1L
    }

    "should succeed for cost = 0 (no tokens consumed)" in {
      // cost = 0 satisfies tokens >= 0; the API layer rejects it before reaching
      // the store (see RateLimitApi.check), but the algorithm itself allows it.
      val state = TokenBucketState(tokens = 5.0, lastRefillMs = 0L, version = 1L)
      val result = TokenBucket.consume(state, cost = 0, nowMs = 1000L)
      result shouldBe defined
      result.get.tokens shouldBe 5.0
    }

    "should increment version exactly once per successful consume" in {
      val state =
        TokenBucketState(tokens = 10.0, lastRefillMs = 0L, version = 4L)
      val result = TokenBucket.consume(state, cost = 1, nowMs = 1000L)
      result.get.version shouldBe 5L
    }

    "should set lastRefillMs to nowMs on successful consume" in {
      val state =
        TokenBucketState(tokens = 10.0, lastRefillMs = 0L, version = 1L)
      val result = TokenBucket.consume(state, cost = 1, nowMs = 9999L)
      result.get.lastRefillMs shouldBe 9999L
    }

    "should not change version on rejection" in {
      val state = TokenBucketState(tokens = 1.0, lastRefillMs = 0L, version = 3L)
      val result = TokenBucket.consume(state, cost = 5, nowMs = 1000L)
      result shouldBe None
      state.version shouldBe 3L // original state untouched
    }
  }

  "TokenBucket under clock corrections" - {
    // nowMs is wall clock and has to be -- lastRefillMs is shared across tasks.
    // Wall clocks get corrected. These pin what a correction may and may not do.

    "a backward correction adds no tokens and deducts none" in {
      // Clock stepped back 3s: now is earlier than the last refill mark.
      val state =
        TokenBucketState(tokens = 5.0, lastRefillMs = 10_000L, version = 1L)
      val result = TokenBucket.refill(state, nowMs = 7_000L, profile)
      result.tokens shouldBe 5.0
    }

    "consume never moves lastRefillMs backward" in {
      val state =
        TokenBucketState(tokens = 5.0, lastRefillMs = 10_000L, version = 1L)
      val result = TokenBucket.consume(state, cost = 1, nowMs = 7_000L)
      result shouldBe defined
      result.get.lastRefillMs shouldBe 10_000L
    }

    "consume still advances lastRefillMs to now when the clock is monotone" in {
      val state =
        TokenBucketState(tokens = 5.0, lastRefillMs = 10_000L, version = 1L)
      val result = TokenBucket.consume(state, cost = 1, nowMs = 12_000L)
      result shouldBe defined
      result.get.lastRefillMs shouldBe 12_000L
    }

    "a backward step followed by catch-up does not mint the step" in {
      // Without the guards a 3s backward step plants lastRefillMs=7_000; the
      // next request at 12_000 then refills 5s instead of 2s and mints the
      // 3s difference. Issue #10 measured exactly this class of excess.
      val before =
        TokenBucketState(tokens = 5.0, lastRefillMs = 10_000L, version = 1L)
      val stepped = TokenBucket
        .consume(TokenBucket.refill(before, 7_000L, profile), 1, 7_000L).get
      stepped.lastRefillMs shouldBe 10_000L
      stepped.tokens shouldBe 4.0

      val later = TokenBucket.refill(stepped, nowMs = 12_000L, profile)
      // 2s of real refill at 1/s from the un-regressed mark, not 5s.
      later.tokens shouldBe 6.0
    }
  }

  "TokenBucket.retryAfterSeconds" - {

    "should return 1 as the minimum retry delay" in {
      // ceil((1 - 0.9) / 1.0) = ceil(0.1) = 1
      val result = TokenBucket.retryAfterSeconds(cost = 1, tokens = 0.9, profile)
      result shouldBe 1
    }

    "should calculate wait proportional to deficit" in {
      // ceil((10 - 0) / 1.0) = 10
      val result = TokenBucket
        .retryAfterSeconds(cost = 10, tokens = 0.0, profile)
      result shouldBe 10
    }

    "should scale with refill rate" in {
      // fast profile: refillRate = 10/s; ceil((5 - 0) / 10) = 1
      val fastProfile = RateLimitProfile(
        capacity = 100,
        refillRatePerSecond = 10.0,
        ttlSeconds = 3600,
      )
      val result = TokenBucket
        .retryAfterSeconds(cost = 5, tokens = 0.0, fastProfile)
      result shouldBe 1
    }
  }

  "TokenBucket.resetAt" - {

    "should return current time when bucket is already full" in {
      val nowMs = 5000L
      val result = TokenBucket.resetAt(nowMs, tokens = 10.0, profile)
      // tokensToFull = 0 → secondsToFull = 0 → Instant.ofEpochMilli(nowMs)
      result.toEpochMilli shouldBe nowMs
    }

    "should return future time proportional to deficit" in {
      val nowMs = 0L
      // 5 tokens missing at 1/s → 5 seconds
      val result = TokenBucket.resetAt(nowMs, tokens = 5.0, profile)
      result.toEpochMilli shouldBe 5000L
    }
  }
