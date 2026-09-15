package core

import java.time.Instant

/** Pure token-bucket state and computation module.
  *
  * This is the canonical implementation of the token-bucket algorithm used by
  * both in-memory and DynamoDB rate-limit stores. All refill/consume logic
  * lives here; stores delegate to these functions to avoid duplication.
  *
  * ==Refill formula==
  * {{{
  *   elapsed_sec  = (nowMs - state.lastRefillMs) / 1000.0
  *   tokensToAdd  = elapsed_sec × profile.refillRatePerSecond
  *   refilled     = min(capacity, state.tokens + tokensToAdd)
  * }}}
  *
  * ==Invariants==
  *   - tokens ∈ [0, capacity] at all times.
  *   - version increments by 1 on every successful [[consume]]; unchanged by
  *     [[refill]].
  *   - lastRefillMs never decreases. A successful consume sets it to nowMs,
  *     unless the stored value is already later (see below).
  *   - cost must be ≤ capacity for [[consume]] to ever return Some.
  *
  * ==Clock corrections==
  * nowMs is wall-clock time, and has to be: lastRefillMs is persisted and
  * shared across tasks, where a per-process monotonic clock means nothing. Wall
  * clocks get corrected -- NTP steps, VM resume, chrony slew -- and each
  * correction is visible here as elapsed time that did not really pass.
  *
  * A backward correction would make elapsed negative. Without the guards below
  * that deducts tokens and then, once the clock catches up, refunds them; the
  * key is denied for the length of the step and the books balance later.
  * Clamping elapsed at zero and refusing to move lastRefillMs backward removes
  * both halves: nothing is deducted, and nothing is owed.
  *
  * A forward correction mints rate × step tokens, once. That is unavoidable on
  * a shared wall clock and bounded by the size of the step. Measured on real
  * DynamoDB and on LocalStack at 2-3 s of extra refill per 30 s run; see issue
  * #10.
  *
  * @see
  *   [[storage.DynamoDBRateLimitStore]] for OCC write semantics built on top of
  *   these functions.
  */

case class TokenBucketState(tokens: Double, lastRefillMs: Long, version: Long):
  def tokensInt: Int = tokens.toInt

object TokenBucket:

  /** Apply time-based token refill to a bucket state.
    *
    * Does not modify lastRefillMs or version — those are updated only on a
    * successful [[consume]]. Elapsed is clamped at zero so a backward clock
    * correction cannot deduct tokens.
    */
  def refill(
      state: TokenBucketState,
      nowMs: Long,
      profile: RateLimitProfile,
  ): TokenBucketState =
    val elapsed = math.max(0L, nowMs - state.lastRefillMs) / 1000.0
    val refilled = math.min(
      profile.capacity.toDouble,
      state.tokens + elapsed * profile.refillRatePerSecond,
    )
    state.copy(tokens = refilled)

  /** Attempt to consume `cost` tokens from an already-refilled bucket.
    *
    * Returns `Some(newState)` if there are enough tokens; `None` if
    * insufficient. On success, advances `lastRefillMs` to `nowMs` -- never
    * backward, so a stale or stepped-back clock cannot plant a mark that a
    * later request refills from -- and increments `version`.
    */
  def consume(
      state: TokenBucketState,
      cost: Int,
      nowMs: Long,
  ): Option[TokenBucketState] =
    if state.tokens >= cost then
      Some(TokenBucketState(
        state.tokens - cost,
        math.max(nowMs, state.lastRefillMs),
        state.version + 1,
      ))
    else None

  /** Instant at which the bucket will be fully replenished. */
  def resetAt(nowMs: Long, tokens: Double, profile: RateLimitProfile): Instant =
    val tokensToFull = profile.capacity - tokens
    val secondsToFull = (tokensToFull / profile.refillRatePerSecond).ceil.toLong
    Instant.ofEpochMilli(nowMs + secondsToFull * 1000)

  /** Seconds a caller must wait before `cost` tokens will be available. */
  def retryAfterSeconds(
      cost: Int,
      tokens: Double,
      profile: RateLimitProfile,
  ): Int = math.ceil((cost - tokens) / profile.refillRatePerSecond).toInt.max(1)
