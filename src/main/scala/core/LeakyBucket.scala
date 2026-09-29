package core

/** Pure leaky-bucket state and computation, used by
  * [[storage.LeakyBucketRateLimitStore]].
  *
  * ==Leak formula==
  * {{{
  *   elapsed_sec = max(0, nowMs - state.lastLeakMs) / 1000.0
  *   level       = max(0, state.level - elapsed_sec × profile.refillRatePerSecond)
  * }}}
  *
  * ==Clock corrections==
  * The same rule as [[TokenBucket]] (commit a9a7acb). A backward correction
  * made elapsed negative, which raised the level, so the key was denied for the
  * length of the step; and the pour moved lastLeakMs back to the stepped clock,
  * so a task whose clock had not stepped drained the gap again. Clamping
  * elapsed at zero and never moving lastLeakMs backward removes both: a step
  * neither denies nor mints.
  */
case class LeakyBucketState(level: Double, lastLeakMs: Long, version: Long)

object LeakyBucket:

  /** Drain the bucket for the time since `lastLeakMs`, clamped at zero. Does
    * not change `lastLeakMs` or `version`; only [[pour]] does.
    */
  def leak(
      state: LeakyBucketState,
      nowMs: Long,
      profile: RateLimitProfile,
  ): LeakyBucketState =
    val elapsedSec = math.max(0L, nowMs - state.lastLeakMs) / 1000.0
    state.copy(level =
      math.max(0.0, state.level - elapsedSec * profile.refillRatePerSecond),
    )

  /** Add `cost` to an already-leaked bucket if it fits under capacity. On
    * success, advances `lastLeakMs` to `nowMs` (never backward) and increments
    * `version`.
    */
  def pour(
      state: LeakyBucketState,
      cost: Int,
      nowMs: Long,
      profile: RateLimitProfile,
  ): Option[LeakyBucketState] =
    if state.level + cost <= profile.capacity then
      Some(LeakyBucketState(
        state.level + cost,
        math.max(nowMs, state.lastLeakMs),
        state.version + 1,
      ))
    else None
