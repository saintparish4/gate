package core

import java.time.Instant

/** Pure sliding window math. No effects; all functions are total.
  *
  * The parent window is divided into `subWindowCount` sub-windows of equal
  * duration. Sub-window boundaries are epoch-aligned so every service instance
  * independently computes identical window boundaries
  *
  * ==Clock skew==
  * Boundaries are identical across tasks, but each task reads its own wall
  * clock, and those clocks disagree by the skew between tasks (or by a step on
  * one task). The state is shared, so a sub-window may hold counts that another
  * task wrote from a clock ahead of this one. Those counts are live: they sit
  * inside every window that ends after them. This used to sum only the
  * `subWindowCount` starts derived from the local clock and prune everything
  * else, so a trailing task ignored, then erased, the newest counts of a
  * leading one, and admitted up to capacity again within one window.
  *
  *   - A count is live if its sub-window starts at or after the oldest active
  *     one, however far ahead of the local clock.
  *   - Pruning keeps one extra window of history, so a task ahead by up to one
  *     window cannot erase counts that a trailing task still has in its window.
  *     The item holds at most about `2 * subWindowCount` entries for skew
  *     within a window.
  *
  * ==Key invariants==
  *   - `currentSubWindowStart` is always <= nowMs.
  *   - `activeSubWindowStarts` always has exactly `subWindowCount` elements.
  */
object SlidingWindow:

  val DefaultSubWindowCount: Int = 10

  /** Duration of each sub-window in milliseconds. */
  def subWindowDurationMs(windowDurationMs: Long, subWindowCount: Int): Long =
    windowDurationMs / subWindowCount

  /** Epoch-aligned start of the sub-window that contains `nowMs`. */
  def currentSubWindowStart(nowMs: Long, subWindowDurationMs: Long): Long =
    nowMs / subWindowDurationMs * subWindowDurationMs

  /** All active sub-window start timestamps, newest first (index 0 = current
    * The oldest sub-window at index (subWindowCount - 1) expires at
    * `oldest + windowDurationMs`.
    */
  def activeSubWindowStarts(
      nowMs: Long,
      windowDurationMs: Long,
      subWindowCount: Int,
  ): List[Long] =
    val subDuration = subWindowDurationMs(windowDurationMs, subWindowCount)
    val current = currentSubWindowStart(nowMs, subDuration)
    (0 until subWindowCount).map(i => current - i * subDuration).toList

  /** Sum of counts in every live sub-window: the active ones, and any ahead of
    * the local clock (see Clock skew).
    */
  def totalCount(counts: Map[Long, Long], activeStarts: List[Long]): Long =
    val oldest = activeStarts.last
    counts.foldLeft(0L) { case (sum, (sw, c)) =>
      if sw >= oldest then sum + c else sum
    }

  /** Requests remaining in the current window (clamped to >= 0). */
  def remaining(capacity: Int, total: Long): Int = math
    .max(0, capacity - total.toInt)

  /** The instant at which the window total will decrease, i.e., when the oldest
    * live sub-window that holds any counts expires. Used to populate
    * X-RateLimit-Reset and Retry-After.
    */
  def resetAt(
      counts: Map[Long, Long],
      activeStarts: List[Long],
      windowDurationMs: Long,
  ): Instant =
    val oldest = activeStarts.last
    // The oldest counted live sub-window expires first.
    counts.collect { case (sw, c) if sw >= oldest && c > 0 => sw }.minOption
      .map(sw => Instant.ofEpochMilli(sw + windowDurationMs)).getOrElse(
        // No counts yet -- reset is at end of current sub-window window.
        Instant.ofEpochMilli(activeStarts.head + windowDurationMs),
      )

  /** Retry-After seconds (min 1) from now to the reset instant. */
  def retryAfterSeconds(resetAt: Instant, nowMs: Long): Int = math
    .max(1, ((resetAt.toEpochMilli - nowMs) / 1000).toInt)

  /** Remove counts a full window older than the oldest active sub-window. Keeps
    * the DynamoDB item bounded, and never removes a count that a task up to one
    * window behind this one still counts (see Clock skew).
    */
  def pruneStale(
      counts: Map[Long, Long],
      activeStarts: List[Long],
      windowDurationMs: Long,
  ): Map[Long, Long] =
    val keepFrom = activeStarts.last - windowDurationMs
    counts.filter { case (sw, _) => sw >= keepFrom }

/** Persisted state for one rate-limited key in DynamoDB. */
case class SlidingWindowState(
    counts: Map[Long, Long], // sub_window_start_ms -> request count
    version: Long,
)
