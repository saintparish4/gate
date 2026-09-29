package core

import java.time.Instant

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SlidingWindowSpec extends AnyFunSuite with Matchers:

  // Fixed reference time: 1 000 000 ms past epoch, 10 sub-windows of 100 ms
  // within a 1000 ms window.
  private val windowMs = 1000L
  private val subCount = 10
  private val subDurMs = SlidingWindow.subWindowDurationMs(windowMs, subCount) // 100

  // Anchor at the start of a fresh sub-window boundary
  private val nowMs = 1_000_000L
  private val currentSw = SlidingWindow.currentSubWindowStart(nowMs, subDurMs)
  private val activeStarts = SlidingWindow
    .activeSubWindowStarts(nowMs, windowMs, subCount)

  test("subWindowDurationMs divides the window evenly")(subDurMs shouldBe 100L)

  test("currentSubWindowStart is epoch-aligned") {
    currentSw % subDurMs shouldBe 0L
    currentSw should be <= nowMs
    currentSw + subDurMs should be > nowMs
  }

  test("activeSubWindowStarts has exactly subWindowCount entries, newest first") {
    (activeStarts should have).length(subCount)
    activeStarts.head shouldBe currentSw
    activeStarts.last shouldBe currentSw - (subCount - 1) * subDurMs
    activeStarts shouldBe activeStarts.sorted.reverse
  }

  test("totalCount sums only active sub-windows, ignores stale entries") {
    val counts = Map(
      activeStarts.head -> 5L, // current sub-window
      activeStarts(2) -> 3L, // older but still active
      activeStarts.last - subDurMs -> 7L, // stale (outside window)
    )
    SlidingWindow.totalCount(counts, activeStarts) shouldBe 8L
  }

  test("remaining clamps to 0 when over capacity") {
    SlidingWindow.remaining(10, 15L) shouldBe 0
    SlidingWindow.remaining(10, 10L) shouldBe 0
    SlidingWindow.remaining(10, 7L) shouldBe 3
  }

  test("resetAt returns oldest counted sub-window + windowDuration") {
    val oldest = activeStarts.last
    val counts = Map(oldest -> 1L, activeStarts.head -> 2L)
    val expected = Instant.ofEpochMilli(oldest + windowMs)
    SlidingWindow.resetAt(counts, activeStarts, windowMs) shouldBe expected
  }

  test("resetAt with no counts returns current sub-window + windowDuration") {
    val reset = SlidingWindow.resetAt(Map.empty, activeStarts, windowMs)
    reset shouldBe Instant.ofEpochMilli(currentSw + windowMs)
  }

  test("retryAfterSeconds is at least 1") {
    val reset = Instant.ofEpochMilli(nowMs + 500)
    SlidingWindow.retryAfterSeconds(reset, nowMs) shouldBe 1 // 500 ms → rounds to 0 → clamped to 1
    val resetFar = Instant.ofEpochMilli(nowMs + 3500)
    SlidingWindow.retryAfterSeconds(resetFar, nowMs) shouldBe 3
  }

  test("pruneStale keeps one window of history before the active window") {
    val justKept = activeStarts.last - windowMs
    val dropped = justKept - subDurMs
    val counts = Map(activeStarts.head -> 5L, justKept -> 2L, dropped -> 99L)
    val pruned = SlidingWindow.pruneStale(counts, activeStarts, windowMs)
    pruned.contains(dropped) shouldBe false
    pruned.get(justKept) shouldBe Some(2L)
    pruned.get(activeStarts.head) shouldBe Some(5L)
  }

  test("resetAt counts a sub-window ahead of the local clock") {
    val ahead = activeStarts.head + 2 * subDurMs
    val reset = SlidingWindow.resetAt(Map(ahead -> 1L), activeStarts, windowMs)
    reset shouldBe Instant.ofEpochMilli(ahead + windowMs)
  }
