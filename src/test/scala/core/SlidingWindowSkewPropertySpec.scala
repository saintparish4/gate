package core

import org.scalacheck.Gen
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

/** Two tasks share one sliding-window key, and their clocks disagree.
  *
  * The step below is DynamoDBSlidingWindowStore.singleAttempt without the OCC
  * write: the write is conditioned on the version read, so the sequence of
  * committed attempts is a serial history like this one. Each admission is
  * booked in the sub-window its task wrote it to. However the clocks disagree,
  * no run of `subWindowCount` consecutive sub-windows may hold more than
  * capacity. Summing only the starts derived from the local clock, and pruning
  * the rest, broke this: the trailing task ignored and erased the leading
  * task's newest counts, and admitted capacity again.
  */
class SlidingWindowSkewPropertySpec
    extends AnyFreeSpec with ScalaCheckPropertyChecks with Matchers:

  private val windowMs = 1000L
  private val subCount = 10
  private val subMs = SlidingWindow.subWindowDurationMs(windowMs, subCount)

  private final case class Request(leading: Boolean, advanceMs: Long)

  private val genRequest: Gen[Request] =
    for
      leading <- Gen.oneOf(true, false)
      advance <- Gen.frequency(4 -> Gen.const(0L), 6 -> Gen.choose(1L, 150L))
    yield Request(leading, advance)

  /** Returns the admissions booked per sub-window start. */
  private def run(
      capacity: Int,
      skewMs: Long,
      startMs: Long,
      requests: List[Request],
  ): Map[Long, Long] =
    val (_, _, booked) = requests.foldLeft(
      (startMs, Map.empty[Long, Long], Map.empty[Long, Long]),
    ) { case ((trueMs, counts, booked), req) =>
      val t = trueMs + req.advanceMs
      val nowMs = if req.leading then t else t - skewMs
      val active = SlidingWindow.activeSubWindowStarts(nowMs, windowMs, subCount)
      val total = SlidingWindow.totalCount(counts, active)
      if total + 1 > capacity then (t, counts, booked)
      else
        val cur = active.head
        val updated = counts.updated(cur, counts.getOrElse(cur, 0L) + 1)
        (
          t,
          SlidingWindow.pruneStale(updated, active, windowMs),
          booked.updated(cur, booked.getOrElse(cur, 0L) + 1),
        )
    }
    booked

  /** The most admissions booked in any `subCount` consecutive sub-windows. */
  private def worstWindow(booked: Map[Long, Long]): Long = booked.keys
    .map(from =>
      booked.collect { case (sw, c) if sw >= from && sw < from + windowMs => c }
        .sum,
    ).maxOption.getOrElse(0L)

  "two skewed clocks on one key never admit more than capacity per window" in {
    val gen =
      for
        capacity <- Gen.choose(1, 20)
        // Skew up to one full window, which pruning is sized to tolerate.
        skew <- Gen.choose(0L, windowMs)
        start <- Gen.choose(1_000_000L, 1_001_000L)
        n <- Gen.choose(20, 300)
        requests <- Gen.listOfN(n, genRequest)
      yield (capacity, skew, start, requests)

    forAll(gen, minSuccessful(500)) { case (capacity, skew, start, requests) =>
      val booked = run(capacity, skew, start, requests)
      withClue(s"capacity=$capacity skew=${skew}ms booked=$booked: ")(
        worstWindow(booked) should be <= capacity.toLong,
      )
    }
  }

  "a count ahead of the local clock is counted and survives pruning" in {
    val nowMs = 1_000_000L
    val active = SlidingWindow.activeSubWindowStarts(nowMs, windowMs, subCount)
    val ahead = active.head + 3 * subMs
    val counts = Map(ahead -> 4L, active.head -> 1L)

    SlidingWindow.totalCount(counts, active) shouldBe 5L
    SlidingWindow.pruneStale(counts, active, windowMs) shouldBe counts
  }
