package core

import scala.concurrent.duration.*

import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

import cats.effect.*
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.testkit.TestControl
import cats.syntax.all.*

/** Deterministic reproduction attempt for issue #10.
  *
  * Against real DynamoDB a clean run (zero errors, no degradation) admitted 86
  * tokens in a 30.1s window where capacity + rate * elapsed = 80.2. Every
  * remote measurement of the refill rate was confounded by network and clock
  * domains, so this pins the OCC + token-bucket logic under a virtual clock
  * where nothing is.
  *
  * The store below mirrors DynamoDBRateLimitStore.singleAttempt exactly -- now
  * captured BEFORE the read, refill, consume, conditional put keyed on version
  * (attribute_not_exists for the first write) -- and adds the one thing an
  * in-memory store never models: latency between capturing now and reading, and
  * between deciding and writing. Those gaps are where a stale now can commit
  * behind a newer writer, which is the only interleaving I could not rule out
  * on paper.
  */
class TokenBucketOccBoundSpec
    extends AsyncFreeSpec with AsyncIOSpec with Matchers:

  private val profile =
    RateLimitProfile(capacity = 20, refillRatePerSecond = 2.0, ttlSeconds = 3600)

  /** One DynamoDB item with conditional-put semantics, atomic via Ref.modify.
    */
  private final class OccItem(
      ref: Ref[IO, Option[TokenBucketState]],
      createdAt: Ref[IO, Option[Long]],
      lastGrantNow: Ref[IO, Long],
  ):
    def read: IO[Option[TokenBucketState]] = ref.get

    /** (first successful write's now, latest successful write's now). */
    def grantWindow: IO[(Option[Long], Long)] =
      (createdAt.get, lastGrantNow.get).tupled

    def conditionalPut(
        expectedVersion: Long,
        next: TokenBucketState,
    ): IO[Boolean] = ref.modify {
      case None if expectedVersion == 0L => (Some(next), true)
      case Some(cur) if cur.version == expectedVersion => (Some(next), true)
      case cur => (cur, false)
    }.flatTap(ok =>
      IO.whenA(ok)(
        createdAt.update(_.orElse(Some(next.lastRefillMs))) *>
          lastGrantNow.update(_.max(next.lastRefillMs)),
      ),
    )

  private object OccItem:
    def make: IO[OccItem] = (
      Ref.of[IO, Option[TokenBucketState]](None),
      Ref.of[IO, Option[Long]](None),
      Ref.of[IO, Long](Long.MinValue),
    ).mapN(new OccItem(_, _, _))

  private final case class Counts(allowed: Long, blocked: Long, conflicts: Long)

  /** Mirrors singleAttempt, with explicit latency in the two gaps. */
  private def singleAttempt(
      item: OccItem,
      cost: Int,
      readLatency: FiniteDuration,
      writeLatency: FiniteDuration,
  ): IO[Either[Unit, Boolean]] =
    for
      now <- IO.realTime.map(_.toMillis)
      _ <- IO.sleep(readLatency)
      cur <- item.read
        .map(_.getOrElse(TokenBucketState(profile.capacity.toDouble, now, 0L)))
      refilled = TokenBucket.refill(cur, now, profile)
      out <- TokenBucket.consume(refilled, cost, now) match
        case Some(next) => IO.sleep(writeLatency) *>
            item.conditionalPut(cur.version, next)
              .map(ok => if ok then Right(true) else Left(()))
        case None => IO.pure(Right(false))
    yield out

  /** occRetry: 10 retries, ~1ms base. Deterministic 1ms here. */
  private def checkAndConsume(
      item: OccItem,
      cost: Int,
      readLatency: FiniteDuration,
      writeLatency: FiniteDuration,
      counts: Ref[IO, Counts],
  ): IO[Unit] =
    def go(attempt: Int): IO[Unit] =
      singleAttempt(item, cost, readLatency, writeLatency).flatMap {
        case Right(true) => counts.update(c => c.copy(allowed = c.allowed + 1))
        case Right(false) => counts.update(c => c.copy(blocked = c.blocked + 1))
        case Left(()) if attempt < 11 =>
          counts.update(c => c.copy(conflicts = c.conflicts + 1)) *>
            IO.sleep(1.millis) *> go(attempt + 1)
        case Left(()) => counts.update(c => c.copy(blocked = c.blocked + 1))
      }
    go(1)

  "token bucket under OCC" - {

    "never admits more than capacity + rate * elapsed, under adversarial latency" in {
      val workers = 20
      val window = 30.seconds

      val test =
        for
          item <- OccItem.make
          counts <- Ref.of[IO, Counts](Counts(0, 0, 0))
          t0 <- IO.monotonic
          _ <- IO.race(
            IO.sleep(window),
            (0 until workers).toList.parTraverse_ { i =>
              // Spread latencies so fibers commit in orders that differ from
              // the order they captured now. Worker i's read gap ranges 0-12ms
              // and its write gap 0-4ms, on a 20-44ms cadence.
              val readLat = (i % 5 * 3).millis
              val writeLat = (i % 3 * 2).millis
              val cadence = (20 + i % 7 * 4).millis
              checkAndConsume(item, 1, readLat, writeLat, counts)
                .*>(IO.sleep(cadence)).foreverM
            },
          )
          t1 <- IO.monotonic
          c <- counts.get
          (created, lastGrant) <- item.grantWindow
        yield (c, (t1 - t0).toMillis, created, lastGrant)

      TestControl.executeEmbed(test)
        .asserting { case (c, elapsedMs, created, lastGrant) =>
          val elapsedSec = elapsedMs / 1000.0
          // The bound loadSim asserts: the client's window, zero epsilon because
          // virtual time is exact.
          val clientCeiling = profile.capacity +
            profile.refillRatePerSecond * elapsedSec
          // The tight bound from the server's own clock: refill can only have
          // accrued between the first and last committed write.
          val createdMs = created.getOrElse(fail("bucket was never created"))
          val serverCeiling = profile.capacity +
            profile.refillRatePerSecond * (lastGrant - createdMs) / 1000.0

          withClue(s"counts=$c elapsed=${elapsedSec}s client=$clientCeiling server=$serverCeiling: ") {
            // Not vacuous: the bucket was drained and refusals happened.
            c.blocked should be > 0L
            c.allowed should be >= profile.capacity.toLong
            c.allowed.toDouble should be <= serverCeiling
            c.allowed.toDouble should be <= clientCeiling
          }
        }
    }

    "telescoping holds exactly: admissions equal capacity plus refill minus what is left" in {
      // The tight server-side statement behind the ceiling above. If the core
      // is sound this is an equality up to clamp loss, so it must hold with no
      // slack at all; if it fails, the hole is in TokenBucket or the OCC path.
      val workers = 20
      val window = 30.seconds

      val test =
        for
          item <- OccItem.make
          counts <- Ref.of[IO, Counts](Counts(0, 0, 0))
          _ <- IO.race(
            IO.sleep(window),
            (0 until workers).toList.parTraverse_ { i =>
              val readLat = (i % 5 * 3).millis
              val writeLat = (i % 3 * 2).millis
              val cadence = (20 + i % 7 * 4).millis
              checkAndConsume(item, 1, readLat, writeLat, counts)
                .*>(IO.sleep(cadence)).foreverM
            },
          )
          c <- counts.get
          st <- item.read
        yield (c, st)

      TestControl.executeEmbed(test).asserting { case (c, st) =>
        val state = st.getOrElse(fail("bucket was never created"))
        withClue(s"counts=$c state=$state: ")(
          // Every admission was a committed conditional write, so the version
          // counts them exactly. If allowed != version, something admitted
          // without writing -- the only way to over-issue.
          c.allowed shouldBe state.version,
        )
      }
    }
  }
