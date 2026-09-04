package core

import org.scalacheck.Gen
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class TokenQuotaReservePropertySpec
    extends AnyFreeSpec with ScalaCheckPropertyChecks with Matchers:

  val window = 3600L

  val genLimit: Gen[Long] = Gen.choose(1L, 5_000L)
  val genDelta: Gen[Long] = Gen.choose(-500L, 1_000L)
  val genState: Gen[TokenQuotaState] =
    for
      in <- Gen.choose(0L, 10_000L)
      out <- Gen.choose(0L, 10_000L)
      start <- Gen.choose(0L, 1_000_000_000L)
      version <- Gen.choose(1L, 1_000_000L)
    yield TokenQuotaState(in, out, start, version)

  private def target(pk: String, limit: Option[Long]): QuotaTarget =
    QuotaTarget(pk, window, limit)

  "QuotaReservation.plan" - {

    "never admits a sequence past the limit, and every rejection is justified" in
      forAll(genLimit, Gen.listOf(Gen.choose(1L, 1_000L))) { (limit, deltas) =>
        val t = target("user:p:3600s", Some(limit))
        val (finalState, consistent) = deltas
          .foldLeft((Map.empty[String, TokenQuotaState], true)) {
            case ((state, ok), delta) => QuotaReservation
                .plan(List(t), state, delta, 0, nowMs = 1_000L) match
                case Right(planned) =>
                  val after = planned.head.after
                  (state + (t.pk -> after), ok && after.totalTokens <= limit)
                case Left(ReserveOutcome.LimitExceeded(_, used, _)) =>
                  (state, ok && used + delta > limit)
          }
        consistent shouldBe true
        finalState.get(t.pk).map(_.totalTokens).getOrElse(0L) should be <= limit
      }

    "is all-or-nothing across targets" in
      forAll(genLimit, genLimit, Gen.choose(1L, 10_000L)) {
        (limitA, limitB, delta) =>
          val targets = List(target("a", Some(limitA)), target("b", Some(limitB)))
          QuotaReservation.plan(targets, Map.empty, delta, 0, 1_000L) match
            case Right(planned) =>
              planned.map(_.target.pk) shouldBe List("a", "b")
              delta should be <= math.min(limitA, limitB)
            case Left(_) => delta should be > math.min(limitA, limitB)
      }
  }

  "TokenQuotaState.next" - {

    "never produces negative counters and always advances the version" in forAll(
      Gen.option(genState),
      genDelta,
      genDelta,
      Gen.choose(0L, 10_000_000L),
    ) { (current, in, out, nowMs) =>
      val n = TokenQuotaState.next(current, in, out, window, nowMs)
      n.inputTokens should be >= 0L
      n.outputTokens should be >= 0L
      n.version shouldBe current.map(_.version + 1).getOrElse(1L)
    }

    "keeps the window start inside the window and resets it once lapsed" in
      forAll(genState, Gen.choose(0L, 2 * window * 1000)) {
        (current, elapsedMs) =>
          val nowMs = current.windowStart + elapsedMs
          val n = TokenQuotaState.next(Some(current), 1, 0, window, nowMs)
          if elapsedMs < window * 1000 then
            n.windowStart shouldBe current.windowStart
          else n.windowStart shouldBe nowMs
      }
  }
