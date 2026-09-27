package core

import org.scalacheck.Gen
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.noop.NoOpLogger

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import config.TokenQuotaConfig
import observability.MetricsPublisher

/** Finding B: reconcile took the estimate from the request, so any caller could
  * erase usage, its own or anyone's. Whatever mix of checks, reconciles,
  * replays, and forgeries a caller sends, each counter must equal what its
  * reservations hold. That means the stored estimate while pending and the
  * first reconciled usage after, so no sequence can push a counter below the
  * usage it has recorded.
  */
class TokenQuotaReconcilePropertySpec
    extends AnyFreeSpec with ScalaCheckPropertyChecks with Matchers:

  given Logger[IO] = NoOpLogger[IO]

  // Large limits so admission is not what is under test; one window.
  private val config = TokenQuotaConfig(
    enabled = true,
    userLimit = Long.MaxValue / 4,
    userWindowSeconds = 3600,
    agentLimit = Long.MaxValue / 8,
    agentWindowSeconds = 3600,
    orgLimit = Long.MaxValue / 4,
    orgWindowSeconds = 3600,
  )

  private val owner = "owner"
  private val user = QuotaIdentifier("u")
  private val userPk = s"user:${TenantKey(owner, "u")}:3600s"

  private enum Op:
    case Check(input: Long, output: Long)
    // Reconcile the n-th reservation made so far (mod the count).
    case Reconcile(n: Int, input: Long, output: Long)
    // The same, sent by another client that learned the ID.
    case Steal(n: Int, input: Long, output: Long)
    case Forge(id: String, input: Long, output: Long)

  private val genTokens = Gen.choose(0L, 2_000L)
  private val genOp: Gen[Op] = Gen.frequency(
    3 -> Gen.zip(genTokens, genTokens).map(Op.Check.apply),
    4 -> Gen.zip(Gen.choose(0, 50), genTokens, genTokens).map(Op.Reconcile.apply),
    1 -> Gen.zip(Gen.choose(0, 50), genTokens, genTokens).map(Op.Steal.apply),
    1 -> Gen.zip(Gen.identifier, genTokens, genTokens).map(Op.Forge.apply),
  )

  // What each reservation holds: its estimate, then its first actual usage.
  private case class Held(
      id: String,
      input: Long,
      output: Long,
      reconciled: Boolean,
  )

  private def run(ops: List[Op]): (TokenQuotaState, List[Held]) =
    val program =
      for
        store <- TokenQuotaStore.inMemory[IO]
        svc = TokenQuotaService[IO](store, config, MetricsPublisher.noop, summon)
        held <- ops.foldLeftM(Vector.empty[Held]) { (held, op) =>
          def nth(n: Int) = held(n % held.size)
          op match
            case Op.Check(in, out) => svc.checkQuota(owner, user, in, out).map {
                case QuotaDecision.Available(_, id) => held :+
                    Held(id, in, out, reconciled = false)
                case other => fail(s"limits are not under test: $other")
              }
            case Op.Reconcile(n, in, out) if held.nonEmpty =>
              val i = n % held.size
              val h = held(i)
              svc.reconcile(owner, h.id, in, out).map {
                // The first reconcile replaces the estimate with the usage.
                case ReconcileResult.Reconciled(_, _) if !h.reconciled =>
                  held.updated(i, Held(h.id, in, out, reconciled = true))
                // A replay with the same usage, or a conflicting one: no change.
                case ReconcileResult.Reconciled(_, _) | ReconcileResult
                      .Conflict(_) if h.reconciled => held
                case other => fail(s"unexpected reconcile result: $other")
              }
            case Op.Steal(n, in, out) if held.nonEmpty =>
              svc.reconcile("intruder", nth(n).id, in, out).map {
                case ReconcileResult.NotFound => held
                case other => fail(s"a stolen ID reconciled: $other")
              }
            case Op.Forge(id, in, out) => svc.reconcile(owner, id, in, out)
                .map {
                  case ReconcileResult.NotFound => held
                  case other => fail(s"a forged ID reconciled: $other")
                }
            case _ => IO.pure(held)
        }
        state <- store.getQuota(userPk)
      yield (state.getOrElse(TokenQuotaState(0, 0, 0, 0)), held.toList)
    program.unsafeRunSync()

  "no sequence of checks, reconciles, replays, or forgeries moves a counter off what its reservations hold" in
    forAll(Gen.listOfN(60, genOp), minSuccessful(200)) { ops =>
      val (state, held) = run(ops)
      state.inputTokens shouldBe held.map(_.input).sum
      state.outputTokens shouldBe held.map(_.output).sum
    }
