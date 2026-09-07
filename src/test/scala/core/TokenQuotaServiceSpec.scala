package core

import java.time.Instant

import scala.concurrent.duration.*

import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.noop.NoOpLogger

import cats.effect.*
import cats.effect.testing.scalatest.AsyncIOSpec
import config.TokenQuotaConfig
import observability.MetricsPublisher

class TokenQuotaServiceSpec
    extends AsyncFreeSpec with AsyncIOSpec with Matchers:

  given Logger[IO] = NoOpLogger[IO]

  val defaultConfig = TokenQuotaConfig(
    enabled = true,
    userLimit = 10_000,
    userWindowSeconds = 3600,
    agentLimit = 5_000,
    agentWindowSeconds = 3600,
    orgLimit = 100_000,
    orgWindowSeconds = 86400,
  )

  def inMemoryStore
      : IO[(TokenQuotaStore[IO], Ref[IO, Map[String, TokenQuotaState]])] = Ref
    .of[IO, Map[String, TokenQuotaState]](Map.empty).map { ref =>
      val store = new TokenQuotaStore[IO]:
        override def getQuota(pk: String): IO[Option[TokenQuotaState]] = ref.get
          .map(_.get(pk))
        override def incrementQuota(
            pk: String,
            inputDelta: Long,
            outputDelta: Long,
            windowSec: Long,
            nowMs: Long,
        ): IO[Boolean] = ref.modify { m =>
          val current = m.get(pk)
          val withinWindow = current
            .exists(s => nowMs - s.windowStart < windowSec * 1000)
          if withinWindow then
            val s = current.get
            val updated = TokenQuotaState(
              math.max(0, s.inputTokens + inputDelta),
              math.max(0, s.outputTokens + outputDelta),
              s.windowStart,
              s.version + 1,
            )
            (m + (pk -> updated), true)
          else
            val fresh = TokenQuotaState(
              math.max(0, inputDelta),
              math.max(0, outputDelta),
              nowMs,
              1L,
            )
            (m + (pk -> fresh), true)
        }
        override def healthCheck: IO[Either[String, Unit]] = IO.pure(Right(()))
      (store, ref)
    }

  "TokenQuotaService" - {
    "should allow quota when under limit" in {
      for
        (store, _) <- inMemoryStore
        svc = TokenQuotaService[IO](
          store,
          defaultConfig,
          MetricsPublisher.noop[IO],
          summon,
        )
        result <- svc.checkQuota(QuotaIdentifier("user1"), 1000, 500)
      yield result shouldBe a[QuotaDecision.Available]
    }

    "should reject when user quota is exceeded" in {
      for
        (store, _) <- inMemoryStore
        svc = TokenQuotaService[IO](
          store,
          defaultConfig,
          MetricsPublisher.noop[IO],
          summon,
        )
        _ <- svc.checkQuota(QuotaIdentifier("user1"), 8000, 0)
        result <- svc.checkQuota(QuotaIdentifier("user1"), 5000, 0)
      yield result shouldBe a[QuotaDecision.Exceeded]
    }

    "should enforce agent limit at 80% of user limit" in {
      val config = defaultConfig.copy(agentLimit = 9_000) // 80% of 10000 = 8000
      for
        (store, _) <- inMemoryStore
        svc =
          TokenQuotaService[IO](store, config, MetricsPublisher.noop[IO], summon)
        result <- svc.checkQuota(
          QuotaIdentifier("user1", agentId = Some("agent1")),
          8500,
          0,
        )
      yield
        // Agent effective limit = min(9000, 10000*0.8) = 8000, so 8500 exceeds
        result shouldBe a[QuotaDecision.Exceeded]
    }

    "should reconcile actual vs estimated" in {
      for
        (store, ref) <- inMemoryStore
        svc = TokenQuotaService[IO](
          store,
          defaultConfig,
          MetricsPublisher.noop[IO],
          summon,
        )
        _ <- svc.checkQuota(QuotaIdentifier("user1"), 1000, 0)
        _ <- svc.reconcile(
          QuotaIdentifier("user1"),
          actualInputTokens = 800,
          actualOutputTokens = 0,
          estimatedInputTokens = 1000,
          estimatedOutputTokens = 0,
        )
        state <- ref.get
      yield
        // After reconcile, net usage should reflect actual (800), not estimate (1000)
        // The delta is -200 input tokens, so usage goes down by 200
        val userKey = state.keys.find(_.startsWith("user:user1"))
        userKey shouldBe defined
        state(userKey.get).inputTokens shouldBe 800
    }
  }
