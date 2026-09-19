package core

import scala.concurrent.duration.*

import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.noop.NoOpLogger

import cats.effect.*
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.testkit.TestControl
import cats.syntax.all.*
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

  def service(
      config: TokenQuotaConfig = defaultConfig,
      store: Option[TokenQuotaStore[IO]] = None,
  ): IO[(TokenQuotaService[IO], TokenQuotaStore[IO])] = store
    .fold(TokenQuotaStore.inMemory[IO])(IO.pure).map(s =>
      (TokenQuotaService[IO](s, config, MetricsPublisher.noop[IO], summon), s),
    )

  def userPk(id: String): String =
    s"user:$id:${defaultConfig.userWindowSeconds}s"

  def orgPk(id: String, config: TokenQuotaConfig): String =
    s"org:$id:${config.orgWindowSeconds}s"

  /** A store that loses every conditional write. */
  def contendedStore(attempts: Int): TokenQuotaStore[IO] =
    new TokenQuotaStore[IO]:
      def getQuota(pk: String): IO[Option[TokenQuotaState]] = IO.pure(None)
      def reserve(
          targets: List[QuotaTarget],
          inputDelta: Long,
          outputDelta: Long,
          nowMs: Long,
      ): IO[ReserveOutcome] = IO.pure(ReserveOutcome.Contended(attempts))
      def healthCheck: IO[Either[String, Unit]] = IO.pure(Right(()))

  "TokenQuotaService admitted-token metrics" - {

    def withProm(
        f: TokenQuotaService[IO] => IO[Unit],
    ): IO[observability.PrometheusMetrics[IO]] =
      for
        prom <- observability.PrometheusMetrics[IO]
        store <- TokenQuotaStore.inMemory[IO]
        metrics = observability.PrometheusMetrics
          .dual(MetricsPublisher.noop[IO], prom)
        _ <- f(TokenQuotaService[IO](store, defaultConfig, metrics, summon))
      yield prom

    def admitted(prom: observability.PrometheusMetrics[IO], level: String) =
      prom.registry.getSampleValue(
        "gate_quota_tokens_admitted_total",
        Array("level"),
        Array(level),
      ).doubleValue

    "an admitted check counts its estimate at every level it reserved" in
      withProm(
        _.checkQuota(QuotaIdentifier("u", Some("a"), Some("o")), 1000, 500).void,
      ).asserting(prom =>
        List("user", "agent", "org").map(admitted(prom, _)) shouldBe
          List(1500.0, 1500.0, 1500.0),
      )

    "a refused check counts nothing" in
      withProm(_.checkQuota(QuotaIdentifier("u"), 50_000, 0).void)
        .asserting(prom => admitted(prom, "user") shouldBe 0.0)
  }

  "TokenQuotaService.checkQuota" - {

    "allows a request under the limit and reports remaining tokens per level" in {
      for
        (svc, _) <- service()
        result <- svc.checkQuota(QuotaIdentifier("user1"), 1000, 500)
      yield result shouldBe QuotaDecision.Available(Map(QuotaLevel.User -> 8500L))
    }

    "rejects once the window usage would exceed the limit" in {
      for
        (svc, _) <- service()
        _ <- svc.checkQuota(QuotaIdentifier("user1"), 8000, 0)
        result <- svc.checkQuota(QuotaIdentifier("user1"), 5000, 0)
      yield result match
        case QuotaDecision.Exceeded(level, limit, used, _) =>
          level shouldBe QuotaLevel.User
          limit shouldBe 10_000L
          used shouldBe 8000L
        case other => fail(s"expected Exceeded, got $other")
    }

    "does not record a rejected request" in {
      for
        (svc, store) <- service()
        _ <- svc.checkQuota(QuotaIdentifier("user1"), 8000, 0)
        _ <- svc.checkQuota(QuotaIdentifier("user1"), 5000, 0)
        state <- store.getQuota(userPk("user1"))
      yield state.map(_.totalTokens) shouldBe Some(8000L)
    }

    "reports Retry-After as the seconds left in the window, not the window length" in {
      val program =
        for
          (svc, _) <- service()
          _ <- svc.checkQuota(QuotaIdentifier("user1"), 8000, 0)
          _ <- IO.sleep(59.minutes)
          result <- svc.checkQuota(QuotaIdentifier("user1"), 5000, 0)
        yield result

      TestControl.executeEmbed(program).asserting {
        case QuotaDecision.Exceeded(_, _, _, retryAfter) => retryAfter shouldBe
            60
        case other => fail(s"expected Exceeded, got $other")
      }
    }

    "starts a fresh window once the previous one lapses" in {
      val program =
        for
          (svc, _) <- service()
          _ <- svc.checkQuota(QuotaIdentifier("user1"), 8000, 0)
          _ <- IO.sleep(61.minutes)
          result <- svc.checkQuota(QuotaIdentifier("user1"), 5000, 0)
        yield result

      TestControl.executeEmbed(program).asserting(
        _ shouldBe QuotaDecision.Available(Map(QuotaLevel.User -> 5000L)),
      )
    }

    "caps the agent level at 80% of the user limit" in {
      val config = defaultConfig.copy(agentLimit = 9_000)
      for
        (svc, _) <- service(config)
        result <- svc.checkQuota(
          QuotaIdentifier("user1", agentId = Some("agent1")),
          8500,
          0,
        )
      yield result match
        case QuotaDecision.Exceeded(level, limit, _, _) =>
          level shouldBe QuotaLevel.Agent
          limit shouldBe 8000L
        case other => fail(s"expected Exceeded, got $other")
    }

    "reserves nothing at any level when a later level is exceeded" in {
      val config = defaultConfig.copy(orgLimit = 1_000)
      for
        (svc, store) <- service(config)
        result <- svc
          .checkQuota(QuotaIdentifier("user1", orgId = Some("org1")), 5000, 0)
        user <- store.getQuota(userPk("user1"))
        org <- store.getQuota(orgPk("org1", config))
      yield
        result shouldBe a[QuotaDecision.Exceeded]
        user shouldBe None
        org shouldBe None
    }

    "never admits more than the limit under concurrent checks" in {
      val perRequest = 300L
      for
        (svc, store) <- service()
        decisions <- (1 to 50).toList
          .parTraverse(_ => svc.checkQuota(QuotaIdentifier("hot"), perRequest, 0))
        state <- store.getQuota(userPk("hot"))
      yield
        val admitted = decisions.count(_.isInstanceOf[QuotaDecision.Available])
        admitted shouldBe 33
        state.map(_.totalTokens) shouldBe Some(admitted * perRequest)
    }

    "fails closed when the store cannot win a write" in {
      for
        (svc, _) <- service(store = Some(contendedStore(25)))
        result <- svc.checkQuota(QuotaIdentifier("user1"), 10, 0)
      yield result shouldBe QuotaDecision.Contended(25)
    }
  }

  "TokenQuotaService.reconcile" - {

    "replaces the estimate with actual usage" in {
      for
        (svc, store) <- service()
        _ <- svc.checkQuota(QuotaIdentifier("user1"), 1000, 0)
        result <- svc.reconcile(
          QuotaIdentifier("user1"),
          actualInputTokens = 800,
          actualOutputTokens = 0,
          estimatedInputTokens = 1000,
          estimatedOutputTokens = 0,
        )
        state <- store.getQuota(userPk("user1"))
      yield
        result shouldBe ReconcileResult.Reconciled(-200, 0)
        state.map(_.inputTokens) shouldBe Some(800L)
    }

    "records overshoot past the limit so the next check is rejected" in {
      for
        (svc, store) <- service()
        _ <- svc.checkQuota(QuotaIdentifier("user1"), 9000, 0)
        _ <- svc.reconcile(QuotaIdentifier("user1"), 9000, 3000, 9000, 0)
        state <- store.getQuota(userPk("user1"))
        next <- svc.checkQuota(QuotaIdentifier("user1"), 1, 0)
      yield
        state.map(_.totalTokens) shouldBe Some(12_000L)
        next match
          case QuotaDecision.Exceeded(_, _, used, _) => used shouldBe 12_000L
          case other => fail(s"expected Exceeded, got $other")
    }

    "is a no-op when actual usage equals the estimate" in {
      for
        (svc, store) <- service()
        result <- svc.reconcile(QuotaIdentifier("user1"), 500, 100, 500, 100)
        state <- store.getQuota(userPk("user1"))
      yield
        result shouldBe ReconcileResult.Reconciled(0, 0)
        state shouldBe None
    }

    "surfaces contention instead of dropping the adjustment" in {
      for
        (svc, _) <- service(store = Some(contendedStore(25)))
        result <- svc.reconcile(QuotaIdentifier("user1"), 800, 0, 1000, 0)
      yield result shouldBe ReconcileResult.Contended(25)
    }
  }
