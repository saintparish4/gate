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

  // Every call in this spec runs as one client; the cross-client cases name
  // their own.
  val client = "client-1"

  def userPk(id: String, clientId: String = client): String =
    s"user:${TenantKey(clientId, id)}:${defaultConfig.userWindowSeconds}s"

  def orgPk(id: String, config: TokenQuotaConfig): String =
    s"org:${TenantKey(client, id)}:${config.orgWindowSeconds}s"

  /** A store that loses every conditional write. */
  def contendedStore(attempts: Int): TokenQuotaStore[IO] =
    new TokenQuotaStore[IO]:
      def getQuota(pk: String): IO[Option[TokenQuotaState]] = IO.pure(None)
      def reserve(
          targets: List[QuotaTarget],
          inputDelta: Long,
          outputDelta: Long,
          nowMs: Long,
          reservation: Option[NewReservation],
      ): IO[ReserveOutcome] = IO.pure(ReserveOutcome.Contended(attempts))
      def reconcile(
          reservationPk: String,
          actualInput: Long,
          actualOutput: Long,
          nowMs: Long,
      ): IO[ReconcileOutcome] = IO.pure(ReconcileOutcome.Contended(attempts))
      def healthCheck: IO[Either[String, Unit]] = IO.pure(Right(()))

  /** Remaining tokens per level, if admitted. */
  def remaining(decision: QuotaDecision): Option[Map[QuotaLevel, Long]] =
    decision match
      case QuotaDecision.Available(byLevel, _) => Some(byLevel)
      case _ => None

  /** The reservation an admitted check made. */
  def reservationOf(decision: QuotaDecision): IO[String] = decision match
    case QuotaDecision.Available(_, id) => IO.pure(id)
    case other => IO
        .raiseError(new AssertionError(s"expected Available, got $other"))

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
        _.checkQuota(
          client,
          QuotaIdentifier("u", Some("a"), Some("o")),
          1000,
          500,
        ).void,
      ).asserting(prom =>
        List("user", "agent", "org").map(admitted(prom, _)) shouldBe
          List(1500.0, 1500.0, 1500.0),
      )

    "a refused check counts nothing" in
      withProm(_.checkQuota(client, QuotaIdentifier("u"), 50_000, 0).void)
        .asserting(prom => admitted(prom, "user") shouldBe 0.0)
  }

  "TokenQuotaService.checkQuota" - {

    "allows a request under the limit and reports remaining tokens per level" in {
      for
        (svc, _) <- service()
        result <- svc.checkQuota(client, QuotaIdentifier("user1"), 1000, 500)
      yield remaining(result) shouldBe Some(Map(QuotaLevel.User -> 8500L))
    }

    "rejects once the window usage would exceed the limit" in {
      for
        (svc, _) <- service()
        _ <- svc.checkQuota(client, QuotaIdentifier("user1"), 8000, 0)
        result <- svc.checkQuota(client, QuotaIdentifier("user1"), 5000, 0)
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
        _ <- svc.checkQuota(client, QuotaIdentifier("user1"), 8000, 0)
        _ <- svc.checkQuota(client, QuotaIdentifier("user1"), 5000, 0)
        state <- store.getQuota(userPk("user1"))
      yield state.map(_.totalTokens) shouldBe Some(8000L)
    }

    "reports Retry-After as the seconds left in the window, not the window length" in {
      val program =
        for
          (svc, _) <- service()
          _ <- svc.checkQuota(client, QuotaIdentifier("user1"), 8000, 0)
          _ <- IO.sleep(59.minutes)
          result <- svc.checkQuota(client, QuotaIdentifier("user1"), 5000, 0)
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
          _ <- svc.checkQuota(client, QuotaIdentifier("user1"), 8000, 0)
          _ <- IO.sleep(61.minutes)
          result <- svc.checkQuota(client, QuotaIdentifier("user1"), 5000, 0)
        yield result

      TestControl.executeEmbed(program)
        .asserting(remaining(_) shouldBe Some(Map(QuotaLevel.User -> 5000L)))
    }

    "caps the agent level at 80% of the user limit" in {
      val config = defaultConfig.copy(agentLimit = 9_000)
      for
        (svc, _) <- service(config)
        result <- svc.checkQuota(
          client,
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
        result <- svc.checkQuota(
          client,
          QuotaIdentifier("user1", orgId = Some("org1")),
          5000,
          0,
        )
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
        decisions <- (1 to 50).toList.parTraverse(_ =>
          svc.checkQuota(client, QuotaIdentifier("hot"), perRequest, 0),
        )
        state <- store.getQuota(userPk("hot"))
      yield
        val admitted = decisions.count(_.isInstanceOf[QuotaDecision.Available])
        admitted shouldBe 33
        state.map(_.totalTokens) shouldBe Some(admitted * perRequest)
    }

    "fails closed when the store cannot win a write" in {
      for
        (svc, _) <- service(store = Some(contendedStore(25)))
        result <- svc.checkQuota(client, QuotaIdentifier("user1"), 10, 0)
      yield result shouldBe QuotaDecision.Contended(25)
    }
  }

  "TokenQuotaService.reconcile" - {

    "replaces the stored estimate with actual usage" in {
      for
        (svc, store) <- service()
        id <- svc.checkQuota(client, QuotaIdentifier("user1"), 1000, 0)
          .flatMap(reservationOf)
        result <- svc.reconcile(client, id, 800, 0)
        state <- store.getQuota(userPk("user1"))
      yield
        result shouldBe ReconcileResult.Reconciled(-200, 0)
        state.map(_.inputTokens) shouldBe Some(800L)
    }

    "records overshoot past the limit so the next check is rejected" in {
      for
        (svc, store) <- service()
        id <- svc.checkQuota(client, QuotaIdentifier("user1"), 9000, 0)
          .flatMap(reservationOf)
        _ <- svc.reconcile(client, id, 9000, 3000)
        state <- store.getQuota(userPk("user1"))
        next <- svc.checkQuota(client, QuotaIdentifier("user1"), 1, 0)
      yield
        state.map(_.totalTokens) shouldBe Some(12_000L)
        next match
          case QuotaDecision.Exceeded(_, _, used, _) => used shouldBe 12_000L
          case other => fail(s"expected Exceeded, got $other")
    }

    "applies once: a replay with the same usage changes nothing" in {
      for
        (svc, store) <- service()
        id <- svc.checkQuota(client, QuotaIdentifier("user1"), 1000, 0)
          .flatMap(reservationOf)
        first <- svc.reconcile(client, id, 0, 0)
        replay <- svc.reconcile(client, id, 0, 0)
        state <- store.getQuota(userPk("user1"))
      yield
        first shouldBe ReconcileResult.Reconciled(-1000, 0)
        replay shouldBe first
        state.map(_.totalTokens) shouldBe Some(0L)
    }

    "refuses a second reconcile with different usage" in {
      for
        (svc, store) <- service()
        id <- svc.checkQuota(client, QuotaIdentifier("user1"), 1000, 0)
          .flatMap(reservationOf)
        _ <- svc.reconcile(client, id, 700, 0)
        second <- svc.reconcile(client, id, 0, 0)
        state <- store.getQuota(userPk("user1"))
      yield
        second shouldBe ReconcileResult.Conflict(ReconciledUsage(700, 0))
        state.map(_.totalTokens) shouldBe Some(700L)
    }

    // Finding B: reconcile took the estimate from the request, so actual = 0
    // with a huge estimate zeroed every counter, other reservations included.
    "can only give back the reservation's own estimate" in {
      for
        (svc, store) <- service()
        _ <- svc.checkQuota(client, QuotaIdentifier("user1"), 6000, 0)
        small <- svc.checkQuota(client, QuotaIdentifier("user1"), 1000, 0)
          .flatMap(reservationOf)
        _ <- svc.reconcile(client, small, 0, 0)
        state <- store.getQuota(userPk("user1"))
      yield state.map(_.totalTokens) shouldBe Some(6000L)
    }

    "answers NotFound for an unknown reservation and writes nothing" in {
      for
        (svc, store) <- service()
        result <- svc.reconcile(client, "no-such-reservation", 0, 0)
        state <- store.getQuota(userPk("user1"))
      yield
        result shouldBe ReconcileResult.NotFound
        state shouldBe None
    }

    "answers NotFound once the reservation has expired" in {
      val config = defaultConfig.copy(reservationTtlSeconds = 60)
      val program =
        for
          (svc, store) <- service(config)
          id <- svc.checkQuota(client, QuotaIdentifier("user1"), 1000, 0)
            .flatMap(reservationOf)
          _ <- IO.sleep(61.seconds)
          result <- svc.reconcile(client, id, 0, 0)
          state <- store.getQuota(userPk("user1"))
        yield (result, state.map(_.totalTokens))
      TestControl.executeEmbed(program)
        .asserting(_ shouldBe (ReconcileResult.NotFound, Some(1000L)))
    }

    "after the window rolls over, records overage but never refunds" in {
      // The estimate was charged to a window that has since ended, so a
      // refund would come out of other reservations' usage in the new one.
      // The reservation has to outlive the window for this to arise.
      val config = defaultConfig.copy(reservationTtlSeconds = 7200)
      val program =
        for
          (svc, store) <- service(config)
          under <- svc.checkQuota(client, QuotaIdentifier("user1"), 1000, 0)
            .flatMap(reservationOf)
          over <- svc.checkQuota(client, QuotaIdentifier("user1"), 1000, 0)
            .flatMap(reservationOf)
          _ <- IO.sleep(3601.seconds)
          _ <- svc.checkQuota(client, QuotaIdentifier("user1"), 500, 0)
          _ <- svc.reconcile(client, under, 0, 0)
          afterRefund <- store.getQuota(userPk("user1"))
          _ <- svc.reconcile(client, over, 1300, 0)
          afterOverage <- store.getQuota(userPk("user1"))
        yield (afterRefund.map(_.totalTokens), afterOverage.map(_.totalTokens))
      TestControl.executeEmbed(program)
        .asserting(_ shouldBe (Some(500L), Some(800L)))
    }

    "surfaces contention instead of dropping the adjustment" in {
      for
        (svc, _) <- service(store = Some(contendedStore(25)))
        result <- svc.reconcile(client, "some-reservation", 800, 0)
      yield result shouldBe ReconcileResult.Contended(25)
    }
  }

  "tenant isolation" - {
    // Counters were keyed by the caller's user ID alone, so two clients naming
    // the same user shared one quota, and either could reconcile the other's.

    "two clients naming the same user meter separate counters" in {
      for
        (svc, store) <- service()
        mine <- svc.checkQuota("client-a", QuotaIdentifier("shared"), 10_000, 0)
        full <- svc.checkQuota("client-a", QuotaIdentifier("shared"), 1, 0)
        theirs <- svc.checkQuota("client-b", QuotaIdentifier("shared"), 1, 0)
        counterA <- store.getQuota(userPk("shared", "client-a"))
        counterB <- store.getQuota(userPk("shared", "client-b"))
      yield
        mine shouldBe a[QuotaDecision.Available]
        full shouldBe a[QuotaDecision.Exceeded]
        remaining(theirs) shouldBe Some(Map(QuotaLevel.User -> 9_999L))
        counterA.map(_.totalTokens) shouldBe Some(10_000L)
        counterB.map(_.totalTokens) shouldBe Some(1L)
    }

    "another client cannot reconcile a reservation, even knowing its ID" in {
      for
        (svc, store) <- service()
        id <- svc.checkQuota("client-a", QuotaIdentifier("shared"), 9_000, 0)
          .flatMap(reservationOf)
        stolen <- svc.reconcile("client-b", id, 0, 0)
        counterA <- store.getQuota(userPk("shared", "client-a"))
        own <- svc.reconcile("client-a", id, 9_000, 0)
      yield
        stolen shouldBe ReconcileResult.NotFound
        counterA.map(_.totalTokens) shouldBe Some(9_000L)
        own shouldBe ReconcileResult.Reconciled(0, 0)
    }
  }
