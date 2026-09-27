package integration

import scala.jdk.CollectionConverters.*

import org.scalatest.BeforeAndAfterEach
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import software.amazon.awssdk.services.dynamodb.model.{
  AttributeValue, PutItemRequest,
}
import core.{
  NewReservation, QuotaTarget, ReconcileOutcome, ReconciledUsage, ReserveOutcome,
  TokenQuotaState,
}
import storage.DynamoDBTokenQuotaStore
import observability.MetricsPublisher
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all.*

/** Integration tests for DynamoDBTokenQuotaStore against LocalStack.
  *
  * Covers window creation, accumulation, rollover, limit enforcement inside the
  * conditional write, all-or-nothing multi-target reservations, and behaviour
  * under concurrent writers. Requires Docker. Without Docker, run unit tests
  * only: sbt unitTest
  */
@Integration
class DynamoDBTokenQuotaStoreIntegrationSpec
    extends AsyncFreeSpec
    with AsyncIOSpec
    with Matchers
    with LocalStackIntegrationSpec
    with BeforeAndAfterEach {

  implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  val tokenQuotaTableName = "test-token-quotas"
  val windowSec = 3600L

  override protected def setupResources(): Unit = {
    super.setupResources()
    createDynamoDBTable(tokenQuotaTableName)
  }

  lazy val store: DynamoDBTokenQuotaStore[IO] = DynamoDBTokenQuotaStore[IO](
    dynamoDbClient,
    tokenQuotaTableName,
    logger,
    MetricsPublisher.noop[IO],
  )

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    clearTable(tokenQuotaTableName)
  }

  def target(pk: String, limit: Option[Long] = None): QuotaTarget =
    QuotaTarget(pk, windowSec, limit)

  def reservedState(outcome: ReserveOutcome, pk: String): TokenQuotaState =
    outcome match {
      case ReserveOutcome.Reserved(states) => states(pk)
      case other => fail(s"expected Reserved, got $other")
    }

  def isReserved(o: ReserveOutcome): Boolean = o
    .isInstanceOf[ReserveOutcome.Reserved]
  def isExceeded(o: ReserveOutcome): Boolean = o
    .isInstanceOf[ReserveOutcome.LimitExceeded]
  def isContended(o: ReserveOutcome): Boolean = o
    .isInstanceOf[ReserveOutcome.Contended]

  "DynamoDBTokenQuotaStore" - {

    "getQuota returns None for missing key" in store.getQuota("user:u999:3600s")
      .asserting(_ shouldBe None)

    "reserve creates a new window on first call" in {
      val pk = "user:u1:3600s"
      val nowMs = System.currentTimeMillis()

      store.reserve(List(target(pk)), 10L, 20L, nowMs).asserting(outcome =>
        reservedState(outcome, pk) shouldBe TokenQuotaState(10L, 20L, nowMs, 1L),
      )
    }

    "reserve accumulates within the same window" in {
      val pk = "user:u2:3600s"
      val nowMs = System.currentTimeMillis()

      val test = for {
        _ <- store.reserve(List(target(pk)), 100L, 50L, nowMs)
        second <- store.reserve(List(target(pk)), 30L, 20L, nowMs)
        state <- store.getQuota(pk)
      } yield (second, state)

      test.asserting { case (second, state) =>
        val expected = TokenQuotaState(130L, 70L, nowMs, 2L)
        reservedState(second, pk) shouldBe expected
        state shouldBe Some(expected)
      }
    }

    "reserve starts a fresh window once the previous one lapses" in {
      val pk = "user:u3:3600s"
      val start = System.currentTimeMillis() - 2 * windowSec * 1000
      val later = start + (windowSec + 1) * 1000

      val test = for {
        _ <- store.reserve(List(target(pk)), 100L, 100L, start)
        _ <- store.reserve(List(target(pk)), 5L, 5L, later)
        state <- store.getQuota(pk)
      } yield state

      // The counter restarts but the version keeps climbing.
      test.asserting(_ shouldBe Some(TokenQuotaState(5L, 5L, later, 2L)))
    }

    "reserve rejects without writing when the limit would be exceeded" in {
      val pk = "user:u4:3600s"
      val nowMs = System.currentTimeMillis()
      val capped = target(pk, limit = Some(100L))

      val test = for {
        first <- store.reserve(List(capped), 60L, 0L, nowMs)
        second <- store.reserve(List(capped), 60L, 0L, nowMs)
        state <- store.getQuota(pk)
      } yield (first, second, state)

      test.asserting { case (first, second, state) =>
        isReserved(first) shouldBe true
        second shouldBe ReserveOutcome.LimitExceeded(pk, 60L, nowMs)
        state shouldBe Some(TokenQuotaState(60L, 0L, nowMs, 1L))
      }
    }

    "reserve never admits past the limit under concurrent writers" in {
      val pk = "user:u5:3600s"
      val nowMs = System.currentTimeMillis()
      val limit = 1000L
      val perWriter = 30L
      val writers = 50
      val capped = target(pk, limit = Some(limit))

      val test = for {
        outcomes <- (1 to writers).toList
          .parTraverse(_ => store.reserve(List(capped), perWriter, 0L, nowMs))
        state <- store.getQuota(pk)
      } yield (outcomes, state)

      test.asserting { case (outcomes, state) =>
        val admitted = outcomes.count(isReserved)
        val exceeded = outcomes.count(isExceeded)
        val contended = outcomes.count(isContended)
        admitted should be >= 1
        admitted * perWriter should be <= limit
        state.map(_.totalTokens) shouldBe Some(admitted * perWriter)
        admitted + exceeded + contended shouldBe writers
      }
    }

    "reserve across several targets writes all or nothing" in {
      val userPk = "user:u6:3600s"
      val orgPk = "org:o6:3600s"
      val nowMs = System.currentTimeMillis()
      val targets = List(target(userPk, Some(100L)), target(orgPk, Some(50L)))

      val test = for {
        rejected <- store.reserve(targets, 60L, 0L, nowMs)
        userAfterReject <- store.getQuota(userPk)
        orgAfterReject <- store.getQuota(orgPk)
        admitted <- store.reserve(targets, 40L, 0L, nowMs)
        userAfterAdmit <- store.getQuota(userPk)
        orgAfterAdmit <- store.getQuota(orgPk)
      } yield (
        rejected,
        userAfterReject,
        orgAfterReject,
        admitted,
        userAfterAdmit,
        orgAfterAdmit,
      )

      test.asserting { case (rejected, u0, o0, admitted, u1, o1) =>
        rejected shouldBe ReserveOutcome.LimitExceeded(orgPk, 0L, nowMs)
        u0 shouldBe None
        o0 shouldBe None
        isReserved(admitted) shouldBe true
        u1.map(_.totalTokens) shouldBe Some(40L)
        o1.map(_.totalTokens) shouldBe Some(40L)
      }
    }

    "reserve across several targets stays consistent under concurrency" in {
      val a = "user:u7:3600s"
      val b = "org:o7:3600s"
      val nowMs = System.currentTimeMillis()
      val writers = 20

      val test = for {
        outcomes <- (1 to writers).toList.parTraverse(_ =>
          store.reserve(List(target(a), target(b)), 1L, 1L, nowMs),
        )
        stateA <- store.getQuota(a)
        stateB <- store.getQuota(b)
      } yield (outcomes, stateA, stateB)

      test.asserting { case (outcomes, stateA, stateB) =>
        val admitted = outcomes.count(isReserved)
        admitted should be >= 1
        admitted + outcomes.count(isContended) shouldBe writers
        stateA.map(_.totalTokens) shouldBe Some(admitted * 2L)
        stateB.map(_.totalTokens) shouldBe Some(admitted * 2L)
      }
    }

    "reservations: reconcile applies the stored estimate once" - {
      // Reconcile used to trust the caller's estimate (finding B). The
      // reservation is now written in the same transaction as the counters,
      // and reconcile flips it to reconciled in the same transaction as the
      // adjustment, conditioned on it still being pending.

      def reservation(pk: String, nowMs: Long): NewReservation =
        NewReservation(pk, nowMs / 1000 + 3600)

      "an admitted reserve leaves a reservation that reconciles once" in {
        val user = "user:r1:3600s"
        val nowMs = System.currentTimeMillis()
        val test = for {
          reserved <- store.reserve(
            List(target(user, Some(1000))),
            100L,
            0L,
            nowMs,
            Some(reservation("reservation:r1", nowMs)),
          )
          first <- store.reconcile("reservation:r1", 40L, 0L, nowMs)
          again <- store.reconcile("reservation:r1", 0L, 0L, nowMs)
          state <- store.getQuota(user)
        } yield (reserved, first, again, state)

        test.asserting { case (reserved, first, again, state) =>
          isReserved(reserved) shouldBe true
          first shouldBe a[ReconcileOutcome.Applied]
          again match {
            case ReconcileOutcome.AlreadyReconciled(record) =>
              record.reconciled shouldBe Some(ReconciledUsage(40L, 0L))
            case other => fail(s"expected AlreadyReconciled, got $other")
          }
          state.map(_.totalTokens) shouldBe Some(40L)
        }
      }

      "concurrent reconciles of one reservation apply exactly once" in {
        val user = "user:r2:3600s"
        val nowMs = System.currentTimeMillis()
        val test = for {
          _ <- store.reserve(
            List(target(user, Some(1000))),
            100L,
            0L,
            nowMs,
            Some(reservation("reservation:r2", nowMs)),
          )
          outcomes <- (1 to 10).toList
            .parTraverse(_ => store.reconcile("reservation:r2", 0L, 0L, nowMs))
          state <- store.getQuota(user)
        } yield (outcomes, state)

        test.asserting { case (outcomes, state) =>
          outcomes.count(_.isInstanceOf[ReconcileOutcome.Applied]) shouldBe 1
          outcomes
            .count(_.isInstanceOf[ReconcileOutcome.AlreadyReconciled]) shouldBe
            9
          state.map(_.totalTokens) shouldBe Some(0L)
        }
      }

      "if the reservation cannot be written, the charge is released and the check refused" in {
        // An item already at the reservation's key makes its conditional
        // write fail every time, standing in for a store that cannot take it.
        val user = "user:r5:3600s"
        val nowMs = System.currentTimeMillis()
        dynamoDbClient.putItem(
          PutItemRequest.builder().tableName(tokenQuotaTableName).item(
            Map("pk" -> AttributeValue.builder().s("reservation:r5").build())
              .asJava,
          ).build(),
        ).get()
        val test = for {
          outcome <- store.reserve(
            List(target(user, Some(1000))),
            100L,
            0L,
            nowMs,
            Some(reservation("reservation:r5", nowMs)),
          )
          state <- store.getQuota(user)
        } yield (outcome, state)

        test.asserting { case (outcome, state) =>
          isContended(outcome) shouldBe true
          state.map(_.totalTokens).getOrElse(0L) shouldBe 0L
        }
      }

      "a rejected reserve leaves nothing to reconcile" in {
        val nowMs = System.currentTimeMillis()
        val test = for {
          outcome <- store.reserve(
            List(target("user:r3:3600s", Some(50))),
            100L,
            0L,
            nowMs,
            Some(reservation("reservation:r3", nowMs)),
          )
          reconciled <- store.reconcile("reservation:r3", 0L, 0L, nowMs)
        } yield (outcome, reconciled)

        test.asserting { case (outcome, reconciled) =>
          isExceeded(outcome) shouldBe true
          reconciled shouldBe ReconcileOutcome.NotFound
        }
      }

      "an expired reservation is not found, even before DynamoDB deletes it" in {
        val nowMs = System.currentTimeMillis()
        val test = for {
          _ <- store.reserve(
            List(target("user:r4:3600s", Some(1000))),
            100L,
            0L,
            nowMs,
            Some(NewReservation("reservation:r4", nowMs / 1000 + 60)),
          )
          later <- store.reconcile("reservation:r4", 0L, 0L, nowMs + 61_000)
        } yield later

        test.asserting(_ shouldBe ReconcileOutcome.NotFound)
      }
    }

    "corrupt state is handled gracefully" in {
      val pk = "corrupt-pk"
      val item = Map(
        "pk" -> AttributeValue.builder().s(pk).build(),
        // omit input_tokens, output_tokens, window_start, version so parseState fails
      )
      val request = PutItemRequest.builder().tableName(tokenQuotaTableName)
        .item(item.asJava).build()
      dynamoDbClient.putItem(request).get()

      store.getQuota(pk).asserting(_ shouldBe None)
    }

    "healthCheck returns Right for existing table" in
      store.healthCheck.asserting(_ shouldBe Right(()))
  }
}
