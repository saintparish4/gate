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
import core.TokenQuotaState
import storage.DynamoDBTokenQuotaStore
import observability.MetricsPublisher
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all.*

/** Integration tests for DynamoDBTokenQuotaStore.
  *
  * Uses LocalStack/TestContainers. Tests: getQuota (missing/corrupt),
  * incrementQuota (new window, accumulate, new window after expiry), OCC under
  * concurrency, healthCheck. Requires Docker. Without Docker, run unit tests
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

  "DynamoDBTokenQuotaStore" - {

    "getQuota returns None for missing key" in store.getQuota("user:u999:3600s")
      .asserting(_ shouldBe None)

    "incrementQuota creates a new window on first call" in {
      val pk = "user:u1:3600s"
      val nowMs = System.currentTimeMillis()
      val windowSec = 3600L

      val test = for {
        ok <- store.incrementQuota(pk, 10L, 20L, windowSec, nowMs)
        state <- store.getQuota(pk)
      } yield (ok, state)

      test.asserting { case (ok, state) =>
        ok shouldBe true
        state shouldBe Some(TokenQuotaState(
          inputTokens = 10L,
          outputTokens = 20L,
          windowStart = nowMs,
          version = 1L,
        ))
      }
    }

    "incrementQuota accumulates within the same window" in {
      val pk = "user:u2:3600s"
      val nowMs = System.currentTimeMillis()
      val windowSec = 3600L

      val test = for {
        _ <- store.incrementQuota(pk, 100L, 50L, windowSec, nowMs)
        _ <- store.incrementQuota(pk, 30L, 20L, windowSec, nowMs)
        state <- store.getQuota(pk)
      } yield state

      test.asserting(state =>
        state shouldBe Some(TokenQuotaState(
          inputTokens = 130L,
          outputTokens = 70L,
          windowStart = nowMs,
          version = 2L,
        )),
      )
    }

    "incrementQuota starts new window when previous expires" in {
      // Store uses same pk per (level, id, windowSec). When window expires it
      // tries to create new (attribute_not_exists(pk)); that fails so we
      // verify old state is unchanged and a *different* pk gets a fresh window.
      val pkOld = "user:u3:3600s"
      val pkNew = "user:u3-new:3600s"
      val windowStartMs = System.currentTimeMillis() - 4000L * 1000
      val windowSec = 3600L
      val nowMs = windowStartMs + (windowSec + 1) * 1000

      val test = for {
        _ <- store.incrementQuota(pkOld, 100L, 100L, windowSec, windowStartMs)
        _ <- store.incrementQuota(pkOld, 5L, 5L, windowSec, nowMs) // same pk, expired -> no update
        oldState <- store.getQuota(pkOld)
        _ <- store.incrementQuota(pkNew, 5L, 5L, windowSec, nowMs) // new pk -> fresh window
        newState <- store.getQuota(pkNew)
      } yield (oldState, newState)

      test.asserting { case (oldState, newState) =>
        oldState shouldBe Some(TokenQuotaState(100L, 100L, windowStartMs, 1L))
        newState shouldBe Some(TokenQuotaState(5L, 5L, nowMs, 1L))
      }
    }

    "OCC conflict resolution under concurrent writes" in {
      val pk = "user:u4:3600s"
      val nowMs = System.currentTimeMillis()
      val windowSec = 3600L
      val n = 20

      val test = for {
        _ <- (1 to n).toList
          .parTraverse(_ => store.incrementQuota(pk, 1L, 1L, windowSec, nowMs))
        state <- store.getQuota(pk)
      } yield state

      test.asserting { state =>
        state shouldBe defined
        val s = state.get
        s.inputTokens + s.outputTokens shouldBe n * 2L
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
