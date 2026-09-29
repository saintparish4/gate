package integration

import scala.jdk.CollectionConverters.*

import org.scalatest.BeforeAndAfterEach
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import software.amazon.awssdk.services.dynamodb.model.{
  AttributeValue, GetItemRequest, PutItemRequest,
}
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import core.{QuotaTarget, RateLimitDecision, RateLimitProfile, ReserveOutcome}
import observability.MetricsPublisher
import storage.{
  DynamoDBOps, DynamoDBRateLimitStore, DynamoDBSlidingWindowStore,
  DynamoDBTokenQuotaStore, LeakyBucketRateLimitStore,
}

/** Corrupt state fails closed and self-heals (owner decision, PR 5), against
  * real DynamoDB conditions. The unit stubs ignore write conditions, and the
  * old fallback's attribute_not_exists write failed only here, against a real
  * existing item, so this is where the policy is proven.
  */
@Integration
class CorruptStateIntegrationSpec
    extends AsyncFreeSpec
    with AsyncIOSpec
    with Matchers
    with LocalStackIntegrationSpec
    with BeforeAndAfterEach {

  implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  val quotaTable = "test-token-quotas-corrupt"

  override protected def setupResources(): Unit = {
    super.setupResources()
    createDynamoDBTable(quotaTable)
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    clearTable(testDynamoDBConfig.rateLimitTable)
    clearTable(quotaTable)
  }

  private val table = testDynamoDBConfig.rateLimitTable
  private val profile =
    RateLimitProfile(capacity = 10, refillRatePerSecond = 1.0, ttlSeconds = 3600)

  private def s(v: String) = AttributeValue.builder().s(v).build()
  private def n(v: String) = AttributeValue.builder().n(v).build()

  private def put(tableName: String, item: Map[String, AttributeValue]): Unit =
    dynamoDbClient.putItem(
      PutItemRequest.builder().tableName(tableName).item(item.asJava).build(),
    ).get()

  private def get(tableName: String, pk: String): Map[String, AttributeValue] =
    dynamoDbClient.getItem(
      GetItemRequest.builder().tableName(tableName)
        .key(Map("pk" -> s(pk)).asJava).consistentRead(true).build(),
    ).get().item().asScala.toMap

  "token bucket: a malformed item is refused, then replaced by an empty bucket at the next version" in {
    put(
      table,
      Map(
        "pk" -> s("ratelimit#tb"),
        "tokens" -> s("garbage"),
        "lastRefillMs" -> n("0"),
        "version" -> n("7"),
      ),
    )
    val store =
      DynamoDBRateLimitStore[IO](dynamoDbClient, table, MetricsPublisher.noop[IO])
    for {
      decision <- store.checkAndConsume("tb", 1, profile)
      status <- store.getStatus("tb", profile)
    } yield {
      decision shouldBe a[RateLimitDecision.Rejected]
      val item = get(table, "ratelimit#tb")
      item("version").n() shouldBe "8"
      item("tokens").n().toDouble shouldBe 0.0 +- 0.5
      status.map(_.tokensRemaining) shouldBe Some(0)
    }
  }

  "token bucket: an item with no version is healed too, at version 1" in {
    put(
      table,
      Map(
        "pk" -> s("ratelimit#nov"),
        "tokens" -> n("5"),
        "lastRefillMs" -> n("0"),
      ),
    )
    val store =
      DynamoDBRateLimitStore[IO](dynamoDbClient, table, MetricsPublisher.noop[IO])
    store.checkAndConsume("nov", 1, profile).map { decision =>
      decision shouldBe a[RateLimitDecision.Rejected]
      get(table, "ratelimit#nov")("version").n() shouldBe "1"
    }
  }

  "leaky bucket: a malformed item is refused, then replaced by a full bucket" in {
    put(
      table,
      Map(
        "pk" -> s("ratelimit#lb"),
        "tokens" -> n("1"),
        "lastRefillMs" -> s("yesterday"),
        "version" -> n("3"),
      ),
    )
    val store = LeakyBucketRateLimitStore[IO](
      dynamoDbClient,
      table,
      MetricsPublisher.noop[IO],
    )
    store.checkAndConsume("lb", 1, profile).map { decision =>
      decision shouldBe a[RateLimitDecision.Rejected]
      val item = get(table, "ratelimit#lb")
      item("version").n() shouldBe "4"
      item("tokens").n().toDouble shouldBe 10.0
    }
  }

  "sliding window: a malformed item is refused, then replaced by a full window" in {
    put(
      table,
      Map("pk" -> s("sw#sw"), "counts" -> s("not-a-map"), "version" -> n("2")),
    )
    val store = DynamoDBSlidingWindowStore[IO](
      dynamoDbClient,
      table,
      MetricsPublisher.noop[IO],
    )
    store.checkAndConsume("sw", 1, profile).map { decision =>
      decision shouldBe a[RateLimitDecision.Rejected]
      val item = get(table, "sw#sw")
      item("version").n() shouldBe "3"
      item("counts").m().asScala.values.map(_.n().toLong).sum shouldBe 10L
    }
  }

  "quota: a malformed counter is refused, then replaced by a window exhausted from now" in {
    put(
      quotaTable,
      Map(
        "pk" -> s("user:corrupt:3600s"),
        "input_tokens" -> s("lots"),
        "output_tokens" -> n("0"),
        "window_start" -> n("0"),
        "version" -> n("5"),
      ),
    )
    val store = DynamoDBTokenQuotaStore[IO](
      dynamoDbClient,
      quotaTable,
      logger,
      MetricsPublisher.noop[IO],
    )
    val target = QuotaTarget("user:corrupt:3600s", 3600, Some(1000L))
    store.reserve(List(target), 10L, 0L, System.currentTimeMillis()).map {
      outcome =>
        outcome shouldBe a[ReserveOutcome.LimitExceeded]
        val item = get(quotaTable, "user:corrupt:3600s")
        item("version").n() shouldBe "6"
        item("input_tokens").n() shouldBe "1000"
    }
  }

  "the replacement is conditioned on the raw version it read" in {
    put(
      table,
      Map(
        "pk" -> s("ratelimit#race"),
        "tokens" -> s("garbage"),
        "version" -> n("7"),
      ),
    )
    val replacement = Map(
      "pk" -> s("ratelimit#race"),
      "tokens" -> n("0"),
      "lastRefillMs" -> n("0"),
      "version" -> n("8"),
    )
    for {
      // Another writer moved the item on after we read version 6: refused.
      stale <- DynamoDBOps.replaceCorrupt[IO](
        dynamoDbClient,
        table,
        DynamoDBOps.CorruptItem(Some(n("6")), "x"),
        replacement,
      )
      // We read "no version", but the item has one: refused.
      absent <- DynamoDBOps.replaceCorrupt[IO](
        dynamoDbClient,
        table,
        DynamoDBOps.CorruptItem(None, "x"),
        replacement,
      )
      current <- DynamoDBOps.replaceCorrupt[IO](
        dynamoDbClient,
        table,
        DynamoDBOps.CorruptItem(Some(n("7")), "x"),
        replacement,
      )
    } yield {
      stale shouldBe false
      absent shouldBe false
      current shouldBe true
      get(table, "ratelimit#race")("version").n() shouldBe "8"
    }
  }
}
