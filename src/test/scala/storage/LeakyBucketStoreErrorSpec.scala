package storage

import java.lang.reflect.{InvocationHandler, Proxy}
import java.util.concurrent.CompletableFuture

import scala.jdk.CollectionConverters.*

import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.typelevel.log4cats.Logger

import testutil.*
import cats.effect.*
import cats.effect.testing.scalatest.AsyncIOSpec
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.*
import core.RateLimitDecision

class LeakyBucketStoreErrorSpec
    extends AsyncFreeSpec with AsyncIOSpec with Matchers:

  private def stubClient(getItemResp: GetItemResponse): DynamoDbAsyncClient =
    Proxy.newProxyInstance(
      classOf[DynamoDbAsyncClient].getClassLoader,
      Array(classOf[DynamoDbAsyncClient]),
      new InvocationHandler:
        override def invoke(
            proxy: Object,
            method: java.lang.reflect.Method,
            args: Array[Object],
        ): Object = method.getName match
          case "getItem" => CompletableFuture.completedFuture(getItemResp)
          case "putItem" => CompletableFuture
              .completedFuture(PutItemResponse.builder().build())
          case "updateItem" => CompletableFuture
              .completedFuture(UpdateItemResponse.builder().build())
          case "describeTable" => CompletableFuture
              .completedFuture(DescribeTableResponse.builder().build())
          case "serviceName" => "DynamoDB"
          case "close" => null
          case other => CompletableFuture
              .failedFuture[Object](new UnsupportedOperationException(
                s"Stub does not implement: $other",
              )),
    ).asInstanceOf[DynamoDbAsyncClient]

  private def corruptTokensResponse(key: String): GetItemResponse =
    val item = Map(
      "pk" -> AttributeValue.builder().s(s"ratelimit#$key").build(),
      "tokens" -> AttributeValue.builder().n("not_a_number").build(),
      "lastRefillMs" -> AttributeValue.builder().n("0").build(),
      "version" -> AttributeValue.builder().n("1").build(),
    ).asJava
    GetItemResponse.builder().item(item).build()

  "LeakyBucketRateLimitStore — corrupt stored state" - {

    "fails open (grants full capacity) when stored tokens attribute is malformed" in {
      val key = "corrupt-leaky-key"
      val client = stubClient(corruptTokensResponse(key))

      (for
        errorLogs <- Ref.of[IO, List[String]](Nil)
        metricNames <- Ref.of[IO, List[String]](Nil)
        metrics = capturingMetrics(metricNames)
        given Logger[IO] = capturingLogger(errorLogs)
        store = LeakyBucketRateLimitStore[IO](client, "test-table", metrics)
        decision <- store.checkAndConsume(key, cost = 1, testProfile)

        logs <- errorLogs.get
        recorded <- metricNames.get
      yield (decision, logs, recorded)).asserting {
        case (decision, logs, recorded) =>
          decision shouldBe a[RateLimitDecision.Allowed]

          logs
            .exists(msg =>
              msg.contains(key) && msg.contains("Corrupt"),
            ) shouldBe true

          recorded should contain("CorruptStateRead")
      }
    }
  }
