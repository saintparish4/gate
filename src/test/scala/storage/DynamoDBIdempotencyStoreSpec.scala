package storage

import java.lang.reflect.{InvocationHandler, Proxy}
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicInteger

import scala.jdk.CollectionConverters.*

import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

import cats.effect.*
import cats.effect.testing.scalatest.AsyncIOSpec
import core.IdempotencyResult
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.*

/** A check whose claim lost, and which then read the record as Failed, used to
  * answer New without claiming it. The caller whose claim on the Failed record
  * won was told New too, and both ran the operation. It became reachable once
  * `POST /v1/idempotency/{key}/fail` exposed markFailed.
  */
class DynamoDBIdempotencyStoreSpec
    extends AsyncFreeSpec with AsyncIOSpec with Matchers:

  private val failedRecord = GetItemResponse.builder().item {
    Map(
      "pk" -> AttributeValue.builder().s("idempotency#k").build(),
      "clientId" -> AttributeValue.builder().s("client-1").build(),
      "status" -> AttributeValue.builder().s("Failed").build(),
      "createdAt" -> AttributeValue.builder().n("0").build(),
      "updatedAt" -> AttributeValue.builder().n("0").build(),
      "version" -> AttributeValue.builder().n("2").build(),
      "ttl" -> AttributeValue.builder().n("3600").build(),
    ).asJava
  }.build()

  /** Loses the first `losses` claims, and reads every record as Failed. */
  private def client(losses: Int, puts: AtomicInteger): DynamoDbAsyncClient =
    Proxy.newProxyInstance(
      classOf[DynamoDbAsyncClient].getClassLoader,
      Array(classOf[DynamoDbAsyncClient]),
      new InvocationHandler:
        override def invoke(
            proxy: Object,
            method: java.lang.reflect.Method,
            args: Array[Object],
        ): Object = method.getName match
          case "putItem" =>
            if puts.getAndIncrement() < losses then
              CompletableFuture.failedFuture[Object](
                ConditionalCheckFailedException.builder().message("lost").build(),
              )
            else
              CompletableFuture.completedFuture(PutItemResponse.builder().build())
          case "getItem" => CompletableFuture.completedFuture(failedRecord)
          case "serviceName" => "DynamoDB"
          case "close" => null
          case other => CompletableFuture
              .failedFuture[Object](new UnsupportedOperationException(
                s"Stub does not implement: $other",
              )),
    ).asInstanceOf[DynamoDbAsyncClient]

  "a Failed record read after a lost claim is claimed again, not answered New" in {
    val puts = AtomicInteger(0)
    DynamoDBIdempotencyStore[IO](client(losses = 1, puts), "t")
      .check("k", "client-1", 3600).asserting { result =>
        result shouldBe a[IdempotencyResult.New]
        puts.get shouldBe 2
      }
  }

  "a check that never wins a claim fails instead of answering New" in {
    val puts = AtomicInteger(0)
    DynamoDBIdempotencyStore[IO](client(losses = Int.MaxValue, puts), "t")
      .check("k", "client-1", 3600).attempt.asserting { result =>
        result.isLeft shouldBe true
        puts.get shouldBe 4
      }
  }
