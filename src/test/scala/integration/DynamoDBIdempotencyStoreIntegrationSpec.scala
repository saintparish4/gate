package integration

import java.time.Instant

import org.scalatest.BeforeAndAfterEach
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import core.{IdempotencyResult, StoredResponse}
import storage.DynamoDBIdempotencyStore
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all.*

/** Integration tests for DynamoDBIdempotencyStore.
  *
  * Tests first-writer-wins semantics and response caching. Requires Docker.
  * Without Docker, run unit tests only: sbt unitTest
  */
@Integration
class DynamoDBIdempotencyStoreIntegrationSpec
    extends AsyncFreeSpec
    with AsyncIOSpec
    with Matchers
    with LocalStackIntegrationSpec
    with BeforeAndAfterEach {

  implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  lazy val store: DynamoDBIdempotencyStore[IO] = new DynamoDBIdempotencyStore[IO](
    dynamoDbClient,
    testDynamoDBConfig.idempotencyTable,
  )

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    clearTable(testDynamoDBConfig.idempotencyTable)
  }

  "DynamoDBIdempotencyStore" - {

    "should return New for first request" in
      store.check("new-key-1", clientId = "client-1", ttlSeconds = 3600)
        .asserting { result =>
          result shouldBe a[IdempotencyResult.New]
          result.asInstanceOf[IdempotencyResult.New].idempotencyKey shouldBe
            "new-key-1"
        }

    "should return InProgress for second request with same key (before completion)" in {
      val test = for {
        first <- store.check("dup-key", clientId = "client-1", ttlSeconds = 3600)
        second <- store
          .check("dup-key", clientId = "client-1", ttlSeconds = 3600)
      } yield (first, second)

      test.asserting { case (first, second) =>
        first shouldBe a[IdempotencyResult.New]
        second shouldBe a[IdempotencyResult.InProgress]
        second.asInstanceOf[IdempotencyResult.InProgress]
          .idempotencyKey shouldBe "dup-key"
      }
    }

    "should return Duplicate after response is stored" in {
      val now = Instant.now()
      val storedResponse = StoredResponse(
        statusCode = 201,
        body = """{"id": "order-123", "status": "created"}""",
        headers = Map("X-Request-Id" -> "req-456"),
        completedAt = now,
      )

      val test = for {
        // First request - mark as new
        _ <- store
          .check("response-key", clientId = "client-1", ttlSeconds = 3600)

        // Store the response
        success <- store.storeResponse("response-key", storedResponse)

        // Second request - should get cached response
        result <- store
          .check("response-key", clientId = "client-1", ttlSeconds = 3600)
      } yield (success, result)

      test.asserting { case (success, result) =>
        success shouldBe true
        result shouldBe a[IdempotencyResult.Duplicate]
        val dup = result.asInstanceOf[IdempotencyResult.Duplicate]
        dup.originalResponse shouldBe defined

        val response = dup.originalResponse.get
        response.statusCode shouldBe 201
        response.body should include("order-123")
        response.headers should contain("X-Request-Id" -> "req-456")
      }
    }

    "should handle concurrent first-writer-wins" in {
      val concurrentRequests = 10

      val test = for {
        results <- (1 to concurrentRequests).toList.parTraverse(_ =>
          store.check(
            "concurrent-idem-key",
            clientId = "client-1",
            ttlSeconds = 3600,
          ),
        )

        newCount = results.count(_.isInstanceOf[IdempotencyResult.New])
        inProgressCount = results
          .count(_.isInstanceOf[IdempotencyResult.InProgress])
      } yield (newCount, inProgressCount)

      test.asserting { case (newCount, inProgressCount) =>
        // Exactly ONE should win (first-writer-wins), rest should see InProgress
        newCount shouldBe 1
        inProgressCount shouldBe 9
      }
    }

    "should isolate different keys" in {
      val test = for {
        r1 <- store.check("key-a", clientId = "client-1", ttlSeconds = 3600)
        r2 <- store.check("key-b", clientId = "client-1", ttlSeconds = 3600)
        r3 <- store.check("key-c", clientId = "client-1", ttlSeconds = 3600)
      } yield (r1, r2, r3)

      test.asserting { case (r1, r2, r3) =>
        r1 shouldBe a[IdempotencyResult.New]
        r2 shouldBe a[IdempotencyResult.New]
        r3 shouldBe a[IdempotencyResult.New]
      }
    }

    "should pass health check" in
      store.healthCheck.asserting(healthy => healthy shouldBe Right(()))

    "should handle complex response bodies" in {
      val now = Instant.now()
      val complexResponse = StoredResponse(
        statusCode = 200,
        body =
          """{
          "items": [
            {"id": 1, "name": "Item 1", "tags": ["a", "b"]},
            {"id": 2, "name": "Item 2", "tags": ["c", "d"]}
          ],
          "pagination": {
            "page": 1,
            "total": 100
          },
          "metadata": {
            "requestId": "abc-123",
            "timestamp": "2024-01-15T10:30:00Z"
          }
        }""",
        headers =
          Map("Content-Type" -> "application/json", "X-Trace-Id" -> "trace-789"),
        completedAt = now,
      )

      val test = for {
        _ <- store.check("complex-key", clientId = "client-1", ttlSeconds = 3600)
        _ <- store.storeResponse("complex-key", complexResponse)
        result <- store
          .check("complex-key", clientId = "client-1", ttlSeconds = 3600)
      } yield result

      test.asserting { result =>
        val dup = result.asInstanceOf[IdempotencyResult.Duplicate]
        dup.originalResponse shouldBe defined

        val response = dup.originalResponse.get
        response.body should include("Item 1")
        response.body should include("pagination")
        response.headers should have size 2
      }
    }

    "should allow retry after marking as failed" in {
      val test = for {
        // First request
        first <- store
          .check("retry-key", clientId = "client-1", ttlSeconds = 3600)

        // Mark as failed
        marked <- store.markFailed("retry-key")

        // Should be able to retry
        retry <- store
          .check("retry-key", clientId = "client-1", ttlSeconds = 3600)
      } yield (first, marked, retry)

      test.asserting { case (first, marked, retry) =>
        first shouldBe a[IdempotencyResult.New]
        marked shouldBe true
        retry shouldBe a[IdempotencyResult.New] // Can retry after failure
      }
    }

    "should return KeyConflict for same key with different request hash" in {
      val test = for {
        _ <- store.check(
          "hash-key-1",
          clientId = "client-1",
          ttlSeconds = 3600,
          requestHash = Some("hash-aaa"),
        )
        result <- store.check(
          "hash-key-1",
          clientId = "client-1",
          ttlSeconds = 3600,
          requestHash = Some("hash-bbb"),
        )
      } yield result

      test.asserting(result => result shouldBe a[IdempotencyResult.KeyConflict])
    }

    "should return InProgress for same key with matching request hash" in {
      val test = for {
        _ <- store.check(
          "hash-key-2",
          clientId = "client-1",
          ttlSeconds = 3600,
          requestHash = Some("hash-same"),
        )
        result <- store.check(
          "hash-key-2",
          clientId = "client-1",
          ttlSeconds = 3600,
          requestHash = Some("hash-same"),
        )
      } yield result

      test.asserting(result => result shouldBe a[IdempotencyResult.InProgress])
    }

    "should handle concurrent same-key different-body" in {
      val test = for {
        results <- (1 to 10).toList.parTraverse(i =>
          store.check(
            "conflict-int-key",
            clientId = "client-1",
            ttlSeconds = 3600,
            requestHash = Some(s"body-$i"),
          ),
        )
        newCount = results.count(_.isInstanceOf[IdempotencyResult.New])
        conflictCount = results
          .count(_.isInstanceOf[IdempotencyResult.KeyConflict])
        inProgressCount = results
          .count(_.isInstanceOf[IdempotencyResult.InProgress])
      } yield (newCount, conflictCount, inProgressCount)

      test.asserting { case (newCount, conflictCount, inProgressCount) =>
        newCount shouldBe 1
        conflictCount + inProgressCount shouldBe 9
      }
    }
  }
}
