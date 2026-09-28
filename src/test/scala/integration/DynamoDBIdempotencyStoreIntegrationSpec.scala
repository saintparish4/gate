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
        success <- store
          .storeResponse("response-key", "client-1", storedResponse)

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
        _ <- store.storeResponse("complex-key", "client-1", complexResponse)
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
        marked <- store.markFailed("retry-key", "client-1")

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

    // The condition used to be only attribute_exists(pk), so a Completed
    // record could be reopened and its operation run again.
    "markFailed refuses a completed record and another client's record" in {
      val done = StoredResponse(200, "ok", Map.empty, Instant.EPOCH)
      val test = for {
        _ <- store.check("fail-done", clientId = "client-1", ttlSeconds = 3600)
        _ <- store.storeResponse("fail-done", "client-1", done)
        reopened <- store.markFailed("fail-done", "client-1")
        _ <- store.check("fail-theirs", clientId = "client-1", ttlSeconds = 3600)
        forged <- store.markFailed("fail-theirs", "client-2")
        missing <- store.markFailed("fail-missing", "client-1")
        replay <- store
          .check("fail-done", clientId = "client-1", ttlSeconds = 3600)
        pending <- store
          .check("fail-theirs", clientId = "client-1", ttlSeconds = 3600)
      } yield (reopened, forged, missing, replay, pending)

      test.asserting { case (reopened, forged, missing, replay, pending) =>
        reopened shouldBe false
        forged shouldBe false
        missing shouldBe false
        replay shouldBe a[IdempotencyResult.Duplicate]
        pending shouldBe a[IdempotencyResult.InProgress]
      }
    }

    // The API caps the encoded response at MaxStoredResponseBytes (413 above
    // it). This proves the cap fits a real item with a partition key near its
    // own 2 KB limit, and that the item limit it guards against is real.
    "a response at the API's cap is stored and replayed; one past the item limit is not" in {
      val at = Instant.ofEpochMilli(1_700_000_000_000L)
      val empty = StoredResponse(200, "", Map.empty, at)
      val atCap = empty.copy(body =
        "a" *
          (api.IdempotencyApi.MaxStoredResponseBytes -
            api.IdempotencyApi.storedSize(empty)),
      )
      val longKey = "k" * 1900
      val tooBig = empty.copy(body = "a" * (410 * 1024))
      val test = for {
        _ <- store.check(longKey, clientId = "client-1", ttlSeconds = 3600)
        stored <- store.storeResponse(longKey, "client-1", atCap)
        replay <- store.check(longKey, clientId = "client-1", ttlSeconds = 3600)
        _ <- store.check("too-big", clientId = "client-1", ttlSeconds = 3600)
        refused <- store.storeResponse("too-big", "client-1", tooBig).attempt
      } yield (stored, replay, refused)

      test.asserting { case (stored, replay, refused) =>
        api.IdempotencyApi.storedSize(atCap) shouldBe
          api.IdempotencyApi.MaxStoredResponseBytes
        stored shouldBe true
        replay match {
          case IdempotencyResult.Duplicate(_, Some(r), _) =>
            r.body.length shouldBe atCap.body.length
          case other => fail(s"expected a replay, got $other")
        }
        refused.isLeft shouldBe true
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

    "storeResponse refuses a client other than the one that created the record" in {
      // The condition behind scoped keys (ADR-005): reaching the row is not
      // enough to complete it.
      val forged = StoredResponse(200, "forged", Map.empty, Instant.now())
      val test = for {
        _ <- store.check("owned-key", clientId = "owner", ttlSeconds = 3600)
        stored <- store.storeResponse("owned-key", "intruder", forged)
        after <- store.check("owned-key", clientId = "owner", ttlSeconds = 3600)
      } yield (stored, after)

      test.asserting { case (stored, after) =>
        stored shouldBe false
        after shouldBe a[IdempotencyResult.InProgress]
      }
    }
  }
}
