package storage

import java.time.Instant

import org.scalatest.Inspectors.*
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

import core.{IdempotencyResult, StoredResponse}
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all.*

/** Concurrency tests for the idempotency store's first-writer-wins contract.
  *
  * All tests run against the in-memory store (backed by a Cats Effect Ref) to
  * keep the suite fast and Docker-free. The same behaviours are validated
  * against LocalStack in DynamoDBIdempotencyStoreIntegrationSpec.
  */
class IdempotencyConcurrencySpec
    extends AsyncFreeSpec with AsyncIOSpec with Matchers:

  private val clientId = "client-1"
  private val ttlSeconds = 3600L
  private val concurrency = 50

  "InMemoryIdempotencyStore under concurrent load" - {

    "exactly one writer wins for a brand-new key" in {
      // 50 fibers all race to claim the same idempotency key.
      // First-writer-wins: exactly 1 New, the rest 49 InProgress.
      val test =
        for
          store <- InMemoryIdempotencyStore.create[IO]
          results <- IO.parSequenceN(concurrency)(
            List.fill(concurrency)(store.check("race-key", clientId, ttlSeconds)),
          )
          newCount = results.count(_.isInstanceOf[IdempotencyResult.New])
          inProgressCount = results
            .count(_.isInstanceOf[IdempotencyResult.InProgress])
          duplicateCount = results
            .count(_.isInstanceOf[IdempotencyResult.Duplicate])
        yield (newCount, inProgressCount, duplicateCount)

      test.asserting { case (newCount, inProgressCount, duplicateCount) =>
        newCount shouldBe 1
        inProgressCount shouldBe concurrency - 1
        duplicateCount shouldBe 0
      }
    }

    "exactly one retry wins after the key is marked failed" in {
      // Simulate a crash mid-processing: one fiber claims the key, the
      // downstream handler fails, and the key is marked Failed.  Then 50
      // concurrent retries race to reclaim it.  Only one should win a New slot.
      val test =
        for
          store <- InMemoryIdempotencyStore.create[IO]
          _ <- store.check("fail-retry-key", clientId, ttlSeconds)
          _ <- store.markFailed("fail-retry-key")
          results <- IO.parSequenceN(concurrency)(List.fill(concurrency)(
            store.check("fail-retry-key", clientId, ttlSeconds),
          ))
          newCount = results.count(_.isInstanceOf[IdempotencyResult.New])
          inProgressCount = results
            .count(_.isInstanceOf[IdempotencyResult.InProgress])
        yield (newCount, inProgressCount)

      test.asserting { case (newCount, inProgressCount) =>
        newCount shouldBe 1
        inProgressCount shouldBe concurrency - 1
      }
    }

    "all concurrent readers see Duplicate once the response is stored" in {
      // After the winner stores a response, every subsequent check must see
      // Duplicate with the original response — even under parallel load.
      val response = StoredResponse(
        statusCode = 201,
        body = """{"id":"abc"}""",
        headers = Map("X-Request-Id" -> "req-001"),
        completedAt = Instant.now(),
      )

      val test =
        for
          store <- InMemoryIdempotencyStore.create[IO]
          _ <- store.check("done-key", clientId, ttlSeconds)
          _ <- store.storeResponse("done-key", response)
          results <- IO.parSequenceN(concurrency)(
            List.fill(concurrency)(store.check("done-key", clientId, ttlSeconds)),
          )
        yield results

      test.asserting(results =>
        forEvery(results) { r =>
          r shouldBe a[IdempotencyResult.Duplicate]
          val dup = r.asInstanceOf[IdempotencyResult.Duplicate]
          dup.originalResponse shouldBe defined
          dup.originalResponse.get.statusCode shouldBe 201
        },
      )
    }

    "independent keys are isolated: each gets exactly one New under concurrent load" in {
      // 10 distinct keys × 10 concurrent requests each.
      // Every key must have exactly 1 New winner and 9 InProgress.
      val keys = (1 to 10).map(i => s"parallel-key-$i").toList
      val parallelism = 10

      val test =
        for
          store <- InMemoryIdempotencyStore.create[IO]
          allResults <- IO.parSequenceN(keys.length)(keys.map(key =>
            IO.parSequenceN(parallelism)(
              List.fill(parallelism)(store.check(key, clientId, ttlSeconds)),
            ),
          ))
        yield allResults

      test.asserting(allResults =>
        forEvery(allResults) { results =>
          results.count(_.isInstanceOf[IdempotencyResult.New]) shouldBe 1
          results.count(_.isInstanceOf[IdempotencyResult.InProgress]) shouldBe
            parallelism - 1
        },
      )
    }

    "concurrent same-key different-body: one New, rest InProgress or KeyConflict" in {
      val test =
        for
          store <- InMemoryIdempotencyStore.create[IO]
          results <- IO
            .parSequenceN(concurrency)((1 to concurrency).toList.map(i =>
              store.check(
                "conflict-key",
                clientId,
                ttlSeconds,
                requestHash = Some(s"body-hash-$i"),
              ),
            ))
          newCount = results.count(_.isInstanceOf[IdempotencyResult.New])
          conflictCount = results
            .count(_.isInstanceOf[IdempotencyResult.KeyConflict])
          inProgressCount = results
            .count(_.isInstanceOf[IdempotencyResult.InProgress])
        yield (newCount, conflictCount, inProgressCount)

      test.asserting { case (newCount, conflictCount, inProgressCount) =>
        newCount shouldBe 1
        // Non-winners see either InProgress (same hash by luck) or KeyConflict
        conflictCount + inProgressCount shouldBe concurrency - 1
      }
    }
  }
