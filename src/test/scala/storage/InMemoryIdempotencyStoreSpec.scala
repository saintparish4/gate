package storage

import java.time.Instant

import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

import core.{IdempotencyResult, IdempotencyStatus, StoredResponse}
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all.*

class InMemoryIdempotencyStoreSpec
    extends AsyncFreeSpec with AsyncIOSpec with Matchers:

  "InMemoryIdempotencyStore" - {

    "should return New for first request" in {
      val test = for {
        store <- InMemoryIdempotencyStore.create[IO]
        result <- store
          .check("new-key", clientId = "client-1", ttlSeconds = 3600)
      } yield result

      test.asserting { result =>
        result shouldBe a[IdempotencyResult.New]
        result.asInstanceOf[IdempotencyResult.New].idempotencyKey shouldBe
          "new-key"
      }
    }

    "should return InProgress for second request (before completion)" in {
      val test = for {
        store <- InMemoryIdempotencyStore.create[IO]
        _ <- store.check("dup-key", clientId = "client-1", ttlSeconds = 3600)
        result <- store
          .check("dup-key", clientId = "client-1", ttlSeconds = 3600)
      } yield result

      test.asserting(result => result shouldBe a[IdempotencyResult.InProgress])
    }

    "should return InProgress with no response when pending" in {
      val test = for {
        store <- InMemoryIdempotencyStore.create[IO]
        _ <- store.check("pending-key", clientId = "client-1", ttlSeconds = 3600)
        result <- store
          .check("pending-key", clientId = "client-1", ttlSeconds = 3600)
      } yield result

      test.asserting(result => result shouldBe a[IdempotencyResult.InProgress])
    }

    "should store and return cached response" in {
      val now = Instant.now()
      val response = StoredResponse(
        201,
        """{"id": 1}""",
        Map("X-Id" -> "abc"),
        completedAt = now,
      )

      val test = for {
        store <- InMemoryIdempotencyStore.create[IO]
        _ <- store
          .check("response-key", clientId = "client-1", ttlSeconds = 3600)
        success <- store.storeResponse("response-key", "client-1", response)
        result <- store
          .check("response-key", clientId = "client-1", ttlSeconds = 3600)
      } yield (success, result)

      test.asserting { case (success, result) =>
        success shouldBe true
        val dup = result.asInstanceOf[IdempotencyResult.Duplicate]
        dup.originalResponse shouldBe defined
        dup.originalResponse.get.statusCode shouldBe 201
      }
    }

    "should isolate different keys" in {
      val test = for {
        store <- InMemoryIdempotencyStore.create[IO]
        r1 <- store.check("key-1", clientId = "client-1", ttlSeconds = 3600)
        r2 <- store.check("key-2", clientId = "client-1", ttlSeconds = 3600)
        r3 <- store.check("key-1", clientId = "client-1", ttlSeconds = 3600)
      } yield (r1, r2, r3)

      test.asserting { case (r1, r2, r3) =>
        r1 shouldBe a[IdempotencyResult.New]
        r2 shouldBe a[IdempotencyResult.New]
        r3 shouldBe a[IdempotencyResult.InProgress] // Pending, not completed yet
      }
    }

    "should handle concurrent first-writer-wins" in {
      val test = for {
        store <- InMemoryIdempotencyStore.create[IO]
        results <- (1 to 10).toList.parTraverse(_ =>
          store.check("concurrent-key", clientId = "client-1", ttlSeconds = 3600),
        )
        newCount = results.count(_.isInstanceOf[IdempotencyResult.New])
        inProgressCount = results
          .count(_.isInstanceOf[IdempotencyResult.InProgress])
      } yield (newCount, inProgressCount)

      test.asserting { case (newCount, inProgressCount) =>
        // Exactly one should win, rest see in-progress
        newCount shouldBe 1
        inProgressCount shouldBe 9
      }
    }

    "should allow retry after marking as failed" in {
      val test = for {
        store <- InMemoryIdempotencyStore.create[IO]
        first <- store
          .check("retry-key", clientId = "client-1", ttlSeconds = 3600)
        marked <- store.markFailed("retry-key", "client-1")
        retry <- store
          .check("retry-key", clientId = "client-1", ttlSeconds = 3600)
      } yield (first, marked, retry)

      test.asserting { case (first, marked, retry) =>
        first shouldBe a[IdempotencyResult.New]
        marked shouldBe true
        retry shouldBe a[IdempotencyResult.New] // Can retry after failure
      }
    }

    // Both in-memory interpreters, the one STORAGE_BACKEND=in-memory wires and
    // the test one: a late complete or fail on an expired record answered true.
    "storeResponse and markFailed refuse an expired record" in {
      val late = StoredResponse(200, "late", Map.empty, Instant.EPOCH)
      val stores = List(
        core.IdempotencyStore.inMemory[IO],
        InMemoryIdempotencyStore.create[IO].widen[core.IdempotencyStore[IO]],
      )
      stores.traverse {
        _.flatMap { store =>
          for {
            _ <- store.check("lapsed-done", clientId = "c", ttlSeconds = -60)
            completed <- store.storeResponse("lapsed-done", "c", late)
            _ <- store.check("lapsed-fail", clientId = "c", ttlSeconds = -60)
            failed <- store.markFailed("lapsed-fail", "c")
            _ <- store.check("live", clientId = "c", ttlSeconds = 3600)
            live <- store.storeResponse("live", "c", late)
          } yield (completed, failed, live)
        }
      }.asserting(_ shouldBe List.fill(2)((false, false, true)))
    }

    "markFailed applies only to its owner's pending record" in {
      val test = for {
        store <- InMemoryIdempotencyStore.create[IO]
        _ <- store.check("owned", clientId = "client-1", ttlSeconds = 3600)
        forged <- store.markFailed("owned", "client-2")
        _ <- store.storeResponse(
          "owned",
          "client-1",
          StoredResponse(200, "ok", Map.empty, Instant.EPOCH),
        )
        reopened <- store.markFailed("owned", "client-1")
        record <- store.get("owned")
      } yield (forged, reopened, record)

      test.asserting { case (forged, reopened, record) =>
        forged shouldBe false
        reopened shouldBe false
        record.map(_.status) shouldBe Some(IdempotencyStatus.Completed)
      }
    }

    "should return KeyConflict when request hash doesn't match (pending)" in {
      val test = for {
        store <- InMemoryIdempotencyStore.create[IO]
        _ <- store.check(
          "hash-key",
          clientId = "client-1",
          ttlSeconds = 3600,
          requestHash = Some("abc123"),
        )
        result <- store.check(
          "hash-key",
          clientId = "client-1",
          ttlSeconds = 3600,
          requestHash = Some("different-hash"),
        )
      } yield result

      test.asserting { result =>
        result shouldBe a[IdempotencyResult.KeyConflict]
        val conflict = result.asInstanceOf[IdempotencyResult.KeyConflict]
        conflict.storedHash shouldBe Some("abc123")
        conflict.incomingHash shouldBe Some("different-hash")
      }
    }

    "should return KeyConflict when request hash doesn't match (completed)" in {
      val now = Instant.now()
      val response =
        StoredResponse(201, """{"id": 1}""", Map.empty, completedAt = now)

      val test = for {
        store <- InMemoryIdempotencyStore.create[IO]
        _ <- store.check(
          "hash-complete-key",
          clientId = "client-1",
          ttlSeconds = 3600,
          requestHash = Some("original-hash"),
        )
        _ <- store.storeResponse("hash-complete-key", "client-1", response)
        result <- store.check(
          "hash-complete-key",
          clientId = "client-1",
          ttlSeconds = 3600,
          requestHash = Some("different-hash"),
        )
      } yield result

      test.asserting(result => result shouldBe a[IdempotencyResult.KeyConflict])
    }

    "should return Duplicate when request hash matches" in {
      val now = Instant.now()
      val response =
        StoredResponse(201, """{"id": 1}""", Map.empty, completedAt = now)

      val test = for {
        store <- InMemoryIdempotencyStore.create[IO]
        _ <- store.check(
          "hash-match-key",
          clientId = "client-1",
          ttlSeconds = 3600,
          requestHash = Some("same-hash"),
        )
        _ <- store.storeResponse("hash-match-key", "client-1", response)
        result <- store.check(
          "hash-match-key",
          clientId = "client-1",
          ttlSeconds = 3600,
          requestHash = Some("same-hash"),
        )
      } yield result

      test.asserting { result =>
        result shouldBe a[IdempotencyResult.Duplicate]
        val dup = result.asInstanceOf[IdempotencyResult.Duplicate]
        dup.originalResponse shouldBe defined
      }
    }

    "should return InProgress when no hash provided and record has hash" in {
      val test = for {
        store <- InMemoryIdempotencyStore.create[IO]
        _ <- store.check(
          "no-hash-key",
          clientId = "client-1",
          ttlSeconds = 3600,
          requestHash = Some("stored-hash"),
        )
        result <- store.check(
          "no-hash-key",
          clientId = "client-1",
          ttlSeconds = 3600,
          requestHash = None,
        )
      } yield result

      test.asserting(result => result shouldBe a[IdempotencyResult.InProgress])
    }

    "health check should return true" in {
      val test = for {
        store <- InMemoryIdempotencyStore.create[IO]
        healthy <- store.healthCheck
      } yield healthy

      test.asserting(healthy => healthy shouldBe Right(()))
    }
  }

  "storeResponse checks the client that created the record" - {
    // The second guard behind scoped keys (ADR-005): a client that somehow
    // reaches another's record still cannot complete it with a forged response.
    val response = StoredResponse(200, "forged", Map.empty, Instant.now())

    def refusesAnotherClient(
        store: core.IdempotencyStore[IO],
    ): IO[(Boolean, IdempotencyResult)] = for {
      _ <- store.check("owned", clientId = "owner", ttlSeconds = 3600)
      stored <- store.storeResponse("owned", "intruder", response)
      after <- store.check("owned", clientId = "owner", ttlSeconds = 3600)
    } yield (stored, after)

    "in the test interpreter" in
      InMemoryIdempotencyStore.create[IO].flatMap(refusesAnotherClient)
        .asserting { case (stored, after) =>
          stored shouldBe false
          after shouldBe a[IdempotencyResult.InProgress]
        }

    "in core's interpreter" in
      core.IdempotencyStore.inMemory[IO].flatMap(refusesAnotherClient).asserting {
        case (stored, after) =>
          stored shouldBe false
          after shouldBe a[IdempotencyResult.InProgress]
      }
  }
