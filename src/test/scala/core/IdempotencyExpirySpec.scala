package core

import java.time.Instant

import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec

/** The in-memory store is what STORAGE_BACKEND=in-memory wires, so it honors
  * the TTL like the DynamoDB store (see
  * DynamoDBIdempotencyStoreIntegrationSpec). A negative ttlSeconds writes a
  * record that is already expired.
  */
class IdempotencyExpirySpec
    extends AsyncFreeSpec with AsyncIOSpec with Matchers:

  "IdempotencyStore.inMemory" - {

    "reads an expired record as absent and claims it as new" in {
      val done = StoredResponse(200, "old", Map.empty, Instant.EPOCH)
      val test =
        for
          store <- IdempotencyStore.inMemory[IO]
          _ <- store.check("k", clientId = "c", ttlSeconds = -60)
          _ <- store.storeResponse("k", "c", done)
          stale <- store.get("k")
          rerun <- store.check("k", clientId = "c", ttlSeconds = 3600)
          live <- store.check("k", clientId = "c", ttlSeconds = 3600)
        yield (stale, rerun, live)

      test.asserting { case (stale, rerun, live) =>
        stale shouldBe None
        rerun shouldBe a[IdempotencyResult.New]
        live shouldBe a[IdempotencyResult.InProgress]
      }
    }
  }
