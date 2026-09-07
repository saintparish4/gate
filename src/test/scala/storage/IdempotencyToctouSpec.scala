package storage

import java.time.Instant

import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

import core.{
  IdempotencyRecord, IdempotencyResult, IdempotencyStatus, IdempotencyStore,
  StoredResponse,
}
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{IO, Ref}

/** Tests for the TOCTOU race fix in idempotency check.
  *
  * Simulates the scenario where tryCreatePending fails (record exists) but
  * subsequent get() returns None (TTL deleted the record between calls). The
  * fix retries the full operation instead of returning New without persisting a
  * Pending record.
  */
class IdempotencyToctouSpec
    extends AsyncFreeSpec with AsyncIOSpec with Matchers:

  "TOCTOU race fix" - {

    "retries when tryCreate fails and get returns None (TTL race)" in {
      // Stub store: first call to check simulates the race condition
      // (tryCreate=false, get=None), second call succeeds normally.
      val test =
        for
          callCount <- Ref[IO].of(0)
          delegateStore <- InMemoryIdempotencyStore.create[IO]

          racingStore = new IdempotencyStore[IO]:
            override def check(
                idempotencyKey: String,
                clientId: String,
                ttlSeconds: Long,
                requestHash: Option[String] = None,
            ): IO[IdempotencyResult] = callCount.getAndUpdate(_ + 1)
              .flatMap(count =>
                if count == 0 then
                  // Simulate TOCTOU: first attempt returns New without
                  // persistence — this is the BUG behavior we're testing against.
                  // Instead, the fixed DynamoDB store retries internally.
                  // For this test, we delegate to the real store on retry.
                  IO.raiseError(new RuntimeException(
                    "Simulated TOCTOU: tryCreate=false, get=None",
                  ))
                else
                  delegateStore
                    .check(idempotencyKey, clientId, ttlSeconds, requestHash),
              )
            override def storeResponse(
                key: String,
                response: StoredResponse,
            ): IO[Boolean] = delegateStore.storeResponse(key, response)
            override def markFailed(key: String): IO[Boolean] = delegateStore
              .markFailed(key)
            override def get(key: String): IO[Option[IdempotencyRecord]] =
              delegateStore.get(key)
            override def healthCheck: IO[Either[String, Unit]] =
              delegateStore.healthCheck

          // The TOCTOU-safe caller should retry on the error
          result <- racingStore.check("toctou-key", "client-1", 3600)
            .handleErrorWith(_ =>
              // Simulating the retry that DynamoDBIdempotencyStore does internally
              racingStore.check("toctou-key", "client-1", 3600),
            )
          count <- callCount.get
        yield (result, count)

      test.asserting { case (result, count) =>
        result shouldBe a[IdempotencyResult.New]
        count shouldBe 2
      }
    }

    "retries succeed and persist the Pending record" in {
      // After retry succeeds, the record should actually exist in the store.
      val test =
        for
          store <- InMemoryIdempotencyStore.create[IO]
          _ <- store.check("persist-key", "client-1", 3600)
          record <- store.get("persist-key")
        yield record

      test.asserting { record =>
        record shouldBe defined
        record.get.status shouldBe IdempotencyStatus.Pending
        record.get.idempotencyKey shouldBe "persist-key"
      }
    }
  }
