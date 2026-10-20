package events

import java.time.Instant

import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import io.circe.generic.auto.*
import io.circe.parser.*
import io.circe.syntax.*

class EventsSpec extends AnyFreeSpec with Matchers:

  "RateLimitEvent" - {

    "QuotaReconciled event carries its type and the usage the aggregator sums" in {
      val event: RateLimitEvent = RateLimitEvent.QuotaReconciled(
        timestamp = Instant.ofEpochMilli(1705312200000L),
        apiKey = "key_001",
        clientId = "client-1",
        reservationId = "res-1",
        actualInputTokens = 80,
        actualOutputTokens = 10,
        inputDelta = -20,
        outputDelta = 10,
      )
      val json = event.asJson(using RateLimitEvent.given_Encoder_RateLimitEvent)
      event.partitionKey shouldBe "client-1"
      json.hcursor.get[String]("event_type").toOption shouldBe
        Some("quota_reconciled")
      json.hcursor.get[Long]("actualInputTokens").toOption shouldBe Some(80L)
      json.hcursor.get[Long]("actualOutputTokens").toOption shouldBe Some(10L)
    }

    "Allowed event" - {
      val timestamp = Instant.ofEpochMilli(1705312200000L)
      val event = RateLimitEvent.Allowed(
        timestamp = timestamp,
        apiKey = "api-key-123",
        clientId = "client-456",
        endpoint = "/api/v1/resource",
        tokensRemaining = 95,
        cost = 5,
        tier = "premium",
      )

      "should have correct event type" in {
        event.eventType shouldBe "rate_limit_allowed"
      }

      "should use apiKey as partition key" in {
        event.partitionKey shouldBe "api-key-123"
      }

      "should serialize to JSON" in {
        val json = event.asJson
        json.hcursor.get[String]("apiKey").toOption shouldBe Some("api-key-123")
        json.hcursor.get[String]("clientId").toOption shouldBe Some("client-456")
        json.hcursor.get[Int]("tokensRemaining").toOption shouldBe Some(95)
        json.hcursor.get[Int]("cost").toOption shouldBe Some(5)
        json.hcursor.get[String]("tier").toOption shouldBe Some("premium")
      }
    }

    "Rejected event" - {
      val timestamp = Instant.ofEpochMilli(1705312200000L)
      val event = RateLimitEvent.Rejected(
        timestamp = timestamp,
        apiKey = "api-key-789",
        clientId = "client-999",
        endpoint = "/api/v1/resource",
        retryAfterSeconds = 10,
        reason = "rate_limit_exceeded",
        tier = "free",
      )

      "should have correct event type" in {
        event.eventType shouldBe "rate_limit_rejected"
      }

      "should use apiKey as partition key" in {
        event.partitionKey shouldBe "api-key-789"
      }

      "should serialize to JSON" in {
        val json = event.asJson
        json.hcursor.get[Int]("retryAfterSeconds").toOption shouldBe Some(10)
        json.hcursor.get[String]("reason").toOption shouldBe
          Some("rate_limit_exceeded")
        json.hcursor.get[String]("tier").toOption shouldBe Some("free")
      }
    }

    "IdempotencyHit event" - {
      val timestamp = Instant.ofEpochMilli(1705312200000L)
      val originalTime = Instant.ofEpochMilli(1705312100000L)
      val event = RateLimitEvent.IdempotencyHit(
        timestamp = timestamp,
        idempotencyKey = "payment:order-123",
        clientId = "client-abc",
        originalRequestTime = originalTime,
      )

      "should have correct event type" in {
        event.eventType shouldBe "idempotency_hit"
      }

      "should use clientId as partition key" in {
        event.partitionKey shouldBe "client-abc"
      }

      "should serialize to JSON" in {
        val json = event.asJson
        json.hcursor.get[String]("idempotencyKey").toOption shouldBe
          Some("payment:order-123")
        json.hcursor.get[String]("clientId").toOption shouldBe Some("client-abc")
      }
    }

    "IdempotencyNew event" - {
      val timestamp = Instant.ofEpochMilli(1705312200000L)
      val event = RateLimitEvent.IdempotencyNew(
        timestamp = timestamp,
        idempotencyKey = "payment:order-456",
        clientId = "client-xyz",
        ttlSeconds = 86400,
      )

      "should have correct event type" in {
        event.eventType shouldBe "idempotency_new"
      }

      "should use clientId as partition key" in {
        event.partitionKey shouldBe "client-xyz"
      }

      "should serialize to JSON" in {
        val json = event.asJson
        json.hcursor.get[String]("idempotencyKey").toOption shouldBe
          Some("payment:order-456")
        json.hcursor.get[Long]("ttlSeconds").toOption shouldBe Some(86400)
      }
    }

    "Base RateLimitEvent encoder" - {
      val timestamp = Instant.ofEpochMilli(1L)

      "should encode Allowed events" in {
        val event: RateLimitEvent = RateLimitEvent.Allowed(
          timestamp = timestamp,
          apiKey = "key",
          clientId = "client-1",
          endpoint = "/api/test",
          tokensRemaining = 10,
          cost = 1,
          tier = "basic",
        )
        val json = event.asJson.noSpaces
        json should include("tokensRemaining")
        json should include("tier")
      }

      "should encode Rejected events" in {
        val event: RateLimitEvent = RateLimitEvent.Rejected(
          timestamp = timestamp,
          apiKey = "key",
          clientId = "client-2",
          endpoint = "/api/test",
          retryAfterSeconds = 5,
          reason = "test",
          tier = "free",
        )
        val json = event.asJson.noSpaces
        json should include("retryAfterSeconds")
        json should include("tier")
      }

      "should encode IdempotencyHit events" in {
        val originalTime = Instant.ofEpochMilli(1L)
        val event: RateLimitEvent = RateLimitEvent.IdempotencyHit(
          timestamp = timestamp,
          idempotencyKey = "idem-key",
          clientId = "client-3",
          originalRequestTime = originalTime,
        )
        val json = event.asJson.noSpaces
        json should include("idempotencyKey")
        json should include("clientId")
      }

      "should encode IdempotencyNew events" in {
        val event: RateLimitEvent = RateLimitEvent.IdempotencyNew(
          timestamp = timestamp,
          idempotencyKey = "idem-key-new",
          clientId = "client-4",
          ttlSeconds = 3600,
        )
        val json = event.asJson.noSpaces
        json should include("idempotencyKey")
        json should include("ttlSeconds")
      }
    }
  }
