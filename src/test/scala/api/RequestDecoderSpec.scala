package api

import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import io.circe.parser.decode

/** docs/API.md documents `cost` and `estimatedOutputTokens` as optional.
  * Circe's derived decoders ignore Scala default values, so without explicit
  * decoders a body that omits them fails to decode.
  */
class RequestDecoderSpec extends AnyFreeSpec with Matchers:

  "RateLimitCheckRequest" - {

    "defaults cost to 1 when the field is absent" in {
      decode[RateLimitCheckRequest]("""{"key": "k"}""") shouldBe
        Right(RateLimitCheckRequest(key = "k", cost = 1))
    }

    "still reads an explicit cost" in {
      decode[RateLimitCheckRequest]("""{"key": "k", "cost": 7}""")
        .map(_.cost) shouldBe Right(7)
    }

    "reads the optional profile and endpoint" in {
      decode[RateLimitCheckRequest](
        """{"key": "k", "profile": "premium", "endpoint": "/v1/chat"}""",
      ) shouldBe
        Right(RateLimitCheckRequest("k", 1, Some("premium"), Some("/v1/chat")))
    }

    "fails when the required key is absent" in {
      decode[RateLimitCheckRequest]("""{"cost": 1}""").isLeft shouldBe true
    }
  }

  "TokenQuotaCheckRequest" - {

    "defaults estimatedOutputTokens to 0 when the field is absent" in {
      decode[TokenQuotaCheckRequest](
        """{"userId": "u", "estimatedInputTokens": 10}""",
      ) shouldBe
        Right(TokenQuotaCheckRequest(userId = "u", estimatedInputTokens = 10))
    }

    "still reads an explicit estimatedOutputTokens" in {
      decode[TokenQuotaCheckRequest]("""{"userId": "u", "estimatedInputTokens": 10, "estimatedOutputTokens": 4}""")
        .map(_.estimatedOutputTokens) shouldBe Right(4L)
    }

    "reads the optional agentId and orgId" in {
      decode[TokenQuotaCheckRequest](
        """{"userId": "u", "agentId": "a", "orgId": "o", "estimatedInputTokens": 1}""",
      ) shouldBe Right(TokenQuotaCheckRequest("u", Some("a"), Some("o"), 1, 0))
    }

    "fails when the required estimatedInputTokens is absent" in {
      decode[TokenQuotaCheckRequest]("""{"userId": "u"}""").isLeft shouldBe true
    }
  }
