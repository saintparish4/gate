package config

import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import pureconfig.ConfigSource

/** Numeric settings that no request can use stop startup. A profile's
  * ttl-seconds was not checked, but RateLimitProfile requires it positive, so
  * ttl-seconds = 0 started fine and every request using it answered 500.
  */
class ConfigValidationSpec extends AnyFreeSpec with Matchers:

  private def loadWith(overrides: String): Either[Throwable, AppConfig] =
    AppConfig.loadFrom[IO](
      ConfigSource.string(overrides)
        .withFallback(ConfigSource.string("security.allow-built-in-keys = true"))
        .withFallback(ConfigSource.default),
    ).attempt.unsafeRunSync()

  private def refused(overrides: String): String = loadWith(overrides).left
    .getOrElse(fail(s"loaded: $overrides")).getMessage

  "the shipped configuration loads" in { loadWith("").isRight shouldBe true }

  "a profile with ttl-seconds = 0 stops startup, naming the profile" in {
    val message = refused("rate-limit.profiles.free.ttl-seconds = 0")
    message should include("profile 'free'")
    message should include("ttlSeconds must be > 0")
  }

  "a negative default ttl stops startup" in {
    refused("rate-limit.default-ttl-seconds = -1") should
      include("profile 'default': ttlSeconds must be > 0")
  }

  "a default capacity of zero stops startup" in {
    refused("rate-limit.default-capacity = 0") should
      include("profile 'default': capacity must be >= 1")
  }

  "an idempotency ttl of zero stops startup" in {
    refused("idempotency.default-ttl-seconds = 0") should
      include("idempotency.default-ttl-seconds must be > 0")
    refused("idempotency.max-ttl-seconds = 0") should
      include("idempotency.max-ttl-seconds must be > 0")
  }
