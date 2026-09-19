package config

import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import pureconfig.ConfigSource
import resilience.GracefulDegradation.DegradationMode

/** degradation-mode decides what every caller sees when the shared circuit
  * breaker opens, so an unusable value must stop startup rather than quietly
  * resolving to reject-all.
  */
class DegradationModeSpec extends AnyFreeSpec with Matchers:

  "validateDegradationMode" - {

    "accepts every mode parsedDegradationMode can actually map" in
      AppConfig.validDegradationModes.foreach(mode =>
        withClue(s"mode '$mode': ")(
          AppConfig.validateDegradationMode(mode) shouldBe None,
        ),
      )

    "rejects a typo instead of letting it become reject-all" in {
      // The underscore form is the plausible slip, and the old catch-all turned
      // it into a full outage the first time the breaker tripped.
      AppConfig.validateDegradationMode("use_cached") should not be None
      AppConfig.validateDegradationMode("rejectall") should not be None
      AppConfig.validateDegradationMode("") should not be None
    }

    "names the offending value and the accepted set" in {
      val message = AppConfig.validateDegradationMode("nonsense")
        .getOrElse(fail("expected a validation error"))
      message should include("nonsense")
      message should include("reject-all")
    }

    "refuses use-cached, which had no cache and failed open" in {
      val message = AppConfig.validateDegradationMode("use-cached")
        .getOrElse(fail("use-cached must be refused"))
      message should include("failed open")
      message should include("allow-all")
    }
  }

  "AppConfig.loadFrom, over the shipped application.conf" - {

    // The real file with one override on top, the way an env var lands.
    def loadWith(overrides: String): Either[Throwable, AppConfig] = AppConfig
      .loadFrom[IO](
        ConfigSource.string(overrides).withFallback(ConfigSource.default),
      ).attempt.unsafeRunSync()

    "loads as shipped" in {
      val config = loadWith("").fold(e => fail(e.getMessage), identity)
      config.resilience.degradationMode shouldBe "reject-all"
      config.rateLimit.profiles.keySet should contain("free")
    }

    "stops on use-cached instead of starting fail-open" in {
      val error = loadWith("resilience.degradation-mode = use-cached").left
        .getOrElse(fail("use-cached loaded"))
      error.getMessage should include("use-cached")
    }

    "stops on an unknown mode instead of starting on a fallback config" in {
      loadWith("resilience.degradation-mode = nonsense").isLeft shouldBe true
    }

    "stops on an invalid profile" in {
      loadWith("rate-limit.profiles.free.capacity = 0").isLeft shouldBe true
    }
  }

  "parsedDegradationMode" - {

    "maps each accepted string to a distinct mode" in {
      def parse(mode: String): DegradationMode =
        ResilienceConfig(degradationMode = mode).parsedDegradationMode

      parse("allow-all") shouldBe DegradationMode.AllowAll
      parse("reject-all") shouldBe DegradationMode.RejectAll
    }

    "defaults to failing closed" in {
      ResilienceConfig().parsedDegradationMode shouldBe
        DegradationMode.RejectAll
    }
  }
