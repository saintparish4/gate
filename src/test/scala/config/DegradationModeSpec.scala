package config

import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import resilience.GracefulDegradation.DegradationMode

/** degradation-mode decides what every caller sees when the shared circuit
  * breaker opens, so an unusable value must stop startup rather than quietly
  * resolving to reject-all.
  */
class DegradationModeSpec extends AnyFreeSpec with Matchers:

  "validateDegradationMode" - {

    "accepts every mode parsedDegradationMode can actually map" in {
      AppConfig.validDegradationModes.foreach { mode =>
        withClue(s"mode '$mode': ") {
          AppConfig.validateDegradationMode(mode) shouldBe None
        }
      }
    }

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
  }

  "parsedDegradationMode" - {

    "maps each accepted string to a distinct mode" in {
      def parse(mode: String): DegradationMode =
        ResilienceConfig(degradationMode = mode).parsedDegradationMode

      parse("allow-all") shouldBe DegradationMode.AllowAll
      parse("reject-all") shouldBe DegradationMode.RejectAll
      parse("use-cached") shouldBe DegradationMode.UseCached
    }

    "defaults to failing closed" in {
      ResilienceConfig().parsedDegradationMode shouldBe
        DegradationMode.RejectAll
    }
  }
