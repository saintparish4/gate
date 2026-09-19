package wiring

import scala.concurrent.duration.*

import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class ObservabilityModuleSpec extends AnyFreeSpec with Matchers:

  "cloudWatchConfig carries every metrics setting through" in {
    // Only namespace and buffer sizes used to reach the publisher, so every
    // datum said Environment=dev and the flush settings were fixed.
    val configured = config.MetricsConfig(
      namespace = "RateLimiter/demo",
      environment = "demo",
      flushInterval = 15.seconds,
      highResolution = true,
      maxBufferSize = 123,
      flushThreshold = 45,
    )
    val cw = ObservabilityModule.cloudWatchConfig(configured)
    (
      cw.namespace,
      cw.environment,
      cw.flushInterval,
      cw.highResolution,
      cw.maxBufferSize,
      cw.flushThreshold,
    ) shouldBe ("RateLimiter/demo", "demo", 15.seconds, true, 123, 45)
  }
