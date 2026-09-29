package config

import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import pureconfig.ConfigSource

/** An unknown algorithm or storage backend used to fall through to the token
  * bucket and DynamoDB, so a typo ran something nobody chose. Both stop startup
  * now, naming the value and the accepted set.
  */
class ChoiceValidationSpec extends AnyFreeSpec with Matchers:

  private def loadWith(overrides: String): Either[Throwable, AppConfig] =
    AppConfig.loadFrom[IO](
      ConfigSource.string(overrides)
        .withFallback(ConfigSource.string("security.allow-built-in-keys = true"))
        .withFallback(ConfigSource.default),
    ).attempt.unsafeRunSync()

  "every accepted algorithm and backend loads" in {
    for
      algorithm <- AppConfig.validAlgorithms
      backend <- AppConfig.validStorageBackends
    do
      withClue(s"$algorithm / $backend: ")(
        loadWith(
          s"""rate-limit.algorithm = "$algorithm"
             |storage.backend = "$backend"""".stripMargin,
        ).isRight shouldBe true,
      )
  }

  "an unknown algorithm stops startup" in {
    val error = loadWith("""rate-limit.algorithm = "token_bucket"""").left
      .getOrElse(fail("a typo loaded"))
    error.getMessage should include("token_bucket")
    error.getMessage should include("sliding-window")
  }

  "an unknown storage backend stops startup" in {
    val error = loadWith("""storage.backend = "dynamo"""").left
      .getOrElse(fail("a typo loaded"))
    error.getMessage should include("dynamo")
    error.getMessage should include("in-memory")
  }
