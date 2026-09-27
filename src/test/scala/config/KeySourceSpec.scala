package config

import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import pureconfig.ConfigSource

/** Every Terraform deploy used to serve the public built-in keys, admin
  * included, because Secrets Manager defaulted off and nothing else was
  * required. A service with neither Secrets Manager nor the explicit dev flag
  * must refuse to start, and say how to fix it.
  */
class KeySourceSpec extends AnyFreeSpec with Matchers:

  private def security(secrets: Boolean, builtIn: Boolean): SecurityConfig =
    SecurityConfig(
      authentication = AuthenticationConfig(),
      secrets = SecretsConfig(enabled = secrets),
      allowBuiltInKeys = builtIn,
    )

  "validateKeySource" - {

    "refuses a service with neither Secrets Manager nor the dev flag" in {
      val message = AppConfig
        .validateKeySource(security(secrets = false, builtIn = false))
        .getOrElse(fail("expected a refusal"))
      message should include("SECRETS_MANAGER_ENABLED=true")
      message should include("ALLOW_BUILT_IN_KEYS=true")
      message should include("admin key")
    }

    "accepts Secrets Manager" in {
      AppConfig
        .validateKeySource(security(secrets = true, builtIn = false)) shouldBe
        None
    }

    "accepts the built-in keys only with the explicit flag" in {
      AppConfig
        .validateKeySource(security(secrets = false, builtIn = true)) shouldBe
        None
    }
  }

  "AppConfig.loadFrom, over the shipped application.conf" - {

    def loadWith(overrides: String): Either[Throwable, AppConfig] = AppConfig
      .loadFrom[IO](
        ConfigSource.string(overrides).withFallback(ConfigSource.default),
      ).attempt.unsafeRunSync()

    "refuses to start as shipped: no Secrets Manager, no dev flag" in {
      val error = loadWith("").left
        .getOrElse(fail("started on the built-in keys without the flag"))
      error.getMessage should include("no API key source")
      error.getMessage should include("ALLOW_BUILT_IN_KEYS")
    }

    "starts with the flag docker-compose sets" in {
      val config = loadWith("security.allow-built-in-keys = true")
        .fold(e => fail(e.getMessage), identity)
      config.security.allowBuiltInKeys shouldBe true
      config.security.secrets.enabled shouldBe false
    }

    "starts with Secrets Manager, the way Terraform configures it" in {
      val config = loadWith("security.secrets.enabled = true")
        .fold(e => fail(e.getMessage), identity)
      config.security.allowBuiltInKeys shouldBe false
    }
  }
