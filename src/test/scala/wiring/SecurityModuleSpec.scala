package wiring

import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.noop.NoOpLogger

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import config.AppConfig
import pureconfig.ConfigSource

/** SecurityModule is where the key source is chosen, so it refuses the built-in
  * keys itself instead of trusting that AppConfig.validate ran.
  */
class SecurityModuleSpec extends AsyncFreeSpec with AsyncIOSpec with Matchers:

  given Logger[IO] = NoOpLogger[IO]

  private val shipped: IO[AppConfig] = AppConfig.loadFrom[IO](
    ConfigSource.string("security.allow-built-in-keys = true")
      .withFallback(ConfigSource.default),
  )

  private def withBuiltInKeys(allowed: Boolean): IO[AppConfig] = shipped
    .map(c => c.copy(security = c.security.copy(allowBuiltInKeys = allowed)))

  "serves the built-in keys when the dev flag is set" in withBuiltInKeys(true)
    .flatMap(
      SecurityModule.resource[IO](_).use(_.apiKeyStore.findByKey("admin-api-key")),
    ).asserting(_.map(_.clientName) shouldBe Some("Admin Client"))

  "refuses to start without the flag or Secrets Manager" in
    withBuiltInKeys(false).flatMap(SecurityModule.resource[IO](_).use_.attempt)
      .asserting(result =>
        result.left.getOrElse(fail("served the built-in keys without the flag"))
          .getMessage should include("ALLOW_BUILT_IN_KEYS"),
      )
