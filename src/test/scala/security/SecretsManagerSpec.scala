package security

import java.util.concurrent.CompletableFuture

import scala.concurrent.duration.*

import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.noop.NoOpLogger

import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.testkit.TestControl
import cats.effect.{IO, Ref}
import io.circe.Decoder
import software.amazon.awssdk.services.secretsmanager.SecretsManagerAsyncClient
import software.amazon.awssdk.services.secretsmanager.model.*

/** A missing or unreadable keys secret used to fall back to the built-in keys,
  * admin included, whenever the environment was "dev", and the keys loaded
  * lazily, so a deploy with no usable keys passed every health check. Both now
  * fail loudly, at startup.
  */
class SecretsManagerSpec extends AsyncFreeSpec with AsyncIOSpec with Matchers:

  given Logger[IO] = NoOpLogger[IO]

  // Answers GetSecretValue from `answer`; nothing else is called.
  private def client(
      answer: => CompletableFuture[GetSecretValueResponse],
  ): SecretsManagerAsyncClient = new SecretsManagerAsyncClient:
    override def serviceName(): String = "secretsmanager"
    override def close(): Unit = ()
    override def getSecretValue(
        request: GetSecretValueRequest,
    ): CompletableFuture[GetSecretValueResponse] = answer

  private def secret(json: String): CompletableFuture[GetSecretValueResponse] =
    CompletableFuture.completedFuture(
      GetSecretValueResponse.builder().secretString(json).versionId("v1").build(),
    )

  private def notFound: CompletableFuture[GetSecretValueResponse] =
    CompletableFuture.failedFuture(
      ResourceNotFoundException.builder().message("no such secret").build(),
    )

  // "dev" is the environment that used to fall back to the built-in keys.
  private val devConfig = SecretsConfig(environment = "dev")

  private def apiKeys(
      answer: => CompletableFuture[GetSecretValueResponse],
  ): IO[Map[String, AuthenticatedClient]] =
    SecretsManagerStore[IO](client(answer), devConfig).flatMap(_.getApiKeys)

  "SecretsManagerStore.getApiKeys" - {

    "fails on a missing secret instead of serving the built-in keys" in
      apiKeys(notFound).attempt.asserting { result =>
        val message = result.left.getOrElse(fail("expected a failure"))
          .getMessage
        message should include("rate-limiter/dev/api-keys")
        message should include("not found")
      }

    "fails on a secret that is not a list of keys" in
      apiKeys(secret("""{"apiKey": "not-a-list"}""")).attempt
        .asserting(result =>
          result.left.getOrElse(fail("expected a failure")).getMessage should
            include("not a JSON list of keys"),
        )

    "fails on an entry without an active flag, naming the field" in
      apiKeys(secret(
        """[{"apiKey": "k", "apiKeyId": "key_1", "clientName": "C",
          |  "tier": "free", "permissions": []}]""".stripMargin,
      )).attempt.asserting(result =>
        result.left.getOrElse(fail("expected a failure")).getMessage should
          include("active"),
      )

    "returns only the active keys, with their tier and permissions" in apiKeys {
      secret {
        """[
          |  {"apiKey": "live", "apiKeyId": "key_1", "clientName": "Live",
          |   "tier": "free", "permissions": ["ratelimit_check", "admin_metrics"],
          |   "active": true},
          |  {"apiKey": "revoked", "apiKeyId": "key_2", "clientName": "Old",
          |   "tier": "basic", "permissions": ["ratelimit_check"], "active": false}
          |]""".stripMargin
      }
    }.asserting { keys =>
      keys.keySet shouldBe Set("live")
      keys("live").tier shouldBe ClientTier.Free
      keys("live").permissions shouldBe
        Set(Permission.RateLimitCheck, Permission.AdminMetrics)
    }

    "parses every route permission by its secret name" in apiKeys(secret(
      """[{"apiKey": "k", "apiKeyId": "key_1", "clientName": "C",
        |  "tier": "basic", "active": true, "permissions": [
        |    "ratelimit_check", "ratelimit_status", "idempotency_check",
        |    "idempotency_complete", "quota_check", "quota_reconcile"]}]"""
        .stripMargin,
    )).asserting(_("k").permissions shouldBe Permission.standard)
  }

  // Serves getApiKeys from a script of answers; the last one repeats.
  private def scriptedStore(
      answers: List[IO[Map[String, AuthenticatedClient]]],
  ): IO[SecretStore[IO]] = Ref
    .of[IO, List[IO[Map[String, AuthenticatedClient]]]](answers).map { script =>
      new SecretStore[IO]:
        override def getSecret(secretName: String): IO[Option[String]] = IO
          .raiseError(new UnsupportedOperationException)
        override def getSecretAs[A: Decoder](
            secretName: String,
        ): IO[Option[A]] = IO.raiseError(new UnsupportedOperationException)
        override def getApiKeys: IO[Map[String, AuthenticatedClient]] = script
          .modify {
            case next :: Nil => (next :: Nil, next)
            case next :: rest => (rest, next)
            case Nil => (Nil, IO.raiseError(new IllegalStateException("empty")))
          }.flatten
    }

  private val client1 = ApiKeyStore.testKeys("test-api-key")
  private val oneKey = Map("key-1" -> client1)

  "SecretsManagerApiKeyStore" - {

    "refuses to start when the secret cannot be read" in
      scriptedStore(
        List(IO.raiseError(new IllegalStateException("secret not found"))),
      ).flatMap(SecretsManagerApiKeyStore[IO](_)).attempt
        .asserting(_.left.map(_.getMessage) shouldBe Left("secret not found"))

    "refuses to start when the secret has no active keys" in
      scriptedStore(List(IO.pure(Map.empty)))
        .flatMap(SecretsManagerApiKeyStore[IO](_)).attempt.asserting(result =>
          result.left.getOrElse(fail("started with zero keys"))
            .getMessage should include("no active keys"),
        )

    "serves the keys it loaded at startup" in scriptedStore(List(IO.pure(oneKey)))
      .flatMap(SecretsManagerApiKeyStore[IO](_)).flatMap(_.findByKey("key-1"))
      .asserting(_ shouldBe Some(client1))

    "keeps the current keys when a refresh cannot read the secret" in
      TestControl.executeEmbed(
        for
          secrets <- scriptedStore(
            List(IO.pure(oneKey), IO.raiseError(new RuntimeException("throttled"))),
          )
          store <- SecretsManagerApiKeyStore[IO](secrets, 1.minute)
          _ <- IO.sleep(2.minutes)
          found <- store.findByKey("key-1")
        yield found,
      ).asserting(_ shouldBe Some(client1))

    "applies a refresh that revokes every key" in TestControl.executeEmbed(
      for
        secrets <- scriptedStore(List(IO.pure(oneKey), IO.pure(Map.empty)))
        store <- SecretsManagerApiKeyStore[IO](secrets, 1.minute)
        _ <- IO.sleep(2.minutes)
        found <- store.findByKey("key-1")
      yield found,
    ).asserting(_ shouldBe None)
  }
