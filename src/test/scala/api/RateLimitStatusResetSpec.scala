package api

import java.time.Instant

import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

import org.http4s.Status
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.noop.NoOpLogger
import org.typelevel.otel4s.trace.Tracer

import cats.effect.*
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.testkit.TestControl
import cats.syntax.all.*
import io.circe.parser.parse
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeValue, GetItemResponse,
}
import config.{RateLimitConfig, RateLimitProfileConfig}
import core.*
import events.EventPublisher
import observability.MetricsPublisher
import storage.{
  DynamoDBRateLimitStore, DynamoDBSlidingWindowStore, LeakyBucketRateLimitStore,
}
import testutil.*

/** `GET /v1/ratelimit/status` reports the reset its store computes.
  *
  * The API used to recompute `resetAt` from `tokensRemaining` with the
  * token-bucket formula under every algorithm, and a never-seen key read
  * `Instant.now()` plus 60 s. Each real store runs here over a fixed item, on
  * TestControl's clock, and the route must repeat that store's answer.
  */
class RateLimitStatusResetSpec
    extends AsyncFreeSpec with AsyncIOSpec with Matchers:

  given Logger[IO] = NoOpLogger[IO]
  given Tracer[IO] = Tracer.noop[IO]

  // The free tier's profile, which is testClient's: 20 tokens, 2 per second,
  // and a one-hour window under the sliding window.
  private val config = RateLimitConfig(
    defaultCapacity = 50,
    defaultRefillRatePerSecond = 5.0,
    defaultTtlSeconds = 3600,
    profiles = Map("free" -> RateLimitProfileConfig(20, 2.0, 3600)),
  )
  private val profile = RateLimitApi.tierProfile(config, testClient.tier)

  private def n(value: Long): AttributeValue = AttributeValue.builder()
    .n(value.toString).build()

  private def found(item: Map[String, AttributeValue]): GetItemResponse =
    GetItemResponse.builder().item(item.asJava).build()

  private val neverSeen: GetItemResponse = GetItemResponse.builder().build()

  // Written at the epoch, where TestControl's clock starts. Both buckets store
  // their level in `tokens`.
  private val bucketItem =
    found(Map("tokens" -> n(3), "lastRefillMs" -> n(0), "version" -> n(1)))
  private val windowItem = found(Map(
    "counts" -> AttributeValue.builder().m(Map("0" -> n(5)).asJava).build(),
    "version" -> n(1),
  ))

  private val algorithms
      : List[(String, GetItemResponse, GetItemResponse => RateLimitStore[IO])] =
    List(
      (
        "token bucket",
        bucketItem,
        item =>
          DynamoDBRateLimitStore[IO](
            stubDynamoClient(item),
            "t",
            MetricsPublisher.noop[IO],
          ),
      ),
      (
        "leaky bucket",
        bucketItem,
        item =>
          LeakyBucketRateLimitStore[IO](
            stubDynamoClient(item),
            "t",
            MetricsPublisher.noop[IO],
          ),
      ),
      (
        "sliding window",
        windowItem,
        item =>
          DynamoDBSlidingWindowStore[IO](
            stubDynamoClient(item),
            "t",
            MetricsPublisher.noop[IO],
          ),
      ),
    )

  private def api(store: RateLimitStore[IO]): RateLimitApi[IO] = RateLimitApi[IO](
    store,
    EventPublisher.noop[IO],
    MetricsPublisher.noop[IO],
    config,
    Logger[IO],
    () => IO.pure("test-request-id"),
  )

  private def statusResetAt(store: RateLimitStore[IO]): IO[(Status, String)] =
    for
      response <- api(store).status("k", testClient)
      body <- response.bodyText.compile.string
      resetAt <- IO
        .fromEither(parse(body).flatMap(_.hcursor.get[String]("resetAt")))
    yield (response.status, resetAt)

  // Two seconds in, so each algorithm has refilled, leaked or aged by a
  // different amount and the three resets differ.
  private val elapsed = 2.seconds

  "GET /v1/ratelimit/status/:key resetAt" - {

    algorithms.foreach { case (name, item, makeStore) =>
      s"$name: is the store's own resetAt" in TestControl.executeEmbed {
        val store = makeStore(item)
        for
          _ <- IO.sleep(elapsed)
          own <- store.getStatus(TenantKey(testClient.clientId, "k"), profile)
          answered <- statusResetAt(store)
        yield (own.map(_.resetAt.toString), answered)
      }.asserting { case (own, (status, resetAt)) =>
        status shouldBe Status.Ok
        own shouldBe defined
        Some(resetAt) shouldBe own
      }

      s"$name: a never-seen key resets now, by the clock" in
        TestControl
          .executeEmbed(IO.sleep(elapsed) *> statusResetAt(makeStore(neverSeen)))
          .asserting(
            _ shouldBe
              (Status.Ok, Instant.ofEpochMilli(elapsed.toMillis).toString),
          )
    }

    "the three algorithms answer three different resets for the same reading" in
      TestControl.executeEmbed(IO.sleep(elapsed) *> algorithms.traverse {
        case (_, item, makeStore) => statusResetAt(makeStore(item)).map(_._2)
      }).asserting(resets => resets.distinct should have size 3)
  }
