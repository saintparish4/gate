package events

import java.time.Instant

import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.noop.NoOpLogger

import cats.effect.std.Queue
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{IO, Ref}
import cats.syntax.all.*
import config.KinesisConfig
import observability.MetricsPublisher
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient

/** A full queue evicts its oldest event. The docs said every drop was counted,
  * but evictions were silent, so the one loss events see under load showed
  * nowhere.
  */
class KinesisPublisherSpec extends AsyncFreeSpec with AsyncIOSpec with Matchers:

  given Logger[IO] = NoOpLogger[IO]

  // Records the dimensions of every DroppedKinesisEvent; nothing else matters.
  private def recording(
      drops: Ref[IO, List[Map[String, String]]],
  ): MetricsPublisher[IO] = new MetricsPublisher[IO]:
    def increment(name: String, dims: Map[String, String]): IO[Unit] =
      if name == "DroppedKinesisEvent" then drops.update(_ :+ dims) else IO.unit
    def count(name: String, amount: Double, dims: Map[String, String]) = IO.unit
    def gauge(name: String, value: Double, dims: Map[String, String]) = IO.unit
    def recordLatency(name: String, ms: Double, dims: Map[String, String]) =
      IO.unit
    def recordRateLimitDecision(allowed: Boolean, id: String, tier: String) =
      IO.unit
    def recordCircuitBreakerState(name: String, state: String, failures: Int) =
      IO.unit
    def flush: IO[Unit] = IO.unit

  private val event = RateLimitEvent
    .IdempotencyNew(Instant.EPOCH, "key", "client", 60)

  "a full queue's evictions are counted as DroppedKinesisEvent{reason=queue_full}" in
    (for
      drops <- Ref.of[IO, List[Map[String, String]]](Nil)
      queue <- Queue.circularBuffer[IO, RateLimitEvent](2)
      // Enqueueing never touches the client; the drain loop does, and it is
      // not started here.
      publisher = new KinesisPublisher[IO](
        null.asInstanceOf[KinesisAsyncClient],
        KinesisConfig("stream", enabled = true, queueSize = 2),
        queue,
        recording(drops),
      )
      _ <- List.fill(5)(event).traverse_(publisher.publish)
      recorded <- drops.get
      held <- queue.size
    yield (recorded, held)).asserting { case (recorded, held) =>
      recorded shouldBe List.fill(3)(Map("reason" -> "queue_full"))
      held shouldBe 2
    }
