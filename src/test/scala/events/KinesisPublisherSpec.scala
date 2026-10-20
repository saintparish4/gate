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
import software.amazon.awssdk.services.kinesis.model.{
  PutRecordsRequest, PutRecordsResponse, PutRecordsResultEntry,
}

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

  /** A client whose PutRecords rejects the records `reject` picks, by call
    * number and position, and records each call's size.
    */
  private def stubClient(
      calls: java.util.concurrent.ConcurrentLinkedQueue[Int],
      reject: (Int, Int) => Boolean,
  ): KinesisAsyncClient = new KinesisAsyncClient:
    def serviceName(): String = "kinesis"
    def close(): Unit = ()
    override def putRecords(
        request: PutRecordsRequest,
    ): java.util.concurrent.CompletableFuture[PutRecordsResponse] =
      val call = calls.size
      val n = request.records.size
      calls.add(n)
      val results = (0 until n).map(i =>
        if reject(call, i) then
          PutRecordsResultEntry.builder()
            .errorCode("ProvisionedThroughputExceededException").build()
        else
          PutRecordsResultEntry.builder().sequenceNumber(s"$call-$i")
            .shardId("s").build(),
      )
      java.util.concurrent.CompletableFuture.completedFuture(
        PutRecordsResponse.builder().records(results*)
          .failedRecordCount(results.count(_.errorCode != null)).build(),
      )

  // Counts every metric by name, so published and dropped can be compared.
  private def counting(ref: Ref[IO, Map[String, Int]]): MetricsPublisher[IO] =
    new MetricsPublisher[IO]:
      def increment(name: String, dims: Map[String, String]): IO[Unit] = ref
        .update(m => m.updated(name, m.getOrElse(name, 0) + 1))
      def count(name: String, amount: Double, dims: Map[String, String]) =
        IO.unit
      def gauge(name: String, value: Double, dims: Map[String, String]) =
        IO.unit
      def recordLatency(name: String, ms: Double, dims: Map[String, String]) =
        IO.unit
      def recordRateLimitDecision(allowed: Boolean, id: String, tier: String) =
        IO.unit
      def recordCircuitBreakerState(
          name: String,
          state: String,
          failures: Int,
      ) = IO.unit
      def flush: IO[Unit] = IO.unit

  private def drained(
      queued: Int,
      reject: (Int, Int) => Boolean,
  ): IO[(List[Int], Map[String, Int])] =
    val calls = new java.util.concurrent.ConcurrentLinkedQueue[Int]()
    for
      counts <- Ref.of[IO, Map[String, Int]](Map.empty)
      queue <- Queue.circularBuffer[IO, RateLimitEvent](10_000)
      publisher = new KinesisPublisher[IO](
        stubClient(calls, reject),
        KinesisConfig("stream", enabled = true, queueSize = 10_000),
        queue,
        counting(counts),
      )
      _ <- List.fill(queued)(event).traverse_(publisher.publish)
      fiber <- publisher.drainLoop.start
      _ <- queue.size.iterateUntil(_ == 0)
      // The last batch is taken before its put completes.
      _ <- counts.get.map(c =>
        c.getOrElse("KinesisEventPublished", 0) +
          c.getOrElse("DroppedKinesisEvent", 0),
      ).iterateUntil(_ == queued)
      _ <- fiber.cancel
      result <- counts.get
    yield (calls.toArray.toList.map(_.asInstanceOf[Int]), result)

  "the drain sends what is queued in PutRecords batches of at most 500" in
    drained(1_200, (_, _) => false).asserting { case (calls, counts) =>
      calls.forall(_ <= KinesisPublisher.MaxBatch) shouldBe true
      calls.sum shouldBe 1_200
      calls.size should be <= 4
      counts.get("KinesisEventPublished") shouldBe Some(1_200)
      counts.get("DroppedKinesisEvent") shouldBe None
    }

  "records a batch rejects are sent once more, and only those" in
    // The first call rejects every third record; every later call takes all.
    drained(300, (call, i) => call == 0 && i % 3 == 0).asserting {
      case (calls, counts) =>
        calls shouldBe List(300, 100)
        counts.get("KinesisEventPublished") shouldBe Some(300)
        counts.get("DroppedKinesisEvent") shouldBe None
    }

  "records rejected twice are dropped and counted, one count each" in
    drained(10, (_, i) => i == 0).asserting { case (calls, counts) =>
      calls shouldBe List(10, 1)
      counts.get("KinesisEventPublished") shouldBe Some(9)
      counts.get("DroppedKinesisEvent") shouldBe Some(1)
    }
