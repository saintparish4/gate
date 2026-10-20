package aggregator

import java.time.Instant

import scala.concurrent.duration.*
import scala.util.Random

import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.noop.NoOpLogger

import cats.effect.*
import cats.effect.std.Random as CERandom
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all.*
import io.circe.syntax.*
import events.RateLimitEvent

/** The consumer's one promise: every record that reaches the stream is in the
  * totals exactly once, through lost answers, failed reads, restarts, and a
  * second reader on the same shards.
  */
class ConsumerSpec extends AsyncFreeSpec with AsyncIOSpec with Matchers:

  given Logger[IO] = NoOpLogger[IO]

  private val base = Instant.parse("2026-10-20T14:00:00Z")

  private def seq(i: Int) = f"$i%020d"

  /** Records the service's encoder wrote, spread over clients and minutes. */
  private def records(shard: Int, n: Int, rnd: Random): Vector[StreamRecord] =
    Vector.tabulate(n) { i =>
      val at = base.plusSeconds(rnd.nextInt(600).toLong)
      val client = s"client-${rnd.nextInt(150)}"
      val event: RateLimitEvent = rnd.nextInt(4) match
        case 0 => RateLimitEvent
            .Allowed(at, "k", client, "/x", 1, 1 + rnd.nextInt(5), "basic")
        case 1 => RateLimitEvent
            .Rejected(at, "k", client, "/x", 1, "rate_limit_exceeded", "basic")
        case 2 => RateLimitEvent.QuotaReconciled(
            at,
            "k",
            client,
            s"r-$shard-$i",
            rnd.nextInt(500).toLong,
            rnd.nextInt(50).toLong,
            0,
            0,
          )
        case _ => RateLimitEvent.AuditEvent(
            at,
            "req",
            "k",
            client,
            "rejected",
            "r",
            None,
            None,
            None,
          )
      StreamRecord(seq(i), event.asJson.noSpaces.getBytes("UTF-8"))
    }

  /** A stream of closed shards held in memory. Reads fail at `readFailure`. */
  private def source(
      held: Map[String, Vector[StreamRecord]],
      readFailure: Double,
      rnd: CERandom[IO],
  ): StreamSource[IO] = new StreamSource[IO]:
    def shards: IO[List[String]] = IO.pure(held.keys.toList.sorted)
    def iterator(shardId: String, after: Option[String]): IO[String] =
      val from = after
        .fold(0)(a => held(shardId).indexWhere(_.sequenceNumber == a) + 1)
      IO.pure(s"$shardId/$from")
    def read(iterator: String, limit: Int): IO[ReadResult] =
      val Array(shardId, from) = iterator.split('/')
      val all = held(shardId)
      val start = from.toInt
      rnd.nextDouble.flatMap(r =>
        if r < readFailure then IO.raiseError(new RuntimeException("read failed"))
        else
          val batch = all.slice(start, start + limit).toList
          val end = start + batch.size
          IO.pure(
            ReadResult(batch, Option.when(end < all.size)(s"$shardId/$end"), 0L),
          ),
      )

  /** Fails before writing at `before`, and writes then loses the answer at
    * `after`: the timeout whose transaction committed anyway.
    */
  private def flaky(
      store: UsageStore[IO],
      before: Double,
      after: Double,
      rnd: CERandom[IO],
  ): UsageStore[IO] = new UsageStore[IO]:
    def checkpoint(shardId: String) = store.checkpoint(shardId)
    def commit(
        shardId: String,
        expected: Option[String],
        next: String,
        deltas: Map[BucketKey, Counts],
    ) = rnd.nextDouble.flatMap(r =>
      if r < before then
        IO.raiseError(new RuntimeException("timed out, not written"))
      else
        store.commit(shardId, expected, next, deltas).flatMap(result =>
          rnd.nextDouble.flatMap(r2 =>
            if r2 < after then
              IO.raiseError(new RuntimeException("timed out, written"))
            else IO.pure(result),
          ),
        ),
    )
    def usage(key: BucketKey) = store.usage(key)
    def healthCheck = store.healthCheck

  private val fast = Consumer.Settings(
    batchLimit = 37,
    idlePoll = 1.milli,
    errorBackoff = 1.milli,
    shardRefresh = 10.millis,
  )

  /** Runs `readers` consumers until every shard's checkpoint is its last
    * record, then compares the table with the sum of every record once.
    */
  private def exactlyOnce(seed: Long, readers: Int, faults: Boolean) =
    val rnd = Random(seed)
    val shards = (0 until 3)
      .map(s => s"shard-$s" -> records(s, 400 + rnd.nextInt(400), rnd)).toMap
    val expected = Usage.fold(shards.values.flatten.map(r => Usage.parse(r.data)))
    for
      ceRnd <- CERandom.scalaUtilRandomSeedLong[IO](seed)
      inner <- UsageStore.inMemory[IO]
      store = if faults then flaky(inner, 0.05, 0.05, ceRnd) else inner
      src = source(shards, if faults then 0.05 else 0.0, ceRnd)
      metrics = AggregatorMetrics.create(withJvm = false)
      consumers = List.fill(readers)(Consumer[IO](src, store, metrics, fast))
      done = shards.toList.forallM((id, rs) =>
        inner.checkpoint(id).map(_.contains(rs.last.sequenceNumber)),
      )
      _ <- consumers.parTraverse_(_.run)
        .race((IO.sleep(5.millis) *> done).iterateUntil(identity))
        .timeout(30.seconds)
      actual <- expected.keys.toList.traverse(k => inner.usage(k).map(k -> _))
    yield actual.toMap shouldBe expected

  "Consumer" - {
    "commits every record once with one reader and no faults" in
      exactlyOnce(seed = 1L, readers = 1, faults = false)

    "commits every record once through lost answers and failed reads" in
      (1L to 5L).toList
        .traverse_(seed => exactlyOnce(seed, readers = 1, faults = true))
        .as(succeed)

    "two readers on the same shards still count every record once" in
      (11L to 15L).toList
        .traverse_(seed => exactlyOnce(seed, readers = 2, faults = true))
        .as(succeed)
  }
