package core

import java.lang.reflect.{InvocationHandler, Proxy}
import java.util.concurrent.CompletableFuture

import scala.concurrent.duration.*

import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.noop.NoOpLogger

import cats.effect.*
import cats.effect.std.Dispatcher
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.testkit.TestControl
import cats.syntax.all.*
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeValue, ConditionalCheckFailedException, GetItemResponse,
  PutItemRequest, PutItemResponse,
}
import observability.MetricsPublisher
import storage.DynamoDBRateLimitStore

/** The token bucket's ceiling, under a virtual clock and adversarial latency.
  *
  * Written as a reproduction attempt for issue #10: against real DynamoDB a
  * clean run admitted 86 tokens in a window the load simulator measured as 30.1
  * s, where capacity + rate * elapsed is 80.2. This pins the OCC and
  * token-bucket logic where no network or second clock is involved. It did not
  * reproduce the excess, and on 1 October 2026 the same kind of excess was
  * traced to the load simulator's own clock running slow.
  *
  * It drives the real `DynamoDBRateLimitStore`. It used to drive a copy: a
  * `singleAttempt` written out again in this file, "mirroring" the store's. A
  * copy keeps passing when the store changes, so the bound was proven for code
  * that does not ship. The store now runs over a DynamoDB stub that keeps one
  * item, honours the two conditions the store writes with, and delays each read
  * and write, because the gaps between capturing now, reading, and writing are
  * where a stale now could commit behind a newer writer.
  */
class TokenBucketOccBoundSpec
    extends AsyncFreeSpec with AsyncIOSpec with Matchers:

  given Logger[IO] = NoOpLogger[IO]

  private val profile =
    RateLimitProfile(capacity = 20, refillRatePerSecond = 2.0, ttlSeconds = 3600)

  /** What the stub saw: the item as it stands, and the `lastRefillMs` of the
    * first and the latest write that was accepted.
    */
  private final class Table:
    private var item: Option[java.util.Map[String, AttributeValue]] = None
    private var created: Option[Long] = None
    private var lastGrant: Long = Long.MinValue
    private var calls: Int = 0

    private def number(name: String): Option[Long] = item
      .map(_.get(name).n().toDouble.toLong)

    def version: Option[Long] = synchronized(number("version"))
    def grantWindow: (Option[Long], Long) = synchronized((created, lastGrant))

    /** The next call's place in the latency pattern. */
    def nextCall(): Int = synchronized { calls += 1; calls }

    def read(): GetItemResponse =
      synchronized(item.fold(GetItemResponse.builder().build())(
        GetItemResponse.builder().item(_).build(),
      ))

    /** Applies a conditional put the way DynamoDB does: atomically, or not at
      * all. Only the two conditions the store uses are understood; anything
      * else is refused loudly, so a change to the store's condition cannot pass
      * here by being ignored.
      */
    def put(request: PutItemRequest): Either[Throwable, PutItemResponse] =
      synchronized {
        val holds = request.conditionExpression() match
          case "attribute_not_exists(pk)" => Right(item.isEmpty)
          case "version = :expectedVersion" => Right(number("version").contains(
              request.expressionAttributeValues().get(":expectedVersion").n()
                .toLong,
            ))
          case other => Left(new UnsupportedOperationException(
              s"The stub does not understand the condition: $other",
            ))
        holds.flatMap(ok =>
          if ok then
            item = Some(request.item())
            val mark = request.item().get("lastRefillMs").n().toLong
            created = created.orElse(Some(mark))
            lastGrant = math.max(lastGrant, mark)
            Right(PutItemResponse.builder().build())
          else
            Left(
              ConditionalCheckFailedException.builder()
                .message("The conditional request failed").build(),
            ),
        )
      }

  /** A DynamoDB client over `table`. A read completes 0 to 12 ms after it is
    * asked for and a write 0 to 4 ms after, in a rotating pattern, so requests
    * commit in an order that differs from the order they captured now.
    */
  private def client(
      table: Table,
      dispatcher: Dispatcher[IO],
  ): DynamoDbAsyncClient =
    def later[A](delay: FiniteDuration)(
        result: => Either[Throwable, A],
    ): CompletableFuture[A] =
      val future = new CompletableFuture[A]()
      dispatcher.unsafeRunAndForget(
        IO.sleep(delay) *>
          IO(result.fold(future.completeExceptionally, future.complete)),
      )
      future
    Proxy.newProxyInstance(
      classOf[DynamoDbAsyncClient].getClassLoader,
      Array(classOf[DynamoDbAsyncClient]),
      new InvocationHandler:
        override def invoke(
            proxy: Object,
            method: java.lang.reflect.Method,
            args: Array[Object],
        ): Object = method.getName match
          case "getItem" =>
            later((table.nextCall() % 5 * 3).millis)(Right(table.read()))
          case "putItem" =>
            val request = args(0).asInstanceOf[PutItemRequest]
            later((table.nextCall() % 3 * 2).millis)(table.put(request))
          case "serviceName" => "DynamoDB"
          case "close" => null // void
          case other => CompletableFuture
              .failedFuture[Object](new UnsupportedOperationException(
                s"Stub does not implement: $other",
              )),
    ).asInstanceOf[DynamoDbAsyncClient]

  private final case class Counts(allowed: Long, blocked: Long)

  /** Twenty workers on one key for thirty virtual seconds, each on its own
    * cadence of 20 to 44 ms, through the real store.
    */
  private def drive: IO[(Counts, Long, Table)] = Dispatcher.parallel[IO]
    .use { dispatcher =>
      val table = new Table
      val store = DynamoDBRateLimitStore[IO](
        client(table, dispatcher),
        "rate-limits",
        MetricsPublisher.noop[IO],
      )
      val workers = 20
      val window = 30.seconds
      for
        counts <- Ref.of[IO, Counts](Counts(0, 0))
        t0 <- IO.monotonic
        _ <- IO.race(
          IO.sleep(window),
          (0 until workers).toList.parTraverse_ { i =>
            val cadence = (20 + i % 7 * 4).millis
            store.checkAndConsume("hot-key", 1, profile).flatMap {
              case _: RateLimitDecision.Allowed => counts
                  .update(c => c.copy(allowed = c.allowed + 1))
              case _: RateLimitDecision.Rejected => counts
                  .update(c => c.copy(blocked = c.blocked + 1))
            }.*>(IO.sleep(cadence)).foreverM
          },
        )
        t1 <- IO.monotonic
        c <- counts.get
      yield (c, (t1 - t0).toMillis, table)
    }

  "token bucket under OCC, through the real store" - {

    "never admits more than capacity + rate * elapsed, under adversarial latency" in
      TestControl.executeEmbed(drive).asserting { case (c, elapsedMs, table) =>
        val elapsedSec = elapsedMs / 1000.0
        // The bound the load simulator asserts on the client's window, with no
        // allowance, because virtual time is exact.
        val clientCeiling = profile.capacity +
          profile.refillRatePerSecond * elapsedSec
        // The tight bound from the server's own clock: refill can only have
        // accrued between the first and last committed write.
        val (created, lastGrant) = table.grantWindow
        val createdMs = created.getOrElse(fail("bucket was never created"))
        val serverCeiling = profile.capacity +
          profile.refillRatePerSecond * (lastGrant - createdMs) / 1000.0

        withClue(s"counts=$c elapsed=${elapsedSec}s client=$clientCeiling server=$serverCeiling: ") {
          // Not vacuous: the bucket was drained and refusals happened.
          c.blocked should be > 0L
          c.allowed should be >= profile.capacity.toLong
          c.allowed.toDouble should be <= serverCeiling
          c.allowed.toDouble should be <= clientCeiling
        }
      }

    "telescoping holds exactly: every admission is one committed write" in
      // If the core is sound, admissions and committed writes are the same
      // events, so the item's version counts them with no slack at all. If
      // allowed != version, something was admitted without writing, which is
      // the only way to over-issue. A check cancelled when the window closed
      // may have written without being counted, so the version may lead by the
      // number of workers at most, never trail.
      TestControl.executeEmbed(drive).asserting { case (c, _, table) =>
        val version = table.version.getOrElse(fail("bucket was never created"))
        withClue(s"counts=$c version=$version: ") {
          c.allowed should be <= version
          version - c.allowed should be <= 20L
        }
      }
  }
