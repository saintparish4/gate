package aggregator

import java.time.Instant

import org.scalacheck.Gen
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import io.circe.syntax.*
import events.RateLimitEvent

class UsageSpec extends AnyFreeSpec with Matchers with ScalaCheckPropertyChecks:

  private val at = Instant.parse("2026-10-20T14:05:42.123Z")
  private val minute = Instant.parse("2026-10-20T14:05:00Z")

  // Encoded by the service's own encoder, so a renamed field or event type
  // fails here rather than as a silent zero in production.
  private def encoded(event: RateLimitEvent): Array[Byte] = event.asJson
    .noSpaces.getBytes("UTF-8")

  "Usage.parse reads what the service publishes" - {
    "an allowed decision adds one request and its cost" in {
      Usage.parse(encoded(
        RateLimitEvent.Allowed(at, "key_1", "client-a", "/x", 7, 3, "basic"),
      )) shouldBe Parsed.Usage(
        "rate_limit_allowed",
        BucketKey("client-a", minute),
        Counts(allowed = 1, allowedCost = 3),
      )
    }
    "a rejected decision adds one rejection" in {
      Usage.parse(encoded(RateLimitEvent.Rejected(
        at,
        "key_1",
        "client-a",
        "/x",
        2,
        "rate_limit_exceeded",
        "basic",
      ))) shouldBe Parsed.Usage(
        "rate_limit_rejected",
        BucketKey("client-a", minute),
        Counts(rejected = 1),
      )
    }
    "a reconcile adds the actual tokens, not the deltas" in {
      Usage.parse(encoded(
        RateLimitEvent
          .QuotaReconciled(at, "key_1", "client-a", "r1", 80, 10, -20, 10),
      )) shouldBe Parsed.Usage(
        "quota_reconciled",
        BucketKey("client-a", minute),
        Counts(
          quotaReconciles = 1,
          quotaInputTokens = 80,
          quotaOutputTokens = 10,
        ),
      )
    }
    "a quota refusal adds one refusal under the client" in {
      Usage.parse(encoded(RateLimitEvent.TokenQuotaExceeded(
        at,
        "u",
        "a",
        "o",
        "user",
        100,
        120,
        "key_1",
        "client-a",
      ))) shouldBe Parsed.Usage(
        "token_quota_exceeded",
        BucketKey("client-a", minute),
        Counts(quotaExceeded = 1),
      )
    }
    "idempotency events count new keys and hits" in {
      Usage
        .parse(encoded(
          RateLimitEvent.IdempotencyNew(at, "k", "client-a", 60),
        )) shouldBe Parsed.Usage(
        "idempotency_new",
        BucketKey("client-a", minute),
        Counts(idempotencyNew = 1),
      )
      Usage
        .parse(encoded(
          RateLimitEvent.IdempotencyHit(at, "k", "client-a", at),
        )) shouldBe Parsed.Usage(
        "idempotency_hit",
        BucketKey("client-a", minute),
        Counts(idempotencyHit = 1),
      )
    }
    "an audit event is ignored: it repeats a decision already counted" in {
      Usage.parse(encoded(RateLimitEvent.AuditEvent(
        at,
        "req",
        "key_1",
        "client-a",
        "rejected",
        "r",
        None,
        None,
        None,
      ))) shouldBe Parsed.Ignored("audit")
    }
    "bytes that are not an event are unreadable, not an error" in {
      Usage.parse("nope".getBytes) shouldBe a[Parsed.Unreadable]
      Usage.parse("""{"event_type":"rate_limit_allowed"}""".getBytes) shouldBe
        a[Parsed.Unreadable]
    }
  }

  private val parsedGen: Gen[Parsed] =
    for
      client <- Gen.oneOf("a", "b", "c", "d", "e")
      minuteOffset <- Gen.choose(0, 200)
      kind <- Gen.choose(0, 3)
      n <- Gen.choose(0L, 1000L)
    yield
      val key = BucketKey.of(client, minute.plusSeconds(60L * minuteOffset))
      kind match
        case 0 => Parsed.Usage(
            "rate_limit_allowed",
            key,
            Counts(allowed = 1, allowedCost = n),
          )
        case 1 => Parsed.Usage(
            "quota_reconciled",
            key,
            Counts(quotaReconciles = 1, quotaInputTokens = n),
          )
        case 2 => Parsed.Ignored("audit")
        case _ => Parsed.Unreadable("x")

  "Consumer.chunks" - {
    "keeps every record, in order, and no chunk exceeds the bucket bound" in
      forAll(Gen.listOf(parsedGen), Gen.choose(1, 10)) { (parsed, max) =>
        val indexed = parsed.zipWithIndex.map(_.swap)
        val chunks = Consumer.chunks(indexed, max)
        chunks.flatten shouldBe indexed
        chunks.foreach(_ should not be empty)
        chunks.foreach(c => Usage.fold(c.map(_._2)).size should be <= max)
      }

    "splitting a batch anywhere sums to the same totals" in
      forAll(Gen.listOf(parsedGen), Gen.choose(1, 10)) { (parsed, max) =>
        val chunks = Consumer.chunks(parsed.map(() -> _), max)
        val summed = chunks.map(c => Usage.fold(c.map(_._2)))
          .foldLeft(Map.empty[BucketKey, Counts])((acc, m) =>
            m.foldLeft(acc) { case (a, (k, c)) =>
              a.updated(k, a.getOrElse(k, Counts.zero) + c)
            },
          )
        summed shouldBe Usage.fold(parsed)
      }
  }
