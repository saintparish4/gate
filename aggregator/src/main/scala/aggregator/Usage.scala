package aggregator

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}

import io.circe.Json

/** One client's usage in one UTC minute. The minute is the decision's, taken
  * from the event's own timestamp, so an event that arrives late still lands
  * where it happened.
  */
final case class BucketKey(clientId: String, minute: Instant):
  def minuteLabel: String = BucketKey.MinuteFormat.format(minute)

object BucketKey:
  // Sorts as text in time order, so a range query over `sk` is a time range.
  val MinuteFormat: DateTimeFormatter = DateTimeFormatter
    .ofPattern("yyyy-MM-dd'T'HH:mm'Z'").withZone(ZoneOffset.UTC)

  def of(clientId: String, at: Instant): BucketKey =
    BucketKey(clientId, Instant.ofEpochSecond(at.getEpochSecond / 60 * 60))

/** What one bucket accumulates. Every field is a sum, so buckets add in any
  * order: shards can be read in any order, and a batch can be split anywhere.
  */
final case class Counts(
    allowed: Long = 0,
    allowedCost: Long = 0,
    rejected: Long = 0,
    quotaReconciles: Long = 0,
    quotaInputTokens: Long = 0,
    quotaOutputTokens: Long = 0,
    quotaExceeded: Long = 0,
    idempotencyNew: Long = 0,
    idempotencyHit: Long = 0,
):
  def +(o: Counts): Counts = Counts(
    allowed + o.allowed,
    allowedCost + o.allowedCost,
    rejected + o.rejected,
    quotaReconciles + o.quotaReconciles,
    quotaInputTokens + o.quotaInputTokens,
    quotaOutputTokens + o.quotaOutputTokens,
    quotaExceeded + o.quotaExceeded,
    idempotencyNew + o.idempotencyNew,
    idempotencyHit + o.idempotencyHit,
  )

  /** The stored attribute names, with the non-zero values only. */
  def fields: List[(String, Long)] = List(
    "allowed" -> allowed,
    "allowedCost" -> allowedCost,
    "rejected" -> rejected,
    "quotaReconciles" -> quotaReconciles,
    "quotaInputTokens" -> quotaInputTokens,
    "quotaOutputTokens" -> quotaOutputTokens,
    "quotaExceeded" -> quotaExceeded,
    "idempotencyNew" -> idempotencyNew,
    "idempotencyHit" -> idempotencyHit,
  ).filter(_._2 != 0)

object Counts:
  val zero: Counts = Counts()

  def fromFields(fields: Map[String, Long]): Counts =
    def f(name: String) = fields.getOrElse(name, 0L)
    Counts(
      f("allowed"),
      f("allowedCost"),
      f("rejected"),
      f("quotaReconciles"),
      f("quotaInputTokens"),
      f("quotaOutputTokens"),
      f("quotaExceeded"),
      f("idempotencyNew"),
      f("idempotencyHit"),
    )

/** How one stream record was read. */
enum Parsed:
  /** A usage event: where it goes and what it adds. */
  case Usage(eventType: String, key: BucketKey, counts: Counts)

  /** A well-formed event this aggregator does not count. Audit events are here:
    * each repeats a rejection or a refusal already counted.
    */
  case Ignored(eventType: String)

  /** Not an event the service publishes. Counted, and skipped. */
  case Unreadable(reason: String)

object Usage:

  /** Reads one record's bytes, as `events.RateLimitEvent`'s encoder writes
    * them. I decode by hand, from the field names, so the aggregator does not
    * depend on the service; the contract test in this project's tests decodes
    * what the service's own encoder produces.
    */
  def parse(bytes: Array[Byte]): Parsed = io.circe.parser
    .parse(String(bytes, java.nio.charset.StandardCharsets.UTF_8))
    .fold(e => Parsed.Unreadable(s"not JSON: ${e.message}"), fromJson)

  def fromJson(json: Json): Parsed =
    val c = json.hcursor
    def str(name: String) = c.get[String](name).toOption
    def num(name: String) = c.get[Long](name).toOption
    val read =
      for
        eventType <- str("event_type").toRight("no event_type")
        at <- str("timestamp")
          .flatMap(t => scala.util.Try(Instant.parse(t)).toOption)
          .toRight("no readable timestamp")
      yield (eventType, at)
    read match
      case Left(reason) => Parsed.Unreadable(reason)
      case Right((eventType, at)) =>
        def usage(counts: Counts) = str("clientId")
          .fold(Parsed.Unreadable(s"$eventType without clientId"))(client =>
            Parsed.Usage(eventType, BucketKey.of(client, at), counts),
          )
        eventType match
          case "rate_limit_allowed" =>
            usage(Counts(allowed = 1, allowedCost = num("cost").getOrElse(0L)))
          case "rate_limit_rejected" => usage(Counts(rejected = 1))
          case "quota_reconciled" => usage(Counts(
              quotaReconciles = 1,
              quotaInputTokens = num("actualInputTokens").getOrElse(0L),
              quotaOutputTokens = num("actualOutputTokens").getOrElse(0L),
            ))
          case "token_quota_exceeded" => usage(Counts(quotaExceeded = 1))
          case "idempotency_new" => usage(Counts(idempotencyNew = 1))
          case "idempotency_hit" => usage(Counts(idempotencyHit = 1))
          case other => Parsed.Ignored(other)

  /** Sums the usage records into buckets. */
  def fold(records: Iterable[Parsed]): Map[BucketKey, Counts] = records
    .foldLeft(Map.empty[BucketKey, Counts]) {
      case (acc, Parsed.Usage(_, key, counts)) => acc
          .updated(key, acc.getOrElse(key, Counts.zero) + counts)
      case (acc, _) => acc
    }
