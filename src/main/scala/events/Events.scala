package events

import java.time.Instant

import cats.effect.*
import cats.syntax.all.*
import io.circe.*
import io.circe.generic.auto.*
import io.circe.syntax.*

/** Rate limit events for analytics and monitoring.
  */
sealed trait RateLimitEvent:
  def timestamp: Instant
  def eventType: String
  def partitionKey: String
  def traceId: Option[String] = None

object RateLimitEvent:
  /** Request was allowed within rate limit.
    */
  case class Allowed(
      timestamp: Instant,
      apiKey: String,
      clientId: String,
      endpoint: String,
      tokensRemaining: Int,
      cost: Int,
      tier: String,
      override val traceId: Option[String] = None,
  ) extends RateLimitEvent:
    val eventType = "rate_limit_allowed"
    val partitionKey = apiKey

  /** Request was rejected due to rate limit exceeded.
    */
  case class Rejected(
      timestamp: Instant,
      apiKey: String,
      clientId: String,
      endpoint: String,
      retryAfterSeconds: Int,
      reason: String,
      tier: String,
      override val traceId: Option[String] = None,
  ) extends RateLimitEvent:
    val eventType = "rate_limit_rejected"
    val partitionKey = apiKey

  /** Idempotency key hit (duplicate detected).
    */
  case class IdempotencyHit(
      timestamp: Instant,
      idempotencyKey: String,
      clientId: String,
      originalRequestTime: Instant,
      override val traceId: Option[String] = None,
  ) extends RateLimitEvent:
    val eventType = "idempotency_hit"
    val partitionKey = clientId

  /** New idempotency key registered.
    */
  case class IdempotencyNew(
      timestamp: Instant,
      idempotencyKey: String,
      clientId: String,
      ttlSeconds: Long,
      override val traceId: Option[String] = None,
  ) extends RateLimitEvent:
    val eventType = "idempotency_new"
    val partitionKey = clientId

  /** Token quota exceeded for an AI workload. */
  case class TokenQuotaExceeded(
      timestamp: Instant,
      userId: String,
      agentId: String,
      orgId: String,
      level: String,
      limit: Long,
      used: Long,
      apiKey: String,
      override val traceId: Option[String] = None,
  ) extends RateLimitEvent:
    val eventType = "token_quota_exceeded"
    val partitionKey = userId

  /** Compliance audit event emitted for every denied/rejected/conflict
    * decision. PCI DSS 4.0.1 requires 7-year retention of all access-denied
    * events.
    */
  case class AuditEvent(
      timestamp: Instant,
      requestId: String,
      apiKey: String,
      clientId: String,
      decision: String,
      reason: String,
      endpoint: Option[String],
      sourceIp: Option[String],
      tier: Option[String],
      override val traceId: Option[String] = None,
  ) extends RateLimitEvent:
    val eventType = "audit"
    val partitionKey = clientId

  // JSON encoder for events
  private def withMetadata(
      json: Json,
      eventType: String,
      traceId: Option[String],
  ): Json =
    val base = json.deepMerge(Json.obj("event_type" -> Json.fromString(eventType)))
    traceId.fold(base)(tid =>
      base.deepMerge(Json.obj("trace_id" -> Json.fromString(tid))),
    )

  given Encoder[RateLimitEvent] = Encoder.instance { event =>
    val (json, eventType) = event match
      case e: Allowed => (e.asJson, e.eventType)
      case e: Rejected => (e.asJson, e.eventType)
      case e: IdempotencyHit => (e.asJson, e.eventType)
      case e: IdempotencyNew => (e.asJson, e.eventType)
      case e: TokenQuotaExceeded => (e.asJson, e.eventType)
      case e: AuditEvent => (e.asJson, e.eventType)
    withMetadata(json, eventType, event.traceId)
  }
