package core

import scala.concurrent.duration.FiniteDuration

/** Sealed error hieracrchy for all domain-level failures in Gate.
  *
  * Split into Retryable (caller may retry after a delay) and NonRetryable
  * (caller must not retry -- either a logic error or a hard resource limit).
  * This replaces the scattered RuntimeException subclasses in CircuitBreaker,
  * GracefulDegradation, and ResilientWrapper.
  */
sealed trait GateError extends Exception:
  override def fillInStackTrace(): Throwable = this // cheap -- no stack trace needed

object GateError:

  sealed trait Retryable extends GateError
  sealed trait NonRetryable extends GateError

  // ── Retryable ──────────────────────────────────────────────────────────────

  /** DynamoDB OCC conditional write failed. Safe to retry with backoff. */
  final case class OCCConflict(key: String, attempts: Int)
      extends RuntimeException(
        s"OCC conflict on key '$key' after $attempts attempt(s)",
      )
      with Retryable

  /** AWS SDK call exceeded the configured socket/read timeout. */
  final case class StoreTimeout(operation: String, duration: FiniteDuration)
      extends RuntimeException(
        s"Operation '$operation' timed out after $duration",
      )
      with Retryable

  // ── NonRetryable ───────────────────────────────────────────────────────────

  /** Circuit breaker is open; request was rejected without attempting the call.
    */
  final case class CircuitOpen(name: String)
      extends RuntimeException(s"Circuit breaker '$name' is open")
      with NonRetryable

  /** Bulkhead concurrency limit reached; request was shed. */
  final case class BulkheadFull(name: String)
      extends RuntimeException(s"Bulkhead '$name' is full -- request rejected")
      with NonRetryable

  /** Persisted state is structurally invalid and cannot be repaired. */
  final case class CorruptState(key: String, detail: String)
      extends RuntimeException(s"Corrupt state for key '$key': $detail")
      with NonRetryable

  /** Application configuration is invalid; surfaced at startup. */
  final case class ConfigError(message: String)
      extends RuntimeException(s"Configuration error: $message")
      with NonRetryable
