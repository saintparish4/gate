package core

import java.time.Instant

import cats.effect.*
import cats.syntax.all.*

/** Result of an idempotency check.
  */
sealed trait IdempotencyResult

object IdempotencyResult:
  /** First time seeing this idempotency key - proceed with the operation.
    */
  case class New(idempotencyKey: String, createdAt: Instant)
      extends IdempotencyResult

  /** Duplicate request detected - return cached response.
    */
  case class Duplicate(
      idempotencyKey: String,
      originalResponse: Option[StoredResponse],
      firstSeenAt: Instant,
  ) extends IdempotencyResult

  /** Operation is currently in progress.
    */
  case class InProgress(idempotencyKey: String, startedAt: Instant)
      extends IdempotencyResult

  /** Idempotency key exists but the request body hash doesn't match. Caller
    * should respond 409 Conflict.
    */
  case class KeyConflict(
      idempotencyKey: String,
      storedHash: Option[String],
      incomingHash: Option[String],
  ) extends IdempotencyResult

/** Stored response from a previous idempotent operation.
  */
case class StoredResponse(
    statusCode: Int,
    body: String,
    headers: Map[String, String],
    completedAt: Instant,
)

/** Idempotency record stored in the backend.
  */
case class IdempotencyRecord(
    idempotencyKey: String,
    clientId: String,
    status: IdempotencyStatus,
    response: Option[StoredResponse],
    createdAt: Instant,
    updatedAt: Instant,
    ttl: Long,
    version: Long,
    requestHash: Option[String] = None,
):
  /** Past its TTL (epoch seconds, as DynamoDB TTL reads it). DynamoDB deletes
    * an expired item lazily, often days later, so every read and claim checks
    * this itself.
    */
  def expired(now: Instant): Boolean = ttl < now.getEpochSecond

sealed trait IdempotencyStatus
object IdempotencyStatus:
  case object Pending extends IdempotencyStatus
  case object Completed extends IdempotencyStatus
  case object Failed extends IdempotencyStatus

/** Trait for idempotency storage backends.
  *
  * Implements first-writer-wins semantics to ensure that only one instance of
  * an operation is executed, even under concurrent requests.
  *
  * @tparam F
  *   the effect type
  */
trait IdempotencyStore[F[_]]:
  /** Check if an operation with this idempotency key has been seen before.
    *
    * Uses first-writer-wins semantics:
    *   - If key doesn't exist: create it with Pending status, return New
    *   - If key exists with Pending: return InProgress (or KeyConflict if
    *     requestHash differs)
    *   - If key exists with Completed: return Duplicate with stored response
    *     (or KeyConflict if requestHash differs)
    *   - If key exists with Failed: allow retry, create new Pending record,
    *     return New
    *
    * When both caller and stored record have a requestHash, a mismatch yields
    * KeyConflict (caller should respond 409).
    *
    * @param idempotencyKey
    *   Unique key for this operation
    * @param clientId
    *   Client making the request. Recorded on the record, and `storeResponse`
    *   only completes a record whose client matches.
    * @param ttlSeconds
    *   Time-to-live for the record
    * @param requestHash
    *   Optional hash of the request body; if present and differing from stored,
    *   returns KeyConflict
    * @return
    *   Result indicating if this is new, duplicate, in-progress, or key
    *   conflict
    */
  def check(
      idempotencyKey: String,
      clientId: String,
      ttlSeconds: Long,
      requestHash: Option[String] = None,
  ): F[IdempotencyResult]

  /** Store the response for a completed operation.
    *
    * Should only be called after check() returns New and the operation has
    * completed successfully. Stores only when the record is Pending and was
    * created by `clientId`. Keys are already scoped per client (ADR-005); this
    * is the second guard, so a key-construction bug still cannot let one client
    * complete another's record with a forged response.
    *
    * @param idempotencyKey
    *   The key from the original check
    * @param clientId
    *   The client completing it; must match the client that created it
    * @param response
    *   The response to store
    * @return
    *   true if stored successfully
    */
  def storeResponse(
      idempotencyKey: String,
      clientId: String,
      response: StoredResponse,
  ): F[Boolean]

  /** Mark a pending operation as failed, so the next check with this key claims
    * it again and the caller can retry.
    *
    * Like `storeResponse`, it applies only to a Pending record created by
    * `clientId`. A Completed record is never reopened: its operation ran, and
    * reopening it would let a retry run it twice.
    *
    * @param idempotencyKey
    *   The key from the original check
    * @param clientId
    *   The client failing it; must match the client that created it
    * @return
    *   true if marked; false if the record is missing, not Pending, or owned by
    *   another client
    */
  def markFailed(idempotencyKey: String, clientId: String): F[Boolean]

  /** Get the current status of an idempotency key.
    *
    * @param idempotencyKey
    *   The key to check
    * @return
    *   The record if it exists and has not expired; an expired record reads as
    *   absent, whether or not the backend has deleted it yet
    */
  def get(idempotencyKey: String): F[Option[IdempotencyRecord]]

  /** Health check for the storage backend.
    *
    * @return
    *   `Right(())` if healthy; `Left(reason)` with a human-readable description
    *   of the failure. Never raises an exception — failures are encoded in the
    *   return type.
    */
  def healthCheck: F[Either[String, Unit]]

object IdempotencyStore:
  /** Create an in-memory store for testing.
    */
  def inMemory[F[_]: Temporal]: F[IdempotencyStore[F]] =
    import cats.effect.Ref
    import cats.effect.Clock

    Ref.of[F, Map[String, IdempotencyRecord]](Map.empty).map { stateRef =>
      new IdempotencyStore[F]:
        override def check(
            idempotencyKey: String,
            clientId: String,
            ttlSeconds: Long,
            requestHash: Option[String] = None,
        ): F[IdempotencyResult] =
          for
            now <- Clock[F].realTime.map(d => Instant.ofEpochMilli(d.toMillis))
            result <- stateRef.modify { records =>
              records.get(idempotencyKey).filterNot(_.expired(now)) match
                case Some(existing) => existing.status match
                    case IdempotencyStatus.Pending =>
                      val conflict =
                        for
                          incoming <- requestHash
                          stored <- existing.requestHash if incoming != stored
                        yield ()
                      conflict match
                        case Some(_) => (
                            records,
                            IdempotencyResult.KeyConflict(
                              idempotencyKey,
                              existing.requestHash,
                              requestHash,
                            ),
                          )
                        case None => (
                            records,
                            IdempotencyResult
                              .InProgress(idempotencyKey, existing.createdAt),
                          )
                    case IdempotencyStatus.Completed =>
                      val conflict =
                        for
                          incoming <- requestHash
                          stored <- existing.requestHash if incoming != stored
                        yield ()
                      conflict match
                        case Some(_) => (
                            records,
                            IdempotencyResult.KeyConflict(
                              idempotencyKey,
                              existing.requestHash,
                              requestHash,
                            ),
                          )
                        case None => (
                            records,
                            IdempotencyResult.Duplicate(
                              idempotencyKey,
                              existing.response,
                              existing.createdAt,
                            ),
                          )
                    case IdempotencyStatus.Failed =>
                      // Allow retry on failed
                      val newRecord = IdempotencyRecord(
                        idempotencyKey = idempotencyKey,
                        clientId = clientId,
                        status = IdempotencyStatus.Pending,
                        response = None,
                        createdAt = now,
                        updatedAt = now,
                        ttl = now.getEpochSecond + ttlSeconds,
                        version = existing.version + 1,
                        requestHash = requestHash,
                      )
                      (
                        records + (idempotencyKey -> newRecord),
                        IdempotencyResult.New(idempotencyKey, now),
                      )

                case None =>
                  val newRecord = IdempotencyRecord(
                    idempotencyKey = idempotencyKey,
                    clientId = clientId,
                    status = IdempotencyStatus.Pending,
                    response = None,
                    createdAt = now,
                    updatedAt = now,
                    ttl = now.getEpochSecond + ttlSeconds,
                    version = 1,
                    requestHash = requestHash,
                  )
                  (
                    records + (idempotencyKey -> newRecord),
                    IdempotencyResult.New(idempotencyKey, now),
                  )
            }
          yield result

        override def storeResponse(
            idempotencyKey: String,
            clientId: String,
            response: StoredResponse,
        ): F[Boolean] =
          for
            now <- Clock[F].realTime.map(d => Instant.ofEpochMilli(d.toMillis))
            result <- stateRef.modify(records =>
              records.get(idempotencyKey) match
                case Some(existing)
                    if existing.status == IdempotencyStatus.Pending &&
                      existing.clientId == clientId =>
                  val updated = existing.copy(
                    status = IdempotencyStatus.Completed,
                    response = Some(response),
                    updatedAt = now,
                  )
                  (records + (idempotencyKey -> updated), true)
                case _ => (records, false),
            )
          yield result

        override def markFailed(
            idempotencyKey: String,
            clientId: String,
        ): F[Boolean] =
          for
            now <- Clock[F].realTime.map(d => Instant.ofEpochMilli(d.toMillis))
            result <- stateRef.modify(records =>
              records.get(idempotencyKey) match
                case Some(existing)
                    if existing.status == IdempotencyStatus.Pending &&
                      existing.clientId == clientId =>
                  val updated = existing
                    .copy(status = IdempotencyStatus.Failed, updatedAt = now)
                  (records + (idempotencyKey -> updated), true)
                case _ => (records, false),
            )
          yield result

        override def get(idempotencyKey: String): F[Option[IdempotencyRecord]] =
          for
            now <- Clock[F].realTime.map(d => Instant.ofEpochMilli(d.toMillis))
            records <- stateRef.get
          yield records.get(idempotencyKey).filterNot(_.expired(now))

        override def healthCheck: F[Either[String, Unit]] = Temporal[F]
          .pure(Right(()))
    }
