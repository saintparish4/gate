package storage

import java.time.Instant

import core.{
  IdempotencyRecord, IdempotencyResult, IdempotencyStatus, IdempotencyStore,
  StoredResponse,
}
import cats.effect.{Clock, Ref, Temporal}
import cats.syntax.all.*

/** In-memory idempotency store for testing purposes.
  */
class InMemoryIdempotencyStore[F[_]: Temporal](
    stateRef: Ref[F, Map[String, IdempotencyRecord]],
) extends IdempotencyStore[F]:

  override def check(
      idempotencyKey: String,
      clientId: String,
      ttlSeconds: Long,
      requestHash: Option[String] = None,
  ): F[IdempotencyResult] =
    for
      now <- Clock[F].realTime.map(d => Instant.ofEpochMilli(d.toMillis))
      result <- stateRef.modify { stateMap =>
        stateMap.get(idempotencyKey) match
          case Some(record) => record.status match
              case IdempotencyStatus.Pending =>
                val conflict =
                  for
                    incoming <- requestHash
                    stored <- record.requestHash if incoming != stored
                  yield ()
                conflict match
                  case Some(_) => (
                      stateMap,
                      IdempotencyResult.KeyConflict(
                        idempotencyKey,
                        record.requestHash,
                        requestHash,
                      ),
                    )
                  case None => (
                      stateMap,
                      IdempotencyResult
                        .InProgress(idempotencyKey, record.createdAt),
                    )
              case IdempotencyStatus.Completed =>
                val conflict =
                  for
                    incoming <- requestHash
                    stored <- record.requestHash if incoming != stored
                  yield ()
                conflict match
                  case Some(_) => (
                      stateMap,
                      IdempotencyResult.KeyConflict(
                        idempotencyKey,
                        record.requestHash,
                        requestHash,
                      ),
                    )
                  case None => (
                      stateMap,
                      IdempotencyResult.Duplicate(
                        idempotencyKey,
                        record.response,
                        record.createdAt,
                      ),
                    )
              case IdempotencyStatus.Failed =>
                // Allow retry on failed - create new pending record
                val newRecord = IdempotencyRecord(
                  idempotencyKey = idempotencyKey,
                  clientId = clientId,
                  status = IdempotencyStatus.Pending,
                  response = None,
                  createdAt = now,
                  updatedAt = now,
                  ttl = now.getEpochSecond + ttlSeconds,
                  version = record.version + 1,
                  requestHash = requestHash,
                )
                (
                  stateMap.updated(idempotencyKey, newRecord),
                  IdempotencyResult.New(idempotencyKey, now),
                )
          case None =>
            // Key doesn't exist - create with pending status
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
              stateMap.updated(idempotencyKey, newRecord),
              IdempotencyResult.New(idempotencyKey, now),
            )
      }
    yield result

  override def storeResponse(
      idempotencyKey: String,
      response: StoredResponse,
  ): F[Boolean] =
    for
      now <- Clock[F].realTime.map(d => Instant.ofEpochMilli(d.toMillis))
      result <- stateRef.modify(stateMap =>
        stateMap.get(idempotencyKey) match
          case Some(record) if record.status == IdempotencyStatus.Pending =>
            val updated = record.copy(
              status = IdempotencyStatus.Completed,
              response = Some(response),
              updatedAt = now,
            )
            (stateMap.updated(idempotencyKey, updated), true)
          case _ => (stateMap, false),
      )
    yield result

  override def markFailed(idempotencyKey: String): F[Boolean] =
    for
      now <- Clock[F].realTime.map(d => Instant.ofEpochMilli(d.toMillis))
      result <- stateRef.modify(stateMap =>
        stateMap.get(idempotencyKey) match
          case Some(record) =>
            val updated = record
              .copy(status = IdempotencyStatus.Failed, updatedAt = now)
            (stateMap.updated(idempotencyKey, updated), true)
          case None => (stateMap, false),
      )
    yield result

  override def get(idempotencyKey: String): F[Option[IdempotencyRecord]] =
    stateRef.get.map(_.get(idempotencyKey))

  override def healthCheck: F[Either[String, Unit]] = Temporal[F].pure(Right(()))

object InMemoryIdempotencyStore:
  def create[F[_]: Temporal]: F[InMemoryIdempotencyStore[F]] = Ref
    .of[F, Map[String, IdempotencyRecord]](Map.empty)
    .map(new InMemoryIdempotencyStore[F](_))
