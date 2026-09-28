package resilience

import java.util.concurrent.TimeoutException

import scala.concurrent.duration.FiniteDuration

import org.typelevel.log4cats.Logger

import cats.effect.*
import cats.syntax.all.*
import config.BulkheadSettings
import core.*

/** Bounds a store call: the wait for a bulkhead permit, then the call itself.
  *
  * Only the rate-limit store was wrapped. The idempotency and quota stores had
  * no timeout and no bulkhead (finding G), so a slow DynamoDB held each request
  * for the SDK's full 10 s read timeout and let callers pile up without limit.
  * A timeout here is one attempt, like the rate-limit path's: the bound covers
  * the whole call, and retrying would multiply the worst case.
  */
final class StoreGuard[F[_]: Temporal](
    name: String,
    timeout: FiniteDuration,
    bulkhead: Option[Bulkhead[F]],
):
  def apply[A](fa: F[A]): F[A] =
    val bounded = Temporal[F].timeout(fa, timeout)
      .adaptError { case _: TimeoutException =>
        GateError.StoreTimeout(name, timeout)
      }
    bulkhead.fold(bounded)(_.execute(bounded))

object StoreGuard:
  def resource[F[_]: Temporal: Logger](
      name: String,
      timeout: FiniteDuration,
      settings: BulkheadSettings,
  ): Resource[F, StoreGuard[F]] = Resource.eval(
    (if settings.enabled then
       Bulkhead[F](
         s"$name-operations",
         BulkheadConfig(settings.maxConcurrent, settings.maxWait),
       ).map(Some(_))
     else Temporal[F].pure(None)).map(new StoreGuard[F](name, timeout, _)),
  )

  def idempotency[F[_]](
      underlying: IdempotencyStore[F],
      guard: StoreGuard[F],
  ): IdempotencyStore[F] = new IdempotencyStore[F]:
    override def check(
        idempotencyKey: String,
        clientId: String,
        ttlSeconds: Long,
        requestHash: Option[String],
    ): F[IdempotencyResult] =
      guard(underlying.check(idempotencyKey, clientId, ttlSeconds, requestHash))
    override def storeResponse(
        idempotencyKey: String,
        clientId: String,
        response: StoredResponse,
    ): F[Boolean] =
      guard(underlying.storeResponse(idempotencyKey, clientId, response))
    override def markFailed(idempotencyKey: String): F[Boolean] =
      guard(underlying.markFailed(idempotencyKey))
    override def get(idempotencyKey: String): F[Option[IdempotencyRecord]] =
      guard(underlying.get(idempotencyKey))
    override def healthCheck: F[Either[String, Unit]] = underlying.healthCheck

  def tokenQuota[F[_]](
      underlying: TokenQuotaStore[F],
      guard: StoreGuard[F],
  ): TokenQuotaStore[F] = new TokenQuotaStore[F]:
    override def getQuota(pk: String): F[Option[TokenQuotaState]] =
      guard(underlying.getQuota(pk))
    override def reserve(
        targets: List[QuotaTarget],
        inputDelta: Long,
        outputDelta: Long,
        nowMs: Long,
        reservation: Option[NewReservation],
    ): F[ReserveOutcome] = guard(
      underlying.reserve(targets, inputDelta, outputDelta, nowMs, reservation),
    )
    override def reconcile(
        reservationPk: String,
        actualInput: Long,
        actualOutput: Long,
        nowMs: Long,
    ): F[ReconcileOutcome] =
      guard(underlying.reconcile(reservationPk, actualInput, actualOutput, nowMs))
    override def healthCheck: F[Either[String, Unit]] = underlying.healthCheck
