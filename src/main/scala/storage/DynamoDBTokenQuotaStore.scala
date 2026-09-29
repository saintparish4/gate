package storage

import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

import org.typelevel.log4cats.Logger

import cats.effect.*
import cats.syntax.all.*
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.*
import io.circe.generic.auto.*
import io.circe.parser.decode
import io.circe.syntax.*
import core.{
  ChargedTarget, NewReservation, PlannedWrite, QuotaReconciliation,
  QuotaReservation, QuotaTarget, ReconcileOutcome, ReconciledUsage,
  ReservationRecord, ReserveOutcome, TokenQuotaState, TokenQuotaStore,
}
import observability.MetricsPublisher
import DynamoDBOps.*

/** DynamoDB-backed token quota store.
  *
  * Table schema (gate-token-quotas):
  *   - pk (S): "{level}:{scoped id}:{window}", where the id is scoped to the
  *     client by core.TenantKey (ADR-005), e.g.
  *     "user:t1:10:client_abc:u123:3600s"
  *   - input_tokens (N): cumulative input tokens in current window
  *   - output_tokens (N): cumulative output tokens in current window
  *   - window_start (N): epoch millis when the current window began
  *   - version (N): OCC version counter, monotonic across window rollovers
  *   - ttl (N): epoch seconds for DynamoDB TTL cleanup
  *
  * A reservation reads every target with a consistent read, checks the limits
  * against exactly that snapshot, and writes all targets with
  * version-conditional puts: one PutItem for a single target,
  * TransactWriteItems for several. Any condition failure re-reads and retries,
  * so the limit check and the write can never disagree about the state.
  *
  * An admitted check then writes its reservation record with its own
  * conditional PutItem. It used to share the counters' transaction, which made
  * every check a transaction: twice the write cost, and on LocalStack a
  * contended key stalled requests past the 10 s read timeout. The record's key
  * is unique, so this write never contends. If it cannot be written, the
  * counters are released and the check answers Contended, so no caller proceeds
  * without a reservation it can reconcile:
  *   - pk (S): "reservation:{scoped id}"
  *   - targets (S): JSON list of the counters charged and their window starts
  *   - estimated_input / estimated_output (N): the estimate reserved
  *   - status (S): "pending" or "reconciled"
  *   - actual_input / actual_output (N): the usage it was reconciled with
  *   - ttl (N): epoch seconds after which reconcile no longer finds it
  *
  * Reconcile reads the record and its counters, then writes the adjusted
  * counters and flips the record to "reconciled" in one transaction conditioned
  * on it still being "pending", so it applies at most once.
  */
class DynamoDBTokenQuotaStore[F[_]: Async](
    client: DynamoDbAsyncClient,
    tableName: String,
    logger: Logger[F],
    metrics: MetricsPublisher[F],
) extends TokenQuotaStore[F]:

  private val MaxAttempts = 25

  private sealed trait WriteItem

  private case class ConditionalItem(
      item: Map[String, AttributeValue],
      condition: String,
      values: Map[String, AttributeValue],
  ) extends WriteItem

  // Flips a pending reservation to reconciled; fails if it is not pending.
  private case class ReconciledMark(record: ReservationRecord) extends WriteItem

  override def getQuota(pk: String): F[Option[TokenQuotaState]] = readItem(pk)
    .flatMap {
      case Some(Right(state)) => Async[F].pure(Some(state))
      case Some(Left(corrupt)) => logger
          .error(s"Corrupt token quota state for pk=$pk: ${corrupt.detail}") *>
          metrics.increment("CorruptStateRead").as(None)
      case None => Async[F].pure(None)
    }

  private def readItem(
      pk: String,
  ): F[Option[Either[CorruptItem, TokenQuotaState]]] =
    val request = GetItemRequest.builder().tableName(tableName)
      .key(Map("pk" -> attr(pk)).asJava).consistentRead(true).build()
    Async[F].fromCompletableFuture(
      Async[F].delay(client.getItem(request).toCompletableFuture),
    ).map(response =>
      if response.hasItem && !response.item().isEmpty then
        val item = response.item().asScala.toMap
        Some(parseState(item).left.map(CorruptItem.of(item, _)))
      else None,
    )

  private case class Current(
      states: Map[String, TokenQuotaState],
      corrupt: Map[String, CorruptItem],
  )

  override def reserve(
      targets: List[QuotaTarget],
      inputDelta: Long,
      outputDelta: Long,
      nowMs: Long,
      reservation: Option[NewReservation],
  ): F[ReserveOutcome] =
    if targets.isEmpty then Async[F].pure(ReserveOutcome.Reserved(Map.empty))
    else
      attemptReserve(
        targets,
        inputDelta,
        outputDelta,
        nowMs,
        reservation,
        attempt = 1,
      )

  override def reconcile(
      reservationPk: String,
      actualInput: Long,
      actualOutput: Long,
      nowMs: Long,
  ): F[ReconcileOutcome] = attemptReconcile(
    reservationPk,
    actualInput,
    actualOutput,
    nowMs,
    attempt = 1,
  )

  override def healthCheck: F[Either[String, Unit]] =
    dynamoHealthCheck(client, tableName)

  private def attemptReserve(
      targets: List[QuotaTarget],
      inputDelta: Long,
      outputDelta: Long,
      nowMs: Long,
      reservation: Option[NewReservation],
      attempt: Int,
  ): F[ReserveOutcome] =
    for
      read <- readCurrent(targets)
      // A corrupt counter with a limit fails closed and self-heals: it becomes
      // a window exhausted from now, and the attempt runs again against it. A
      // counter without one (a release) cannot be healed to "exhausted"; it is
      // left out, and the next check heals it.
      healable = targets
        .filter(t => read.corrupt.contains(t.pk) && t.limit.isDefined)
      usable = targets.filterNot(t => read.corrupt.contains(t.pk))
      outcome <-
        if healable.nonEmpty then
          healable.traverse_(t => heal(t, read.corrupt(t.pk), nowMs)) *>
            (if attempt < MaxAttempts then
               attemptReserve(
                 targets,
                 inputDelta,
                 outputDelta,
                 nowMs,
                 reservation,
                 attempt + 1,
               )
             else Async[F].pure(ReserveOutcome.Contended(attempt)))
        else
          reserveUsable(
            usable,
            targets,
            read.states,
            inputDelta,
            outputDelta,
            nowMs,
            reservation,
            attempt,
          )
    yield outcome

  private def reserveUsable(
      usable: List[QuotaTarget],
      targets: List[QuotaTarget],
      current: Map[String, TokenQuotaState],
      inputDelta: Long,
      outputDelta: Long,
      nowMs: Long,
      reservation: Option[NewReservation],
      attempt: Int,
  ): F[ReserveOutcome] =
    if usable.isEmpty then Async[F].pure(ReserveOutcome.Reserved(Map.empty))
    else
      for outcome <- QuotaReservation
          .plan(usable, current, inputDelta, outputDelta, nowMs) match
          case Left(exceeded) => Async[F].pure(exceeded)
          case Right(planned) => write(planned.map(conditionalItem(_, nowMs)))
              .flatMap {
                case true =>
                  val reserved = ReserveOutcome
                    .Reserved(planned.map(p => p.target.pk -> p.after).toMap)
                  reservation match
                    case None => Async[F].pure(reserved)
                    case Some(r) => recordOrRelease(
                        r.record(planned, inputDelta, outputDelta),
                        usable,
                        nowMs,
                      ).map(recorded =>
                        if recorded then reserved
                        else ReserveOutcome.Contended(attempt),
                      )
                case false if attempt < MaxAttempts =>
                  metrics.increment("TokenQuotaOCCRetry") *>
                    jitteredBackoff(attempt) *> attemptReserve(
                      targets,
                      inputDelta,
                      outputDelta,
                      nowMs,
                      reservation,
                      attempt + 1,
                    )
                case false => logger
                    .warn(s"OCC retries exhausted reserving quota for ${targets
                        .map(_.pk).mkString(", ")}")
                    .as(ReserveOutcome.Contended(attempt))
              }
      yield outcome

  private def readCurrent(targets: List[QuotaTarget]): F[Current] = targets
    .traverse(t => readItem(t.pk).map(t.pk -> _)).map(reads =>
      Current(
        reads.collect { case (pk, Some(Right(state))) => pk -> state }.toMap,
        reads.collect { case (pk, Some(Left(corrupt))) => pk -> corrupt }.toMap,
      ),
    )

  // Corrupt counters fail closed and self-heal: replaced, conditioned on the
  // raw version read, by a window exhausted from now, so checks are refused
  // until it ends. They used to read as absent, and the attribute_not_exists
  // write that followed always failed against the existing item, ending in 503
  // Contended until TTL.
  private def heal(
      target: QuotaTarget,
      corrupt: CorruptItem,
      nowMs: Long,
  ): F[Unit] =
    val exhausted =
      TokenQuotaState(target.limit.getOrElse(0L), 0L, nowMs, corrupt.nextVersion)
    logger.error(s"Corrupt token quota state for pk=${target.pk}: ${corrupt
        .detail}; replacing it with an exhausted window") *>
      metrics.increment("CorruptStateRead") *> replaceCorrupt(
        client,
        tableName,
        corrupt,
        stateItem(target.pk, exhausted, target.windowSeconds, nowMs),
      ).flatMap(healed =>
        if healed then metrics.increment("CorruptStateHealed")
        else Async[F].unit,
      )

  private def attemptReconcile(
      reservationPk: String,
      actualInput: Long,
      actualOutput: Long,
      nowMs: Long,
      attempt: Int,
  ): F[ReconcileOutcome] = getReservation(reservationPk).flatMap {
    case None => Async[F].pure(ReconcileOutcome.NotFound)
    case Some(record) if record.expired(nowMs) =>
      // DynamoDB deletes expired items lazily, so expiry is checked here too.
      Async[F].pure(ReconcileOutcome.NotFound)
    case Some(record) if record.reconciled.isDefined =>
      Async[F].pure(ReconcileOutcome.AlreadyReconciled(record))
    case Some(record) =>
      val targets = record.targets
        .map(t => QuotaTarget(t.pk, t.windowSeconds, limit = None))
      val done = record
        .copy(reconciled = Some(ReconciledUsage(actualInput, actualOutput)))
      for
        read <- readCurrent(targets)
        // A corrupt counter is left out: its charge cannot be known, and the
        // next check heals it to an exhausted window. The reservation is still
        // marked reconciled.
        _ <-
          if read.corrupt.isEmpty then Async[F].unit
          else
            logger
              .warn(s"Reconciling $reservationPk without corrupt counters ${read
                  .corrupt.keys.mkString(", ")}") *>
              metrics.increment("CorruptStateRead")
        usable = record.copy(targets =
          record.targets.filterNot(t => read.corrupt.contains(t.pk)),
        )
        planned = QuotaReconciliation
          .plan(usable, read.states, actualInput, actualOutput, nowMs)
        written <-
          write(planned.map(conditionalItem(_, nowMs)) :+ markReconciled(done))
        outcome <-
          if written then Async[F].pure(ReconcileOutcome.Applied(done))
          else if attempt < MaxAttempts then
            // Either a counter moved or another reconcile won; the re-read
            // tells which.
            metrics.increment("TokenQuotaOCCRetry") *>
              jitteredBackoff(attempt) *> attemptReconcile(
                reservationPk,
                actualInput,
                actualOutput,
                nowMs,
                attempt + 1,
              )
          else
            logger.warn(s"OCC retries exhausted reconciling $reservationPk")
              .as(ReconcileOutcome.Contended(attempt))
      yield outcome
  }

  private def getReservation(pk: String): F[Option[ReservationRecord]] =
    val request = GetItemRequest.builder().tableName(tableName)
      .key(Map("pk" -> attr(pk)).asJava).consistentRead(true).build()
    Async[F].fromCompletableFuture(
      Async[F].delay(client.getItem(request).toCompletableFuture),
    ).flatMap(response =>
      if response.hasItem && !response.item().isEmpty then
        parseReservation(pk, response.item().asScala.toMap) match
          case Right(record) => Async[F].pure(Some(record))
          // A record that cannot be read cannot be reconciled safely; the
          // estimate stays counted, which errs toward under-issue.
          case Left(err) => logger
              .error(s"Corrupt quota reservation pk=$pk: $err") *>
              metrics.increment("CorruptStateRead") *> Async[F].pure(None)
      else Async[F].pure(None),
    )

  private def write(items: List[WriteItem]): F[Boolean] = items match
    case (single: ConditionalItem) :: Nil => putOne(single)
    case many => writeAll(many)

  // 60 s of grace past the window so a late reconcile still finds the item
  private def stateItem(
      pk: String,
      state: TokenQuotaState,
      windowSeconds: Long,
      nowMs: Long,
  ): Map[String, AttributeValue] = Map(
    "pk" -> attr(pk),
    "input_tokens" -> attrN(state.inputTokens),
    "output_tokens" -> attrN(state.outputTokens),
    "window_start" -> attrN(state.windowStart),
    "version" -> attrN(state.version),
    "ttl" -> attrN(nowMs / 1000 + windowSeconds + 60),
  )

  private def conditionalItem(p: PlannedWrite, nowMs: Long): ConditionalItem =
    val item = stateItem(p.target.pk, p.after, p.target.windowSeconds, nowMs)
    p.before match
      case Some(s) => ConditionalItem(
          item,
          "version = :expectedVersion",
          Map(":expectedVersion" -> attrN(s.version)),
        )
      case None => ConditionalItem(item, "attribute_not_exists(pk)", Map.empty)

  private def putOne(ci: ConditionalItem): F[Boolean] =
    val builder = PutItemRequest.builder().tableName(tableName)
      .item(ci.item.asJava).conditionExpression(ci.condition)
    val request =
      if ci.values.isEmpty then builder.build()
      else builder.expressionAttributeValues(ci.values.asJava).build()
    conditionalPut(client, request).recover {
      case _: TransactionConflictException => false
    }

  private def writeAll(items: List[WriteItem]): F[Boolean] =
    val actions = items.map {
      case ci: ConditionalItem =>
        val put = Put.builder().tableName(tableName).item(ci.item.asJava)
          .conditionExpression(ci.condition)
        val withValues =
          if ci.values.isEmpty then put
          else put.expressionAttributeValues(ci.values.asJava)
        TransactWriteItem.builder().put(withValues.build()).build()
      case ReconciledMark(record) =>
        val usage = record.reconciled.getOrElse(ReconciledUsage(0, 0))
        val update = Update.builder().tableName(tableName)
          .key(Map("pk" -> attr(record.pk)).asJava).updateExpression(
            "SET #status = :reconciled, actual_input = :ai, actual_output = :ao",
          ).conditionExpression("#status = :pending")
          .expressionAttributeNames(Map("#status" -> "status").asJava)
          .expressionAttributeValues(
            Map(
              ":reconciled" -> attr("reconciled"),
              ":pending" -> attr("pending"),
              ":ai" -> attrN(usage.actualInput),
              ":ao" -> attrN(usage.actualOutput),
            ).asJava,
          ).build()
        TransactWriteItem.builder().update(update).build()
    }
    val request = TransactWriteItemsRequest.builder()
      .transactItems(actions.asJava).build()
    Async[F].fromCompletableFuture(
      Async[F].delay(client.transactWriteItems(request).toCompletableFuture),
    ).as(true).recover { case _: TransactionCanceledException => false }

  private def markReconciled(record: ReservationRecord): WriteItem =
    ReconciledMark(record)

  // The counters are already charged. The record's key is unique, so a failure
  // here is transient, not contention: three tries, then the charge is given
  // back and the check refused. Even if the release fails, the caller was
  // refused and makes no call, so the stale charge can only under-issue.
  private def recordOrRelease(
      record: ReservationRecord,
      targets: List[QuotaTarget],
      nowMs: Long,
  ): F[Boolean] =
    def put(attempt: Int): F[Boolean] = putOne(newReservationItem(record))
      .handleErrorWith(e =>
        logger.warn(s"Writing quota reservation ${record
            .pk} failed (attempt $attempt): ${e.getMessage}").as(false),
      ).flatMap {
        case false if attempt < 3 => jitteredBackoff(attempt) *> put(attempt + 1)
        case recorded => Async[F].pure(recorded)
      }
    put(1).flatTap(recorded =>
      if recorded then Async[F].unit else release(record, targets, nowMs),
    )

  private def release(
      record: ReservationRecord,
      targets: List[QuotaTarget],
      nowMs: Long,
  ): F[Unit] = metrics.increment("TokenQuotaReservationReleased") *>
    attemptReserve(
      targets.map(_.copy(limit = None)),
      -record.estimatedInput,
      -record.estimatedOutput,
      nowMs,
      reservation = None,
      attempt = 1,
    ).flatMap {
      case _: ReserveOutcome.Reserved => logger
          .error(s"Could not record quota reservation ${record
              .pk}; released its charge and refused the check")
      case other =>
        logger.error(s"Could not record quota reservation ${record.pk}, and releasing its charge failed ($other); the estimate stays counted")
    }

  private def newReservationItem(record: ReservationRecord): ConditionalItem =
    ConditionalItem(
      Map(
        "pk" -> attr(record.pk),
        "targets" -> attr(record.targets.asJson.noSpaces),
        "estimated_input" -> attrN(record.estimatedInput),
        "estimated_output" -> attrN(record.estimatedOutput),
        "status" -> attr("pending"),
        "ttl" -> attrN(record.expiresAtSeconds),
      ),
      "attribute_not_exists(pk)",
      Map.empty,
    )

  private def parseReservation(
      pk: String,
      item: Map[String, AttributeValue],
  ): Either[String, ReservationRecord] =
    def num(name: String): Either[String, Long] = longAttr(item, name)
    for
      targetsJson <- item.get("targets").toRight("missing 'targets'")
        .flatMap(a => Option(a.s()).toRight("'targets' is not a string"))
      targets <- decode[List[ChargedTarget]](targetsJson).left
        .map(e => s"malformed 'targets': ${e.getMessage}")
      estimatedInput <- num("estimated_input")
      estimatedOutput <- num("estimated_output")
      ttl <- num("ttl")
      status <- item.get("status").toRight("missing 'status'")
        .flatMap(a => Option(a.s()).toRight("'status' is not a string"))
      reconciled <- status match
        case "pending" => Right(None)
        case "reconciled" =>
          for
            ai <- num("actual_input")
            ao <- num("actual_output")
          yield Some(ReconciledUsage(ai, ao))
        case other => Left(s"unknown status '$other'")
    yield ReservationRecord(
      pk,
      targets,
      estimatedInput,
      estimatedOutput,
      ttl,
      reconciled,
    )

  private def jitteredBackoff(attempt: Int): F[Unit] =
    val baseMs = math.min(1L << attempt, 64L)
    Async[F].delay(scala.util.Random.nextLong(baseMs + 1))
      .flatMap(jitter => Async[F].sleep(jitter.millis))

  private def parseState(
      item: Map[String, AttributeValue],
  ): Either[String, TokenQuotaState] =
    for
      i <- longAttr(item, "input_tokens")
      o <- longAttr(item, "output_tokens")
      w <- longAttr(item, "window_start")
      v <- longAttr(item, "version")
    yield TokenQuotaState(i, o, w, v)

object DynamoDBTokenQuotaStore:
  def apply[F[_]: Async](
      client: DynamoDbAsyncClient,
      tableName: String,
      logger: Logger[F],
      metrics: MetricsPublisher[F],
  ): DynamoDBTokenQuotaStore[F] =
    new DynamoDBTokenQuotaStore[F](client, tableName, logger, metrics)
