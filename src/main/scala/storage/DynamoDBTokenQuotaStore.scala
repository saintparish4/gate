package storage

import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

import org.typelevel.log4cats.Logger

import cats.effect.*
import cats.syntax.all.*
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.*
import core.{
  PlannedWrite, QuotaReservation, QuotaTarget, ReserveOutcome, TokenQuotaState,
  TokenQuotaStore,
}
import observability.MetricsPublisher
import DynamoDBOps.*

/** DynamoDB-backed token quota store.
  *
  * Table schema (gate-token-quotas):
  *   - pk (S): "{level}:{id}:{window}" e.g. "user:u123:3600s"
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
  */
class DynamoDBTokenQuotaStore[F[_]: Async](
    client: DynamoDbAsyncClient,
    tableName: String,
    logger: Logger[F],
    metrics: MetricsPublisher[F],
) extends TokenQuotaStore[F]:

  private val MaxAttempts = 25

  private case class ConditionalItem(
      item: Map[String, AttributeValue],
      condition: String,
      values: Map[String, AttributeValue],
  )

  override def getQuota(pk: String): F[Option[TokenQuotaState]] =
    val request = GetItemRequest.builder().tableName(tableName)
      .key(Map("pk" -> attr(pk)).asJava).consistentRead(true).build()

    Async[F].fromCompletableFuture(
      Async[F].delay(client.getItem(request).toCompletableFuture),
    ).flatMap(response =>
      if response.hasItem && !response.item().isEmpty then
        parseState(response.item().asScala.toMap) match
          case Right(state) => Async[F].pure(Some(state))
          case Left(err) => logger
              .error(s"Corrupt token quota state for pk=$pk: $err") *>
              metrics.increment("CorruptStateRead") *> Async[F].pure(None)
      else Async[F].pure(None),
    )

  override def reserve(
      targets: List[QuotaTarget],
      inputDelta: Long,
      outputDelta: Long,
      nowMs: Long,
  ): F[ReserveOutcome] =
    if targets.isEmpty then Async[F].pure(ReserveOutcome.Reserved(Map.empty))
    else attemptReserve(targets, inputDelta, outputDelta, nowMs, attempt = 1)

  override def healthCheck: F[Either[String, Unit]] =
    dynamoHealthCheck(client, tableName)

  private def attemptReserve(
      targets: List[QuotaTarget],
      inputDelta: Long,
      outputDelta: Long,
      nowMs: Long,
      attempt: Int,
  ): F[ReserveOutcome] =
    for
      current <- readCurrent(targets)
      outcome <- QuotaReservation
        .plan(targets, current, inputDelta, outputDelta, nowMs) match
        case Left(exceeded) => Async[F].pure(exceeded)
        case Right(planned) => write(planned, nowMs).flatMap {
            case true => Async[F].pure(ReserveOutcome.Reserved(
                planned.map(p => p.target.pk -> p.after).toMap,
              ))
            case false if attempt < MaxAttempts =>
              metrics.increment("TokenQuotaOCCRetry") *>
                jitteredBackoff(attempt) *> attemptReserve(
                  targets,
                  inputDelta,
                  outputDelta,
                  nowMs,
                  attempt + 1,
                )
            case false => logger
                .warn(s"OCC retries exhausted reserving quota for ${targets
                    .map(_.pk).mkString(", ")}")
                .as(ReserveOutcome.Contended(attempt))
          }
    yield outcome

  private def readCurrent(
      targets: List[QuotaTarget],
  ): F[Map[String, TokenQuotaState]] = targets
    .traverse(t => getQuota(t.pk).map(t.pk -> _)).map(_.collect {
      case (pk, Some(state)) => pk -> state
    }.toMap)

  private def write(planned: List[PlannedWrite], nowMs: Long): F[Boolean] =
    planned.map(conditionalItem(_, nowMs)) match
      case single :: Nil => putOne(single)
      case many => putAll(many)

  private def conditionalItem(p: PlannedWrite, nowMs: Long): ConditionalItem =
    // 60 s of grace past the window so a late reconcile still finds the item
    val ttl = nowMs / 1000 + p.target.windowSeconds + 60
    val item = Map(
      "pk" -> attr(p.target.pk),
      "input_tokens" -> attrN(p.after.inputTokens),
      "output_tokens" -> attrN(p.after.outputTokens),
      "window_start" -> attrN(p.after.windowStart),
      "version" -> attrN(p.after.version),
      "ttl" -> attrN(ttl),
    )
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

  private def putAll(items: List[ConditionalItem]): F[Boolean] =
    val actions = items.map { ci =>
      val put = Put.builder().tableName(tableName).item(ci.item.asJava)
        .conditionExpression(ci.condition)
      val withValues =
        if ci.values.isEmpty then put
        else put.expressionAttributeValues(ci.values.asJava)
      TransactWriteItem.builder().put(withValues.build()).build()
    }
    val request = TransactWriteItemsRequest.builder()
      .transactItems(actions.asJava).build()
    Async[F].fromCompletableFuture(
      Async[F].delay(client.transactWriteItems(request).toCompletableFuture),
    ).as(true).recover { case _: TransactionCanceledException => false }

  private def jitteredBackoff(attempt: Int): F[Unit] =
    val baseMs = math.min(1L << attempt, 64L)
    Async[F].delay(scala.util.Random.nextLong(baseMs + 1))
      .flatMap(jitter => Async[F].sleep(jitter.millis))

  private def parseState(
      item: Map[String, AttributeValue],
  ): Either[String, TokenQuotaState] =
    try
      val inputTokens = item.get("input_tokens")
        .toRight("missing 'input_tokens'").map(_.n().toLong)
      val outputTokens = item.get("output_tokens")
        .toRight("missing 'output_tokens'").map(_.n().toLong)
      val windowStart = item.get("window_start")
        .toRight("missing 'window_start'").map(_.n().toLong)
      val version = item.get("version").toRight("missing 'version'")
        .map(_.n().toLong)
      for
        i <- inputTokens
        o <- outputTokens
        w <- windowStart
        v <- version
      yield TokenQuotaState(i, o, w, v)
    catch
      case e: NumberFormatException =>
        Left(s"malformed numeric attribute: ${e.getMessage}")

object DynamoDBTokenQuotaStore:
  def apply[F[_]: Async](
      client: DynamoDbAsyncClient,
      tableName: String,
      logger: Logger[F],
      metrics: MetricsPublisher[F],
  ): DynamoDBTokenQuotaStore[F] =
    new DynamoDBTokenQuotaStore[F](client, tableName, logger, metrics)
