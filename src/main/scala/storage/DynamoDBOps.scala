package storage

import scala.concurrent.duration.*
import scala.jdk.FutureConverters.*

import org.typelevel.log4cats.Logger

import cats.effect.{Async, Temporal}
import cats.syntax.all.*
import resilience.{OCCConflictException, Retry, RetryPolicy}
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.*

/** Shared DynamoDB helper utilities used by all store implementations.
  *
  * Centralises AttributeValue construction and conditional-write wrappers so
  * each store avoids repeating the same boilerplate. Both wrappers return
  * `false` on `ConditionalCheckFailedException` rather than propagating the
  * exception, keeping OCC retry logic uniform across callers.
  */
object DynamoDBOps:

  def attr(s: String): AttributeValue = AttributeValue.builder().s(s).build()

  def attrN(n: Long): AttributeValue = AttributeValue.builder().n(n.toString)
    .build()

  def attrND(d: Double): AttributeValue = AttributeValue.builder().n(d.toString)
    .build()

  def attrBool(b: Boolean): AttributeValue = AttributeValue.builder().bool(b)
    .build()

  /** Execute a conditional PutItem; returns false on condition failure. */
  def conditionalPut[F[_]: Async](
      client: DynamoDbAsyncClient,
      request: PutItemRequest,
  ): F[Boolean] = Async[F].fromCompletableFuture(
    Async[F].delay(client.putItem(request).toCompletableFuture),
  ).map(_ => true).recover { case _: ConditionalCheckFailedException => false }

  /** Execute a conditional UpdateItem; returns false on condition failure. */
  def conditionalUpdate[F[_]: Async](
      client: DynamoDbAsyncClient,
      request: UpdateItemRequest,
  ): F[Boolean] = Async[F].fromCompletableFuture(
    Async[F].delay(client.updateItem(request).toCompletableFuture),
  ).map(_ => true).recover { case _: ConditionalCheckFailedException => false }

  /** OCC retry loop: run `attempt`, on false raise OCCConflictException so
    * Retry engine handles backoff; on exhaustion after retries return
    * `onExhaustion`. Delegates to Retry.withPolicy internally.
    */
  def retryOnConditionFail[F[_]: Temporal: Logger, A](
      attempt: F[Boolean],
      maxRetries: Int,
      delay: FiniteDuration = 1.millis,
  )(onSuccess: F[A])(onExhaustion: F[A])(
      onRetry: Option[F[Unit]] = None,
  ): F[A] =
    val policy = RetryPolicy(
      maxRetries = maxRetries,
      baseDelay = delay,
      maxDelay = delay * 10,
      multiplier = 1.0,
      jitterFactor = 0.0,
      retryOn = { case _: OCCConflictException => true; case _ => false },
    )
    val retryAction = onRetry.getOrElse(Temporal[F].unit)
    val op: F[A] = attempt.flatMap {
      case true => onSuccess
      case false => retryAction *>
          Temporal[F].raiseError(OCCConflictException("condition-fail", 0))
    }
    Retry.withPolicy(policy, "conditionalWrite")(op).handleErrorWith {
      case _: OCCConflictException => onExhaustion
    }

  /** A number attribute, as text. A missing attribute or one stored under
    * another type is a Left: `.n()` returns null for a string, and
    * `Double.parseDouble(null)` throws NullPointerException, which the parsers'
    * NumberFormatException handlers did not catch. A wrongly typed attribute
    * made the whole check throw instead of reading as corrupt.
    */
  def numberText(
      item: Map[String, AttributeValue],
      name: String,
  ): Either[String, String] = item.get(name)
    .toRight(s"missing '$name' attribute")
    .flatMap(av => Option(av.n()).toRight(s"'$name' is not a number attribute"))

  def longAttr(
      item: Map[String, AttributeValue],
      name: String,
  ): Either[String, Long] = numberText(item, name)
    .flatMap(v => v.toLongOption.toRight(s"'$name' is not a whole number: $v"))

  def doubleAttr(
      item: Map[String, AttributeValue],
      name: String,
  ): Either[String, Double] = numberText(item, name)
    .flatMap(v => v.toDoubleOption.toRight(s"'$name' is not a number: $v"))

  /** An item that failed to parse, kept with its raw `version` attribute so a
    * repair can be conditioned on exactly the item that was read.
    */
  final case class CorruptItem(
      rawVersion: Option[AttributeValue],
      detail: String,
  ):
    /** Version for the replacement: one past the stored one when that parses,
      * else 1. The replacement is conditioned on the raw value, so it cannot
      * overwrite a concurrent writer either way.
      */
    def nextVersion: Long = rawVersion.flatMap(v => Option(v.n()))
      .flatMap(_.toLongOption).map(_ + 1).getOrElse(1L)

  object CorruptItem:
    def of(item: Map[String, AttributeValue], detail: String): CorruptItem =
      CorruptItem(item.get("version"), detail)

  /** Replace a corrupt item with `replacement`, but only while the stored
    * `version` is still the raw value read (or still absent). Corrupt state
    * fails closed and self-heals (owner decision, PR 5): each store writes the
    * most conservative valid state here, then reads it back through the normal
    * path. The old fallback wrote with attribute_not_exists, which always fails
    * against the existing item, so a corrupt key burned its OCC retries and
    * stayed blocked until TTL.
    */
  def replaceCorrupt[F[_]: Async](
      client: DynamoDbAsyncClient,
      tableName: String,
      corrupt: CorruptItem,
      replacement: Map[String, AttributeValue],
  ): F[Boolean] =
    import scala.jdk.CollectionConverters.*
    val base = PutItemRequest.builder().tableName(tableName)
      .item(replacement.asJava)
      .expressionAttributeNames(Map("#version" -> "version").asJava)
    val request = corrupt.rawVersion match
      case Some(raw) => base.conditionExpression("#version = :raw")
          .expressionAttributeValues(Map(":raw" -> raw).asJava).build()
      case None => base.conditionExpression(
          "attribute_exists(pk) AND attribute_not_exists(#version)",
        ).build()
    conditionalPut(client, request)

  /** Standard DynamoDB table health check via describeTable. */
  def dynamoHealthCheck[F[_]: Async](
      client: DynamoDbAsyncClient,
      tableName: String,
  ): F[Either[String, Unit]] = Async[F].fromCompletableFuture(Async[F].delay(
    client
      .describeTable(DescribeTableRequest.builder().tableName(tableName).build())
      .toCompletableFuture,
  )).map(_ => Right(())).handleError(e => Left(e.getMessage))
