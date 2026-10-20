package aggregator

import java.time.Instant

import scala.jdk.CollectionConverters.*

import cats.effect.Async
import cats.syntax.all.*
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.*

/** Usage and checkpoints in one DynamoDB table, keyed `pk`/`sk`:
  *
  *   - usage: `pk = usage#<clientId>`, `sk = <UTC minute>`, one number per
  *     field of `Counts`, and `ttl`;
  *   - checkpoints: `pk = checkpoint#<stream>`, `sk = <shardId>`, `seq`.
  *
  * They share a table because a transaction cannot span accounts or Regions,
  * and keeping them together keeps the one table the one thing to back up.
  */
final class DynamoDBUsageStore[F[_]: Async](
    client: DynamoDbAsyncClient,
    table: String,
    stream: String,
    ttlSeconds: Long,
) extends UsageStore[F]:

  private def s(v: String) = AttributeValue.builder().s(v).build()
  private def n(v: Long) = AttributeValue.builder().n(v.toString).build()

  private def call[A](f: => java.util.concurrent.CompletableFuture[A]): F[A] =
    Async[F].fromCompletableFuture(Async[F].delay(f))

  private def checkpointKey(shardId: String) =
    Map("pk" -> s(s"checkpoint#$stream"), "sk" -> s(shardId)).asJava

  private def bucketKey(key: BucketKey) =
    Map("pk" -> s(s"usage#${key.clientId}"), "sk" -> s(key.minuteLabel)).asJava

  def checkpoint(shardId: String): F[Option[String]] = call(client.getItem(
    GetItemRequest.builder().tableName(table).key(checkpointKey(shardId))
      .consistentRead(true).build(),
  )).map(r => Option(r.item).flatMap(i => Option(i.get("seq"))).map(_.s))

  def commit(
      shardId: String,
      expected: Option[String],
      next: String,
      deltas: Map[BucketKey, Counts],
  ): F[CommitResult] = Async[F].realTime.flatMap { now =>
    val expiresAt = now.toSeconds + ttlSeconds
    val buckets = deltas.toList.filter(_._2.fields.nonEmpty)
      .map { (key, counts) =>
        val fields = counts.fields.zipWithIndex
        val adds = fields.map { case ((name, _), i) => s"#f$i :f$i" }
        val names = fields.map { case ((name, _), i) => s"#f$i" -> name }.toMap
        val values = fields.map { case ((_, v), i) => s":f$i" -> n(v) }.toMap
        TransactWriteItem.builder().update(
          Update.builder().tableName(table).key(bucketKey(key))
            .updateExpression(s"ADD ${adds.mkString(", ")} SET #ttl = :ttl")
            .expressionAttributeNames((names + ("#ttl" -> "ttl")).asJava)
            .expressionAttributeValues(
              (values + (":ttl" -> n(expiresAt))).asJava,
            ).build(),
        ).build()
      }
    val condition = expected
      .fold("attribute_not_exists(seq)")(_ => "seq = :expected")
    val checkpointValues = Map(":next" -> s(next), ":at" -> n(now.toMillis)) ++
      expected.map(e => ":expected" -> s(e))
    val move = TransactWriteItem.builder().update(
      Update.builder().tableName(table).key(checkpointKey(shardId))
        .updateExpression("SET seq = :next, committedAt = :at")
        .conditionExpression(condition)
        .expressionAttributeValues(checkpointValues.asJava).build(),
    ).build()
    // The checkpoint goes last, so its cancellation reason is the last one.
    val request = TransactWriteItemsRequest.builder()
      .transactItems((buckets :+ move).asJava).build()
    call(
      client.transactWriteItems(request),
    ).as(CommitResult.Committed).recover {
      case e: TransactionCanceledException
          if Option(e.cancellationReasons).exists(reasons =>
            reasons.asScala.lastOption.exists(_.code == "ConditionalCheckFailed"),
          ) => CommitResult.Fenced
    }
  }

  def usage(key: BucketKey): F[Counts] = call(client.getItem(
    GetItemRequest.builder().tableName(table).key(bucketKey(key))
      .consistentRead(true).build(),
  )).map(r =>
    Counts.fromFields(
      Option(r.item).map(_.asScala.toMap).getOrElse(Map.empty).collect {
        case (k, v) if v.n != null => k -> v.n.toLong
      },
    ),
  )

  def healthCheck: F[Either[String, Unit]] = call(
    client.describeTable(DescribeTableRequest.builder().tableName(table).build()),
  ).as(Right(())).handleError(e => Left(e.getMessage))
