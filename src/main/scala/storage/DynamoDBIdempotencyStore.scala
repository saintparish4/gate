package storage

import java.time.Instant

import scala.jdk.CollectionConverters.*
import scala.jdk.FutureConverters.*

import cats.effect.*
import cats.syntax.all.*
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.*
import io.circe.*
import io.circe.generic.auto.*
import io.circe.parser.*
import io.circe.syntax.*
import core.{
  CorruptIdempotencyRecordException, IdempotencyRecord, IdempotencyResult,
  IdempotencyStatus, IdempotencyStore, StoreError, StoredResponse,
}
import DynamoDBOps.*

/** DynamoDB implementation of IdempotencyStore.
  *
  * Uses first-writer-wins semantics with conditional writes to ensure only one
  * instance processes a request with a given idempotency key.
  *
  * Table Schema:
  *   - pk (S): Partition key - "idempotency#<key>", where the key is already
  *     scoped to the client by core.TenantKey (ADR-005)
  *   - clientId (S): Client that created the record; completing it requires the
  *     same client
  *   - claimId (S): The claim that created the record (ADR-006); a complete or
  *     fail that names one must match it
  *   - status (S): Pending | Completed | Failed
  *   - response (S): JSON-encoded response (if completed)
  *   - createdAt (N): Creation timestamp
  *   - updatedAt (N): Last update timestamp
  *   - version (N): Version for OCC
  *   - ttl (N): TTL for automatic cleanup
  */
class DynamoDBIdempotencyStore[F[_]: Async](
    client: DynamoDbAsyncClient,
    tableName: String,
) extends IdempotencyStore[F]:

  override def check(
      idempotencyKey: String,
      clientId: String,
      ttlSeconds: Long,
      requestHash: Option[String] = None,
  ): F[IdempotencyResult] = checkWithRetry(
    idempotencyKey,
    clientId,
    ttlSeconds,
    requestHash,
    retries = 3,
  )

  private def checkWithRetry(
      idempotencyKey: String,
      clientId: String,
      ttlSeconds: Long,
      requestHash: Option[String],
      retries: Int,
  ): F[IdempotencyResult] =
    // Retry the full operation instead of returning New without persisting.
    def reclaim: F[IdempotencyResult] =
      if retries > 0 then
        checkWithRetry(
          idempotencyKey,
          clientId,
          ttlSeconds,
          requestHash,
          retries - 1,
        )
      else
        Async[F].raiseError(new RuntimeException(
          s"Idempotency TOCTOU race exhausted retries for key=$idempotencyKey",
        ))
    for
      now <- Clock[F].realTime.map(d => Instant.ofEpochMilli(d.toMillis))
      // A new ID per attempt: it is stored only if this attempt's claim wins.
      claimId <- IdempotencyStore.newClaimId[F]
      result <- tryCreatePending(
        idempotencyKey,
        clientId,
        now,
        ttlSeconds,
        requestHash,
        claimId,
      ).flatMap {
        case true => Async[F]
            .pure(IdempotencyResult.New(idempotencyKey, now, claimId))
        case false => get(idempotencyKey).flatMap {
            case Some(record) => record.status match
                case IdempotencyStatus.Pending => requestHash match
                    case Some(incoming) =>
                      val stored = record.requestHash
                      if stored.contains(incoming) || stored.isEmpty then
                        Async[F].pure(
                          IdempotencyResult
                            .InProgress(idempotencyKey, record.createdAt),
                        )
                      else
                        Async[F].pure(
                          IdempotencyResult
                            .KeyConflict(idempotencyKey, stored, Some(incoming)),
                        )
                    case None => Async[F].pure(
                        IdempotencyResult
                          .InProgress(idempotencyKey, record.createdAt),
                      )
                case IdempotencyStatus.Completed => requestHash match
                    case Some(incoming) =>
                      val stored = record.requestHash
                      if stored.contains(incoming) || stored.isEmpty then
                        Async[F].pure(IdempotencyResult.Duplicate(
                          idempotencyKey,
                          record.response,
                          record.createdAt,
                        ))
                      else
                        Async[F].pure(
                          IdempotencyResult
                            .KeyConflict(idempotencyKey, stored, Some(incoming)),
                        )
                    case None => Async[F].pure(IdempotencyResult.Duplicate(
                        idempotencyKey,
                        record.response,
                        record.createdAt,
                      ))
                // Failed after our claim lost to its Pending owner. This
                // answered New without claiming, so a caller whose claim
                // on the Failed record did win was also told New, and both
                // ran the operation. Exposing markFailed made it reachable.
                case IdempotencyStatus.Failed => reclaim
            // Deleted by TTL between tryCreatePending and get, or expired
            // by then (the claim condition checks an earlier now).
            case None => reclaim
          }
      }
    yield result

  override def storeResponse(
      idempotencyKey: String,
      clientId: String,
      response: StoredResponse,
      claimId: Option[String] = None,
  ): F[Boolean] =
    for
      now <- Clock[F].realTime.map(d => Instant.ofEpochMilli(d.toMillis))

      request = UpdateItemRequest.builder().tableName(tableName)
        .key(Map("pk" -> attr(s"idempotency#$idempotencyKey")).asJava)
        .updateExpression(
          "SET #status = :status, #response = :response, #updatedAt = :updatedAt, #version = #version + :one",
        ).conditionExpression(DynamoDBIdempotencyStore.endCondition(claimId))
        .expressionAttributeNames(
          (Map(
            "#status" -> "status",
            "#response" -> "response",
            "#updatedAt" -> "updatedAt",
            "#version" -> "version",
            "#clientId" -> "clientId",
            "#ttl" -> "ttl",
          ) ++ claimId.map(_ => "#claimId" -> "claimId")).asJava,
        ).expressionAttributeValues(
          (Map(
            ":status" -> attr("Completed"),
            ":response" -> attr(response.asJson.noSpaces),
            ":updatedAt" -> attrN(now.toEpochMilli),
            ":pending" -> attr("Pending"),
            ":one" -> attrN(1),
            ":clientId" -> attr(clientId),
            ":now" -> attrN(now.getEpochSecond),
          ) ++ claimId.map(id => ":claimId" -> attr(id))).asJava,
        ).build()

      result <- conditionalUpdate(client, request)
    yield result

  /** Conditioned like `storeResponse`. It was only `attribute_exists(pk)`, so
    * anything that reached it could reopen a Completed record, and the next
    * check would run the operation again.
    */
  override def markFailed(
      idempotencyKey: String,
      clientId: String,
      claimId: Option[String] = None,
  ): F[Boolean] =
    for
      now <- Clock[F].realTime.map(d => Instant.ofEpochMilli(d.toMillis))

      request = UpdateItemRequest.builder().tableName(tableName)
        .key(Map("pk" -> attr(s"idempotency#$idempotencyKey")).asJava)
        .updateExpression(
          "SET #status = :status, #updatedAt = :updatedAt, #version = #version + :one",
        ).conditionExpression(DynamoDBIdempotencyStore.endCondition(claimId))
        .expressionAttributeNames(
          (Map(
            "#status" -> "status",
            "#updatedAt" -> "updatedAt",
            "#version" -> "version",
            "#clientId" -> "clientId",
            "#ttl" -> "ttl",
          ) ++ claimId.map(_ => "#claimId" -> "claimId")).asJava,
        ).expressionAttributeValues(
          (Map(
            ":status" -> attr("Failed"),
            ":updatedAt" -> attrN(now.toEpochMilli),
            ":pending" -> attr("Pending"),
            ":one" -> attrN(1),
            ":clientId" -> attr(clientId),
            ":now" -> attrN(now.getEpochSecond),
          ) ++ claimId.map(id => ":claimId" -> attr(id))).asJava,
        ).build()

      result <- conditionalUpdate(client, request)
    yield result

  override def get(idempotencyKey: String): F[Option[IdempotencyRecord]] =
    val request = GetItemRequest.builder().tableName(tableName)
      .key(Map("pk" -> attr(s"idempotency#$idempotencyKey")).asJava)
      .consistentRead(true).build()

    Clock[F].realTime.map(d => Instant.ofEpochMilli(d.toMillis)).flatMap {
      now =>
        Async[F].fromCompletableFuture(
          Async[F].delay(client.getItem(request).toCompletableFuture),
        ).flatMap { response =>
          if response.hasItem && !response.item().isEmpty then
            parseRecord(idempotencyKey, response.item().asScala.toMap) match
              // DynamoDB deletes expired items lazily, often days late. Until
              // then an expired record answered in_progress or replayed, and a
              // crashed owner's key stayed stuck that long.
              case Right(record) if record.expired(now) => Async[F].pure(None)
              case Right(record) => Async[F].pure(Some(record))
              case Left(StoreError.CorruptRecord(key, detail)) => Async[F]
                  .raiseError(new CorruptIdempotencyRecordException(key, detail))
          else Async[F].pure(None)
        }
    }

  override def healthCheck: F[Either[String, Unit]] = Async[F]
    .fromCompletableFuture(Async[F].delay(
      client
        .describeTable(DescribeTableRequest.builder().tableName(tableName).build())
        .toCompletableFuture,
    )).map(_ => Right(())).handleError(e => Left(e.getMessage))

  private def tryCreatePending(
      idempotencyKey: String,
      clientId: String,
      now: Instant,
      ttlSeconds: Long,
      requestHash: Option[String],
      claimId: String,
  ): F[Boolean] =
    val ttl = now.getEpochSecond + ttlSeconds

    val baseItem = Map(
      "pk" -> attr(s"idempotency#$idempotencyKey"),
      "clientId" -> attr(clientId),
      "claimId" -> attr(claimId),
      "status" -> attr("Pending"),
      "createdAt" -> attrN(now.toEpochMilli),
      "updatedAt" -> attrN(now.toEpochMilli),
      "version" -> attrN(1),
      "ttl" -> attrN(ttl),
    )

    val item = requestHash match
      case Some(hash) => baseItem + ("requestHash" -> attr(hash))
      case None => baseItem

    val request = PutItemRequest.builder().tableName(tableName).item(item.asJava)
      // An expired record is claimable, as if TTL had already deleted it; get
      // reads it as absent by the same rule. A missing ttl never compares.
      .conditionExpression(
        "attribute_not_exists(pk) OR #status = :failed OR #ttl < :now",
      ).expressionAttributeNames(
        Map("#status" -> "status", "#ttl" -> "ttl").asJava,
      ).expressionAttributeValues(
        Map(":failed" -> attr("Failed"), ":now" -> attrN(now.getEpochSecond))
          .asJava,
      ).build()

    conditionalPut(client, request)

  private def parseRecord(
      idempotencyKey: String,
      item: Map[String, AttributeValue],
  ): Either[StoreError, IdempotencyRecord] =
    val statusResult: Either[StoreError, IdempotencyStatus] =
      item.get("status").map(_.s()) match
        case Some("Pending") => Right(IdempotencyStatus.Pending)
        case Some("Completed") => Right(IdempotencyStatus.Completed)
        case Some("Failed") => Right(IdempotencyStatus.Failed)
        case Some(unknown) => Left(
            StoreError
              .CorruptRecord(idempotencyKey, s"unknown status value: '$unknown'"),
          )
        case None => Left(
            StoreError
              .CorruptRecord(idempotencyKey, "missing 'status' attribute"),
          )

    val responseResult: Either[StoreError, Option[StoredResponse]] =
      item.get("response") match
        case None => Right(None)
        case Some(av) => decode[StoredResponse](av.s()) match
            case Right(r) => Right(Some(r))
            case Left(err) => Left(StoreError.CorruptRecord(
                idempotencyKey,
                s"response JSON decode failed: $err",
              ))

    val requestHash = item.get("requestHash").map(_.s())
    // Absent numbers keep their defaults, but a present one must be a number.
    // They were read with `.n().toLong`, so a wrongly typed attribute threw a
    // NullPointerException and answered 503 storage_unavailable, which says
    // retry, instead of storage_corruption, which says do not proceed.
    def number(name: String): Either[StoreError, Option[Long]] =
      if item.contains(name) then
        longAttr(item, name).map(Some(_)).left
          .map(StoreError.CorruptRecord(idempotencyKey, _))
      else Right(None)
    for
      status <- statusResult
      response <- responseResult
      createdMs <- number("createdAt")
      updatedMs <- number("updatedAt")
      ttl <- number("ttl")
      version <- number("version")
      createdAt = createdMs.map(Instant.ofEpochMilli).getOrElse(Instant.now())
    yield IdempotencyRecord(
      idempotencyKey = idempotencyKey,
      clientId = item.get("clientId").map(_.s()).getOrElse("unknown"),
      status = status,
      response = response,
      createdAt = createdAt,
      updatedAt = updatedMs.map(Instant.ofEpochMilli).getOrElse(createdAt),
      // No ttl never expires, matching the claim condition.
      ttl = ttl.getOrElse(Long.MaxValue),
      version = version.getOrElse(0L),
      requestHash = requestHash,
      claimId = item.get("claimId").map(_.s()),
    )

object DynamoDBIdempotencyStore:

  /** What `storeResponse` and `markFailed` require: a Pending record of this
    * client's that has not expired. Without the ttl term a late call on an
    * expired record answered true, though the claim had lapsed and no check
    * would ever replay what it stored. A record with no ttl never expires,
    * matching the claim condition and `get`.
    */
  private val LivePendingOwnedBy =
    "#status = :pending AND #clientId = :clientId AND (attribute_not_exists(#ttl) OR #ttl >= :now)"

  /** With a claim ID the record must also be that claim's (ADR-006): a key
    * reclaimed after expiry is live, Pending, and the same client's, so nothing
    * else tells the first run's late call from the second run's. A record with
    * no `claimId` attribute never matches one.
    */
  private def endCondition(claimId: Option[String]): String = claimId.fold(
    LivePendingOwnedBy,
  )(_ => s"$LivePendingOwnedBy AND #claimId = :claimId")

  def apply[F[_]: Async](
      client: DynamoDbAsyncClient,
      tableName: String,
  ): DynamoDBIdempotencyStore[F] =
    new DynamoDBIdempotencyStore[F](client, tableName)
