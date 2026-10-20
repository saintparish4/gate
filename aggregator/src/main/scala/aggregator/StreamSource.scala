package aggregator

import scala.jdk.CollectionConverters.*

import cats.effect.Async
import cats.syntax.all.*
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.*

/** One record as read from a shard. */
final case class StreamRecord(sequenceNumber: String, data: Array[Byte])

/** One read: its records, where to read next (None once a closed shard has been
  * read to its end), and how far behind the tip of the shard it was.
  */
final case class ReadResult(
    records: List[StreamRecord],
    next: Option[String],
    millisBehindLatest: Long,
)

/** The stream as the aggregator reads it. Kinesis in production; a fake in the
  * tests.
  */
trait StreamSource[F[_]]:
  /** Every shard, open or closed. A closed shard is read to its end, so a
    * reshard loses nothing; sums do not care which shard is read first.
    */
  def shards: F[List[String]]

  /** An iterator just after `after`, or at the oldest record kept. */
  def iterator(shardId: String, after: Option[String]): F[String]

  def read(iterator: String, limit: Int): F[ReadResult]

object StreamSource:

  def kinesis[F[_]: Async](
      client: KinesisAsyncClient,
      stream: String,
  ): StreamSource[F] = new StreamSource[F]:

    private def call[A](f: => java.util.concurrent.CompletableFuture[A]): F[A] =
      Async[F].fromCompletableFuture(Async[F].delay(f))

    def shards: F[List[String]] =
      def page(token: Option[String], acc: List[String]): F[List[String]] =
        val request = token.fold(
          ListShardsRequest.builder().streamName(stream),
        )(t => ListShardsRequest.builder().nextToken(t)).build()
        call(client.listShards(request)).flatMap { response =>
          val ids = acc ++ response.shards.asScala.map(_.shardId)
          Option(response.nextToken)
            .fold(Async[F].pure(ids))(t => page(Some(t), ids))
        }
      page(None, Nil)

    def iterator(shardId: String, after: Option[String]): F[String] =
      val base = GetShardIteratorRequest.builder().streamName(stream)
        .shardId(shardId)
      val request = after
        .fold(base.shardIteratorType(ShardIteratorType.TRIM_HORIZON))(seq =>
          base.shardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
            .startingSequenceNumber(seq),
        ).build()
      call(client.getShardIterator(request)).map(_.shardIterator)

    def read(iterator: String, limit: Int): F[ReadResult] =
      val request = GetRecordsRequest.builder().shardIterator(iterator)
        .limit(limit).build()
      call(client.getRecords(request)).map(response =>
        ReadResult(
          response.records.asScala.toList
            .map(r => StreamRecord(r.sequenceNumber, r.data.asByteArray)),
          Option(response.nextShardIterator),
          Option(response.millisBehindLatest).map(_.longValue).getOrElse(0L),
        ),
      )
