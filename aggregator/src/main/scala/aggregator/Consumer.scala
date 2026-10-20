package aggregator

import scala.concurrent.duration.*

import org.typelevel.log4cats.Logger

import cats.effect.*
import cats.effect.syntax.all.*
import cats.syntax.all.*

/** Reads every shard of the stream and commits its usage, each record once.
  *
  * The loop for one shard: read the checkpoint, open an iterator just after it,
  * read a batch, and commit it in chunks, each chunk's sums with the checkpoint
  * of its last record. Any doubt (a fenced commit, an error, an expired
  * iterator) sends it back to the checkpoint. Whether a timed-out commit landed
  * does not matter: the checkpoint says, because the sums and the checkpoint
  * land together or not at all.
  *
  * Exactly once holds for what reaches the stream. The service publishes at
  * most once (ADR-003), so these totals are a lower bound on what the service
  * decided, never above it. ADR-007 says what that makes them good for.
  */
final class Consumer[F[_]: Async: Logger](
    source: StreamSource[F],
    store: UsageStore[F],
    metrics: AggregatorMetrics,
    settings: Consumer.Settings,
):
  private val log = Logger[F]

  /** Runs forever: finds shards every `shardRefresh`, and reads each new one on
    * its own fiber until it is closed and read to its end.
    */
  def run: F[Nothing] = Ref.of[F, Set[String]](Set.empty).flatMap { started =>
    val discover = source.shards.flatMap(ids =>
      ids.traverse_(id =>
        started.modify(s => (s + id, !s(id))).flatMap(isNew =>
          if isNew then
            // A shard stays in `started` once it ends: a closed shard read to
            // its end has nothing more to give.
            log.info(s"Reading shard $id") *> shard(id).start.void
          else Async[F].unit,
        ),
      ),
    ).handleErrorWith(e => log.warn(e)("Could not list shards; will retry"))
    (discover *> Async[F].sleep(settings.shardRefresh)).foreverM
  }

  /** Reads one shard until it is closed and finished. */
  def shard(shardId: String): F[Unit] =
    val attempt =
      for
        checkpoint <- store.checkpoint(shardId)
        iterator <- source.iterator(shardId, checkpoint)
        done <- readFrom(shardId, checkpoint, iterator)
      yield done
    attempt.handleErrorWith(e =>
      Async[F].delay(metrics.commits.labels("error").inc()) *>
        log.warn(e)(s"Shard $shardId: ${e.getClass
            .getSimpleName}; resuming from its checkpoint") *>
        Async[F].sleep(settings.errorBackoff).as(false),
    ).flatMap(finished =>
      if finished then log.info(s"Shard $shardId is closed and read to its end")
      else shard(shardId),
    )

  /** Reads and commits until the shard ends (true) or the position is in doubt
    * (false, and the caller starts again from the checkpoint).
    */
  private def readFrom(
      shardId: String,
      checkpoint: Option[String],
      iterator: String,
  ): F[Boolean] = source.read(iterator, settings.batchLimit).flatMap { batch =>
    Async[F].delay(
      metrics.millisBehind.labels(shardId).set(batch.millisBehindLatest.toDouble),
    ) *> commitAll(shardId, checkpoint, batch.records).flatMap {
      case None => Async[F].pure(false)
      case Some(position) => batch.next match
          case None => Async[F].pure(true)
          case Some(next) =>
            val pause =
              if batch.records.isEmpty then Async[F].sleep(settings.idlePoll)
              else Async[F].unit
            pause *> readFrom(shardId, position, next)
    }
  }

  /** Commits a batch chunk by chunk. Some(new checkpoint), or None when a
    * commit was fenced.
    */
  private def commitAll(
      shardId: String,
      checkpoint: Option[String],
      records: List[StreamRecord],
  ): F[Option[Option[String]]] =
    val parsed = records.map(r => (r, Usage.parse(r.data)))
    Consumer.chunks(parsed, UsageStore.MaxBucketsPerCommit)
      .foldLeftM[F, Option[Option[String]]](Some(checkpoint)) {
        case (None, _) => Async[F].pure(None)
        case (Some(expected), chunk) =>
          val next = chunk.last._1.sequenceNumber
          val deltas = Usage.fold(chunk.map(_._2))
          store.commit(shardId, expected, next, deltas).flatMap {
            case CommitResult.Committed => Async[F].delay {
                metrics.commits.labels("committed").inc()
                metrics.recordCommitted(chunk.map(_._2), deltas)
              }.as(Some(Some(next)))
            case CommitResult.Fenced => Async[F]
                .delay(metrics.commits.labels("fenced").inc()) *> log.info(
                s"Shard $shardId: checkpoint moved under me; resuming from it",
              ).as(None)
          }
      }

object Consumer:

  final case class Settings(
      batchLimit: Int = 1000,
      // GetRecords allows 5 calls a second per shard, shared by every reader.
      idlePoll: FiniteDuration = 1.second,
      errorBackoff: FiniteDuration = 2.seconds,
      shardRefresh: FiniteDuration = 60.seconds,
  )

  /** Splits a batch, in order, into runs touching at most `maxBuckets` distinct
    * buckets, so each fits one transaction.
    */
  def chunks[A](
      parsed: List[(A, Parsed)],
      maxBuckets: Int,
  ): List[List[(A, Parsed)]] = parsed
    .foldLeft(List.empty[(List[(A, Parsed)], Set[BucketKey])]) {
      case (acc, item) =>
        val key = item._2 match
          case Parsed.Usage(_, k, _) => Some(k)
          case _ => None
        acc match
          case (chunk, keys) :: rest
              if key.forall(k => keys(k) || keys.size < maxBuckets) =>
            (item :: chunk, keys ++ key) :: rest
          case _ => (List(item), key.toSet) :: acc
    }.reverse.map(_._1.reverse)
