package aggregator

import cats.effect.{Ref, Sync}
import cats.syntax.all.*

/** Where usage and the read position live, together.
  *
  * The one rule that makes the totals exact: a batch's sums and the checkpoint
  * that says "read up to here" are written in one atomic step, conditioned on
  * the checkpoint still being where this reader found it. A batch read twice,
  * by a restarted reader or a second one, fails that condition instead of
  * adding twice.
  */
trait UsageStore[F[_]]:

  /** The last sequence number committed for a shard, read consistently. None
    * when the shard has never been committed.
    */
  def checkpoint(shardId: String): F[Option[String]]

  /** Adds `deltas` and moves the shard's checkpoint from `expected` to `next`,
    * or does neither.
    */
  def commit(
      shardId: String,
      expected: Option[String],
      next: String,
      deltas: Map[BucketKey, Counts],
  ): F[CommitResult]

  /** A bucket's totals, for tests and for reading usage back. */
  def usage(key: BucketKey): F[Counts]

  def healthCheck: F[Either[String, Unit]]

enum CommitResult:
  case Committed

  /** The checkpoint had moved: another reader, or an earlier attempt of this
    * one that committed after it was given up on. Nothing was written.
    */
  case Fenced

object UsageStore:

  /** Most buckets one commit may write: DynamoDB's 100 items per transaction,
    * less the checkpoint.
    */
  val MaxBucketsPerCommit: Int = 99

  /** The same rule over a `Ref`, for tests and local runs. */
  def inMemory[F[_]: Sync]: F[UsageStore[F]] = Ref
    .of[F, (Map[String, String], Map[BucketKey, Counts])]((Map.empty, Map.empty))
    .map { state =>
      new UsageStore[F]:
        def checkpoint(shardId: String): F[Option[String]] = state.get
          .map(_._1.get(shardId))

        def commit(
            shardId: String,
            expected: Option[String],
            next: String,
            deltas: Map[BucketKey, Counts],
        ): F[CommitResult] = state.modify { case s @ (checkpoints, buckets) =>
          if checkpoints.get(shardId) != expected then (s, CommitResult.Fenced)
          else
            val added = deltas.foldLeft(buckets) { case (acc, (k, c)) =>
              acc.updated(k, acc.getOrElse(k, Counts.zero) + c)
            }
            ((checkpoints.updated(shardId, next), added), CommitResult.Committed)
        }

        def usage(key: BucketKey): F[Counts] = state.get
          .map(_._2.getOrElse(key, Counts.zero))

        def healthCheck: F[Either[String, Unit]] = Sync[F].pure(Right(()))
    }
