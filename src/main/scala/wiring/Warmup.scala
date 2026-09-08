package wiring

import java.util.UUID

import scala.concurrent.duration.*

import org.typelevel.log4cats.Logger

import cats.effect.*
import cats.syntax.all.*
import core.{RateLimitProfile, RateLimitStore}

/** Exercises the rate-limit hot path before the server binds.
  *
  * A cold instance answering its first burst pays JIT and SDK initialisation on
  * every in-flight request; they all exceed the check timeout together, which
  * is enough to open the circuit breaker and reject traffic for a whole reset
  * window. I run a few calls against the raw store (no breaker, no request
  * timeout) so that readiness means the path has actually been used.
  */
object Warmup:
  def rateLimitPath[F[_]: Async: Logger](
      store: RateLimitStore[F],
      profile: RateLimitProfile,
      rounds: Int = 5,
      perCallTimeout: FiniteDuration = 15.seconds,
  ): F[Unit] =
    val key = s"gate:warmup:${UUID.randomUUID()}"
    for
      start <- Clock[F].realTime
      failed <- (1 to rounds).toList.foldLeftM(0)((failed, i) =>
        Async[F].timeout(store.checkAndConsume(key, 1, profile), perCallTimeout)
          .attempt.flatMap {
            case Right(_) => Async[F].pure(failed)
            case Left(e) => Logger[F]
                .warn(s"Warm-up call $i/$rounds failed: ${e.getMessage}")
                .as(failed + 1)
          },
      )
      end <- Clock[F].realTime
      _ <- Logger[F]
        .info(s"Rate-limit hot path warmed: $rounds calls in ${(end - start)
            .toMillis} ms ($failed failed)")
    yield ()
