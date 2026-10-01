package resilience

import scala.concurrent.duration.*

import org.typelevel.log4cats.Logger

import cats.effect.*
import cats.syntax.all.*

/** Circuit Breaker implementation for resilience against cascading failures.
  *
  * States:
  *   - Closed: Normal operation, requests pass through
  *   - Open: Requests fail fast without calling the protected resource
  *   - HalfOpen: Limited requests allowed to test if the resource recovered
  *
  * Transitions:
  *   - Closed -> Open: When failure count exceeds threshold within window
  *   - Open -> HalfOpen: After reset timeout expires
  *   - HalfOpen -> Closed: When a probe request succeeds
  *   - HalfOpen -> Open: When a probe request fails
  *
  * While half-open, at most `halfOpenMaxCalls` probes are in flight; every
  * other call is refused as if the breaker were open.
  */
trait CircuitBreaker[F[_]]:
  /** Protect a call with the circuit breaker.
    *
    * @param fa
    *   The effect to protect
    * @return
    *   The result, or fails with CircuitBreakerOpen if the breaker is open
    */
  def protect[A](fa: F[A]): F[A]

  /** Get current circuit state */
  def state: F[CircuitState]

  /** Get circuit breaker metrics */
  def metrics: F[CircuitBreakerMetrics]

sealed trait CircuitState
object CircuitState:
  case object Closed extends CircuitState
  case object Open extends CircuitState
  case object HalfOpen extends CircuitState

case class CircuitBreakerMetrics(
    state: CircuitState,
    failureCount: Int,
    successCount: Int,
    rejectedCount: Long,
    lastFailureTime: Option[Long],
    lastSuccessTime: Option[Long],
)

case class CircuitBreakerConfig(
    maxFailures: Int = 5,
    resetTimeout: FiniteDuration = 30.seconds,
    halfOpenMaxCalls: Int = 3,
    failureRateThreshold: Double = 0.5,
    slidingWindowSize: Int = 10,
)

object CircuitBreakerConfig:
  val default: CircuitBreakerConfig = CircuitBreakerConfig()

  val aggressive: CircuitBreakerConfig = CircuitBreakerConfig(
    maxFailures = 3,
    resetTimeout = 15.seconds,
    halfOpenMaxCalls = 1,
  )

  val relaxed: CircuitBreakerConfig = CircuitBreakerConfig(
    maxFailures = 10,
    resetTimeout = 60.seconds,
    halfOpenMaxCalls = 5,
  )

/** Kept for binary/source compatibility. Use GateError.CircuitOpen directly.
  */
type CircuitBreakerOpen = core.GateError.CircuitOpen

object CircuitBreaker:

  private case class InternalState(
      circuitState: CircuitState,
      failures: Int,
      successes: Int,
      rejectedCount: Long,
      lastFailureTime: Option[Long],
      lastSuccessTime: Option[Long],
      openedAt: Option[Long],
      // Probes in flight while half-open, at most halfOpenMaxCalls.
      halfOpenCalls: Int,
  )

  /** What `protect` may do with a call, decided in one state update. */
  private enum Admission:
    case Run
    case Probe(openedHalfOpen: Boolean)
    case Refuse

  private object InternalState:
    def initial: InternalState = InternalState(
      circuitState = CircuitState.Closed,
      failures = 0,
      successes = 0,
      rejectedCount = 0,
      lastFailureTime = None,
      lastSuccessTime = None,
      openedAt = None,
      halfOpenCalls = 0,
    )

  /** @param countsAsFailure
    *   Which errors count toward opening the breaker. Defaults to "all of
    *   them". A breaker shared across callers should pass a classifier that
    *   excludes caller-scoped failures -- see
    *   ResilientRateLimitStore.dependencyFailure.
    */
  def apply[F[_]: Temporal: Logger](
      name: String,
      config: CircuitBreakerConfig = CircuitBreakerConfig.default,
      countsAsFailure: Throwable => Boolean = _ => true,
  ): F[CircuitBreaker[F]] =
    for stateRef <- Ref.of[F, InternalState](InternalState.initial)
    yield new CircuitBreaker[F]:
      private val logger = Logger[F]

      // A limit below one would leave a half-open breaker with no probe to
      // close it.
      private val maxProbes = math.max(1, config.halfOpenMaxCalls)

      // Only `fa` itself can be cancelled, so a claimed probe slot is always
      // either used or handed back.
      override def protect[A](fa: F[A]): F[A] = Temporal[F]
        .uncancelable { poll =>
          for
            now <- Clock[F].realTime.map(_.toMillis)
            admission <- admit(now)
            result <- admission match
              case Admission.Run => executeAndRecord(poll(fa), now)
              case Admission.Probe(openedHalfOpen) =>
                (if openedHalfOpen then
                   logger
                     .info(s"Circuit breaker '$name' transitioning to half-open")
                 else Temporal[F].unit) *> executeHalfOpen(poll(fa), now)
              case Admission.Refuse => Temporal[F]
                  .raiseError(core.GateError.CircuitOpen(name))
          yield result
        }

      // The transition to half-open and the claim of a probe slot are one
      // update. They were a read followed by a write, behind a single-permit
      // semaphore: every caller that saw the timeout pass reset the probe
      // count to zero, and the semaphore let one probe run at a time whatever
      // `halfOpenMaxCalls` said, so the setting did nothing.
      private def admit(now: Long): F[Admission] = stateRef.modify { s =>
        def refuse =
          (s.copy(rejectedCount = s.rejectedCount + 1), Admission.Refuse)
        s.circuitState match
          case CircuitState.Closed => (s, Admission.Run)
          case CircuitState.Open
              if s.openedAt.exists(now - _ >= config.resetTimeout.toMillis) =>
            (
              s.copy(circuitState = CircuitState.HalfOpen, halfOpenCalls = 1),
              Admission.Probe(openedHalfOpen = true),
            )
          case CircuitState.Open => refuse
          case CircuitState.HalfOpen if s.halfOpenCalls < maxProbes =>
            (
              s.copy(halfOpenCalls = s.halfOpenCalls + 1),
              Admission.Probe(openedHalfOpen = false),
            )
          case CircuitState.HalfOpen => refuse
      }

      override def state: F[CircuitState] = stateRef.get.map(_.circuitState)

      override def metrics: F[CircuitBreakerMetrics] = stateRef.get.map(s =>
        CircuitBreakerMetrics(
          state = s.circuitState,
          failureCount = s.failures,
          successCount = s.successes,
          rejectedCount = s.rejectedCount,
          lastFailureTime = s.lastFailureTime,
          lastSuccessTime = s.lastSuccessTime,
        ),
      )

      private def executeAndRecord[A](fa: F[A], now: Long): F[A] = fa.attempt
        .flatMap {
          case Right(result) => recordSuccess(now).as(result)
          // Not evidence either way about the dependency: leave the counters
          // alone and hand the caller its original error.
          case Left(error) if !countsAsFailure(error) =>
            Temporal[F].raiseError(error)
          case Left(error) => recordFailure(now) *> checkAndMaybeOpen(now) *>
              Temporal[F].raiseError(error)
        }

      // Several probes can be in flight, so each outcome applies only while
      // the breaker is still half-open: a probe that returns after another one
      // closed or reopened it must not undo that.
      private def executeHalfOpen[A](fa: F[A], now: Long): F[A] = Temporal[F]
        // A cancelled probe says nothing either. Its slot was never returned,
        // and enough of them left no probe to close the breaker.
        .onCancel(fa.attempt, releaseProbe).flatMap {
          case Right(result) => stateRef.modify(s =>
              if s.circuitState == CircuitState.HalfOpen then
                (
                  s.copy(
                    circuitState = CircuitState.Closed,
                    failures = 0,
                    successes = 0,
                    openedAt = None,
                    halfOpenCalls = 0,
                    lastSuccessTime = Some(now),
                  ),
                  true,
                )
              else (s, false),
            ).flatMap(closed =>
              if closed then
                logger
                  .info(s"Circuit breaker '$name' closed after successful probe")
              else Temporal[F].unit,
            ).as(result)
          // The probe told us nothing about the dependency. Return the slot, or
          // a run of these strands the breaker in half-open with no probes left
          // and it never recovers.
          case Left(error) if !countsAsFailure(error) =>
            releaseProbe *> Temporal[F].raiseError(error)
          case Left(error) => stateRef.modify(s =>
              if s.circuitState == CircuitState.HalfOpen then
                (
                  s.copy(
                    circuitState = CircuitState.Open,
                    openedAt = Some(now),
                    halfOpenCalls = 0,
                    lastFailureTime = Some(now),
                  ),
                  true,
                )
              else (s, false),
            ).flatMap(reopened =>
              if reopened then
                logger
                  .warn(s"Circuit breaker '$name' reopened after failed probe")
              else Temporal[F].unit,
            ) *> Temporal[F].raiseError(error)
        }

      private def releaseProbe: F[Unit] = stateRef.update(s =>
        if s.circuitState == CircuitState.HalfOpen then
          s.copy(halfOpenCalls = math.max(0, s.halfOpenCalls - 1))
        else s,
      )

      private def recordSuccess(now: Long): F[Unit] = stateRef.update(s =>
        s.copy(
          successes = s.successes + 1,
          lastSuccessTime = Some(now),
          // Reset failures on success in closed state
          failures =
            if s.circuitState == CircuitState.Closed then 0 else s.failures,
        ),
      )

      private def recordFailure(now: Long): F[Unit] = stateRef.update(s =>
        s.copy(failures = s.failures + 1, lastFailureTime = Some(now)),
      )

      // Transition atomically and log exactly once. Reading state and then
      // writing it let all 50 concurrent callers each see failures >= threshold
      // and log their own "opened after N failures", which is how one tripped
      // breaker produced 60k log lines in a 30s run.
      private def checkAndMaybeOpen(now: Long): F[Unit] = stateRef.modify(s =>
        if s.circuitState == CircuitState.Closed &&
          s.failures >= config.maxFailures
        then
          (
            s.copy(
              circuitState = CircuitState.Open,
              openedAt = Some(now),
              halfOpenCalls = 0,
            ),
            Some(s.failures),
          )
        else (s, None),
      ).flatMap {
        case Some(failures) => logger
            .warn(s"Circuit breaker '$name' opened after $failures failures")
        case None => Temporal[F].unit
      }
