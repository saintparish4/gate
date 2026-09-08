import cats.effect.*
import cats.effect.std.Console
import cats.syntax.all.*
import org.http4s.*
import org.http4s.circe.*
import org.http4s.client.*
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.implicits.*
import io.circe.generic.auto.*
import io.circe.syntax.*
import io.circe.Json
import scala.concurrent.duration.*
import java.util.concurrent.atomic.{AtomicLong, AtomicInteger}
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}
import org.HdrHistogram.ConcurrentHistogram

/**
 * Load simulation runner for Gate.
 *
 * Scenarios:
 *   normal         — many unique keys, steady rate, verifies base throughput
 *   burst          — many unique keys with bursts, exercises token refill
 *   idempotency    — repeated idempotency keys, verifies exactly-one-Created
 *   realistic      — mix of rate-limit (80%) and idempotency (20%) traffic
 *   highContention — all requests use a single fixed key at maximum concurrency;
 *                    stresses OCC retry logic and measures retry rate vs throughput
 *   correctness    — CI-gated invariants:
 *                      A) token-bucket non-over-issue under 50-way parallel load
 *                      B) idempotency exactly-one-Created under K=10 shared keys
 *                    exits non-zero on any invariant violation
 *   latency        — fixed-RPS latency measurement with HdrHistogram; emits a
 *                    markdown table row of rps, p50, p95, p99, error_rate
 *
 * Usage:
 *   sbt "loadSim/run --scenario correctness"
 *   sbt "loadSim/run --scenario latency --rps 1000 --duration 60"
 *   sbt "loadSim/run --scenario highContention"
 *   sbt "loadSim/run --scenario normal"
 *
 * Optional flags:
 *   --url http://localhost:8080   base URL (default)
 *   --rps N                       target RPS for latency scenario (default 1000)
 *   --duration N                  duration in seconds for latency scenario (default 60)
 */
object LoadSim extends IOApp:

  val defaultBaseUrl = "http://localhost:8080"
  val defaultApiKey  = "test-api-key"

  override def run(args: List[String]): IO[ExitCode] =
    val scenario = Args.string(args, "--scenario", "normal")
    val baseUrl  = Args.string(args, "--url", defaultBaseUrl)
    val rps      = Args.intOpt(args, "--rps")
    val duration = Args.intOpt(args, "--duration")

    // Larger connection pool so workers don't queue waiting for a connection
    // at high RPS, and an explicit per-request timeout so a hung server fails
    // fast instead of tying up workers.
    val clientR = EmberClientBuilder
      .default[IO]
      .withMaxTotal(256)
      .withIdleConnectionTime(30.seconds)
      .withTimeout(30.seconds)
      .build

    clientR.use { client =>
      def withPreflight(action: IO[ExitCode]): IO[ExitCode] =
        Http.healthCheck(client, baseUrl).flatMap {
          case Right(_) =>
            Console[IO].println(s"Preflight OK: $baseUrl/health responded 200.") *> action
          case Left(msg) =>
            Console[IO].errorln(
              s"""Preflight failed: GET $baseUrl/health -> $msg
                 |
                 |The load simulator cannot run against a service that is not healthy.
                 |Start the stack first:
                 |  docker compose up -d
                 |  curl -sf $baseUrl/health
                 |Then re-run this scenario.""".stripMargin
            ) *> IO.pure(ExitCode.Error)
        }

      scenario match
        case "normal"         => withPreflight(Scenarios.normal(client, baseUrl).as(ExitCode.Success))
        case "burst"          => withPreflight(Scenarios.burst(client, baseUrl).as(ExitCode.Success))
        case "idempotency"    => withPreflight(Scenarios.idempotency(client, baseUrl).as(ExitCode.Success))
        case "realistic"      => withPreflight(Scenarios.realistic(client, baseUrl).as(ExitCode.Success))
        case "highContention" => withPreflight(Scenarios.highContention(client, baseUrl).as(ExitCode.Success))
        case "correctness"    => withPreflight(Scenarios.correctness(client, baseUrl))
        case "latency"        => withPreflight(Scenarios.latency(client, baseUrl, rps.getOrElse(1000), duration.getOrElse(60)))
        case unknown =>
          Console[IO].errorln(
            s"Unknown scenario: $unknown. Valid: normal, burst, idempotency, realistic, highContention, correctness, latency"
          ) *> IO.pure(ExitCode.Error)
    }

object Args:
  def string(args: List[String], flag: String, default: String): String =
    args.sliding(2).collectFirst { case `flag` :: v :: Nil => v }.getOrElse(default)
  def intOpt(args: List[String], flag: String): Option[Int] =
    args.sliding(2).collectFirst { case `flag` :: v :: Nil => v.toIntOption }.flatten

/**
 * Thread-safe collector for error messages. Keeps up to `maxDistinct` unique
 * normalised messages and counts the total number of errors recorded. Used
 * so that a run with a high error rate tells you WHY, not just HOW MANY.
 */
final class ErrorSampler(maxDistinct: Int = 5):
  private val seen  = new ConcurrentHashMap[String, java.lang.Boolean]()
  private val order = new ConcurrentLinkedQueue[String]()
  private val count = new AtomicLong(0)

  def record(msg: String): Unit =
    count.incrementAndGet()
    val key = normalize(msg)
    // putIfAbsent returns null when the key is new; only then append to order.
    if seen.size < maxDistinct && seen.putIfAbsent(key, java.lang.Boolean.TRUE) == null then
      order.offer(key)
    ()

  def total: Long = count.get

  def render: String =
    if order.isEmpty then "(no errors recorded)"
    else
      val it = order.iterator()
      val sb = new StringBuilder
      var i  = 0
      while it.hasNext && i < maxDistinct do
        sb.append(s"    [${i + 1}] ${it.next()}")
        if it.hasNext && i + 1 < maxDistinct then sb.append("\n")
        i += 1
      sb.toString

  private def normalize(msg: String): String =
    val trimmed = if msg == null then "(null error)" else msg
    trimmed.take(200).replaceAll("\\s+", " ").trim

object Scenarios:
  import Http.*

  def normal(client: Client[IO], baseUrl: String): IO[Unit] =
    val cfg = RunConfig(
      concurrency    = 20,
      durationSecs   = 60,
      description    = "normal — 20 VUs, many unique keys, 60 s",
      keyFn          = i => s"user:${i % 500}",
    )
    runRateLimitScenario(client, baseUrl, cfg)

  def burst(client: Client[IO], baseUrl: String): IO[Unit] =
    val cfg = RunConfig(
      concurrency    = 50,
      durationSecs   = 60,
      description    = "burst — 50 VUs, bursty per-user keys, 60 s",
      keyFn          = i => s"user:${i % 50}",
    )
    runRateLimitScenario(client, baseUrl, cfg)

  def idempotency(client: Client[IO], baseUrl: String): IO[Unit] =
    val total     = new AtomicLong(0)
    val created   = new AtomicLong(0)
    val duplicate = new AtomicLong(0)
    val errors    = new AtomicLong(0)

    val concurrency = 30
    val durationSecs = 30

    val runId = System.currentTimeMillis.toString

    Console[IO].println(s"=== idempotency — $concurrency VUs, repeated keys, ${durationSecs}s (run $runId) ===") *>
    IO.race(
      progressTicker("idempotency", durationSecs, total, created, duplicate, errors),
      (0 until concurrency).toList.parTraverse_ { vuid =>
        val key = s"idem:$runId:key-${vuid % 5}" // 5 shared keys per run, unique across runs
        (for
          res <- sendIdempotencyCheck(client, baseUrl, key)
          _   <- IO(total.incrementAndGet())
          _ <- res match
            case Right("new")         => IO(created.incrementAndGet())
            case Right("in_progress") => IO(duplicate.incrementAndGet())
            case Right("duplicate")   => IO(duplicate.incrementAndGet())
            case _                    => IO(errors.incrementAndGet())
        yield ()).loop
      }
    ) *>
    IO.defer {
      Console[IO].println(
        s"""idempotency results:
           |  total      = ${total.get}
           |  created    = ${created.get}
           |  duplicate  = ${duplicate.get}
           |  errors     = ${errors.get}
           |  assertion  : created should equal number of distinct keys (5 keys = 5 Created expected)
           |""".stripMargin
      )
    }

  def realistic(client: Client[IO], baseUrl: String): IO[Unit] =
    val concurrency  = 40
    val durationSecs = 60
    val description  = "realistic — 40 VUs, 80% rate-limit / 20% idempotency, 60s"
    val keyFn        = (i: Int) => s"user:${i % 200}"

    val total      = new AtomicLong(0)
    val rlAllowed  = new AtomicLong(0)
    val rlBlocked  = new AtomicLong(0)
    val idemCount  = new AtomicLong(0)
    val errors     = new AtomicLong(0)

    Console[IO].println(s"=== $description ===") *>
    IO.race(
      progressTicker("realistic", durationSecs, total, rlAllowed, rlBlocked, errors),
      (0 until concurrency).toList.parTraverse_ { vuid =>
        (for
          roll <- IO(scala.util.Random.nextInt(100))
          _ <-
            if roll < 80 then
              sendRateLimitCheck(client, baseUrl, keyFn(vuid)).flatMap {
                case Right(true)  => IO { total.incrementAndGet(); rlAllowed.incrementAndGet() }
                case Right(false) => IO { total.incrementAndGet(); rlBlocked.incrementAndGet() }
                case Left(_)      => IO { total.incrementAndGet(); errors.incrementAndGet() }
              }
            else
              sendIdempotencyCheck(client, baseUrl, s"idem:${vuid}:${System.currentTimeMillis / 10000}").flatMap {
                case Right(_) => IO { total.incrementAndGet(); idemCount.incrementAndGet() }
                case Left(_)  => IO { total.incrementAndGet(); errors.incrementAndGet() }
              }
        yield ()).loop
      }
    ) *>
    IO.defer {
      val t = total.get
      val rl = rlAllowed.get + rlBlocked.get
      val id = idemCount.get
      Console[IO].println(
        s"""$description results:
           |  total        = $t  (~${t / durationSecs} RPS)
           |  rate-limit   = $rl  (allowed=${rlAllowed.get}, blocked=${rlBlocked.get})
           |  idempotency  = $id
           |  errors       = ${errors.get}
           |  split        = ${if t > 0 then rl * 100 / t else 0}% rate-limit / ${if t > 0 then id * 100 / t else 0}% idempotency
           |""".stripMargin
      )
    }

  /**
   * highContention: all VUs hammer the same single key with maximum concurrency.
   *
   * Purpose: stress the OCC retry path. With N concurrent writers on one DynamoDB
   * item, all but one will get ConditionalCheckFailedException per round-trip and
   * must retry. This measures:
   *   - throughput degradation as concurrency rises
   *   - OCC retry rate (visible in RateLimitOCCRetry CloudWatch metric)
   *   - tail latency under contention
   *
   * Expected behaviour:
   *   - Allowed count stays close to the configured capacity (tokens per second * window);
   *     most requests are rejected (429) after the bucket empties.
   *   - A fraction of 429s are due to OCC exhaustion (not just empty bucket) — these
   *     are observable via the RateLimitOCCRetry metric counter.
   *   - Latency P99 rises compared to the normal scenario due to retries.
   */
  def highContention(client: Client[IO], baseUrl: String): IO[Unit] =
    val concurrency  = 50
    val durationSecs = 60
    val fixedKey     = "contention:single-hot-key"

    val total       = new AtomicLong(0)
    val allowed     = new AtomicLong(0)
    val blocked     = new AtomicLong(0)
    val errors      = new AtomicLong(0)
    val latencySum  = new AtomicLong(0)
    val latencyMax  = new AtomicLong(0)

    Console[IO].println(
      s"""=== highContention — $concurrency VUs, single key "$fixedKey", ${durationSecs}s ===
         |All requests target the same DynamoDB item. OCC retry rate will be high.
         |Watch: RateLimitOCCRetry metric, tail latency, and allowed vs blocked counts.
         |""".stripMargin
    ) *>
    IO.race(
      progressTicker("highContention", durationSecs, total, allowed, blocked, errors),
      (0 until concurrency).toList.parTraverse_ { _ =>
        (for
          t0  <- IO(System.currentTimeMillis)
          res <- sendRateLimitCheck(client, baseUrl, fixedKey)
          t1  <- IO(System.currentTimeMillis)
          ms   = t1 - t0
          _   <- IO {
            total.incrementAndGet()
            latencySum.addAndGet(ms)
            var prev = latencyMax.get
            while ms > prev && !latencyMax.compareAndSet(prev, ms) do prev = latencyMax.get
            res match
              case Right(true)  => allowed.incrementAndGet()
              case Right(false) => blocked.incrementAndGet()
              case Left(_)      => errors.incrementAndGet()
          }
        yield ()).loop
      }
    ) *>
    IO {
      val t   = total.get
      val avg = if t > 0 then latencySum.get / t else 0
      (t, avg)
    }.flatMap { (t, avgMs) =>
      Console[IO].println(
        s"""highContention results (${durationSecs}s window):
           |  total requests = $t  (${t / durationSecs} RPS)
           |  allowed        = ${allowed.get}  (${if t > 0 then allowed.get * 100 / t else 0}%)
           |  blocked (429)  = ${blocked.get}  (${if t > 0 then blocked.get * 100 / t else 0}%)
           |  errors         = ${errors.get}
           |  avg latency    = ${avgMs}ms
           |  max latency    = ${latencyMax.get}ms
           |
           |OCC retry rate is visible in CloudWatch metric RateLimitOCCRetry.
           |High blocked% under a single hot key is expected — the bucket empties quickly
           |and OCC exhaustion (after 10 retries) contributes additional rejections.
           |Compare with 'normal' scenario to see latency cost of contention.
           |""".stripMargin
      )
    }

  // ------------------------------------------------------------------
  // Correctness scenario: two invariants, CI-gated
  // ------------------------------------------------------------------

  /**
   * correctness: runs three invariants and exits non-zero on any violation.
   *
   * Invariant A — token-bucket non-over-issue:
   *   Server must never issue more tokens than capacity + refillRate * duration.
   *   Default profile: capacity=100 tokens, refillRate=10 tokens/s.
   *   N=50 parallel clients target a single unique key for T=30s.
   *   Assert: allowed_count <= 100 + ceil(10 * 30) + epsilon (epsilon=2 for clock skew).
   *
   * Invariant B — idempotency exactly-one-Created:
   *   For K=10 shared keys, N=50 parallel clients must never observe more than
   *   one "new" response per key. No HTTP errors, no 409 conflicts (same body).
   *   Assert: created_count == K and error_count == 0 and conflict_count == 0.
   *
   * Invariant C — token-quota non-over-admission:
   *   N=50 parallel clients each ask for 25,000 tokens against one user whose
   *   limit is 1,000,000 (the default), so at most 40 checks may be admitted.
   *   Requires TOKEN_QUOTA_ENABLED=true on the server.
   *   Assert: admitted * 25,000 <= 1,000,000, at least one 429, no HTTP errors.
   */
  def correctness(client: Client[IO], baseUrl: String): IO[ExitCode] =
    val runId = System.currentTimeMillis.toString
    for
      _  <- Console[IO].println(s"=== correctness — run $runId ===")
      a  <- invariantA_tokenBucketNonOverIssue(client, baseUrl, runId)
      b  <- invariantB_idempotencyExactlyOneCreated(client, baseUrl, runId)
      c  <- invariantC_quotaNonOverAdmission(client, baseUrl, runId)
      ok  = a.passed && b.passed && c.passed
      _  <- Console[IO].println(
              s"""
                 |=== Correctness Results ===
                 |  A) Token bucket non-over-issue     : ${if a.passed then "PASS" else "FAIL"}
                 |     ${a.details}
                 |  B) Idempotency exactly-one-Created : ${if b.passed then "PASS" else "FAIL"}
                 |     ${b.details}
                 |  C) Token quota non-over-admission  : ${if c.passed then "PASS" else "FAIL"}
                 |     ${c.details}
                 |
                 |Overall: ${if ok then "PASS" else "FAIL"}
                 |""".stripMargin
            )
    yield if ok then ExitCode.Success else ExitCode.Error

  private case class InvariantResult(passed: Boolean, details: String)

  private def invariantA_tokenBucketNonOverIssue(
    client:  Client[IO],
    baseUrl: String,
    runId:   String,
  ): IO[InvariantResult] =
    val concurrency   = 50
    val durationSecs  = 30
    val capacity      = 100      // default profile
    val refillPerSec  = 10       // default profile
    val epsilon       = 2        // small slack for clock skew
    val maxAllowed    = capacity + (refillPerSec * durationSecs) + epsilon
    val key           = s"correctness:A:$runId"  // fresh key so prior state doesn't bias

    val total   = new AtomicLong(0)
    val allowed = new AtomicLong(0)
    val blocked = new AtomicLong(0)
    val errors  = new AtomicLong(0)
    val sampler = new ErrorSampler(5)

    Console[IO].println(
      s"""-- Invariant A — token-bucket non-over-issue --
         |  key           = $key
         |  concurrency   = $concurrency
         |  duration      = ${durationSecs}s
         |  capacity      = $capacity
         |  refillPerSec  = $refillPerSec
         |  maxAllowed    = $maxAllowed  (capacity + refill*duration + $epsilon)
         |""".stripMargin
    ) *>
    IO.race(
      progressTicker("invariantA", durationSecs, total, allowed, blocked, errors),
      (0 until concurrency).toList.parTraverse_ { _ =>
        sendRateLimitCheck(client, baseUrl, key).flatMap {
          case Right(true)  => IO { total.incrementAndGet(); allowed.incrementAndGet() }
          case Right(false) => IO { total.incrementAndGet(); blocked.incrementAndGet() }
          case Left(msg)    => IO { total.incrementAndGet(); errors.incrementAndGet(); sampler.record(msg) }
        }.loop
      }
    ) *> IO.defer {
      val a     = allowed.get
      val t     = total.get
      val e     = errors.get
      val pass  = a <= maxAllowed && e == 0
      val why   =
        if pass then s"allowed=$a <= maxAllowed=$maxAllowed (total=$t, blocked=${blocked.get}, errors=$e)"
        else if e > 0 then s"errors=$e (total=$t, allowed=$a, blocked=${blocked.get})\n     Sample errors:\n${sampler.render}"
        else s"OVER-ISSUE: allowed=$a > maxAllowed=$maxAllowed (total=$t, blocked=${blocked.get}, errors=$e)"
      IO.pure(InvariantResult(pass, why))
    }

  private def invariantB_idempotencyExactlyOneCreated(
    client:  Client[IO],
    baseUrl: String,
    runId:   String,
  ): IO[InvariantResult] =
    val K             = 10
    val concurrency   = 50
    val durationSecs  = 30
    val keys          = (0 until K).map(i => s"correctness:B:$runId:key-$i").toVector

    val total     = new AtomicLong(0)
    val created   = new AtomicLong(0)
    val duplicate = new AtomicLong(0)
    val conflict  = new AtomicLong(0)
    val errors    = new AtomicLong(0)
    val sampler   = new ErrorSampler(5)

    Console[IO].println(
      s"""-- Invariant B — idempotency exactly-one-Created --
         |  K (shared keys) = $K
         |  concurrency     = $concurrency
         |  duration        = ${durationSecs}s
         |""".stripMargin
    ) *>
    IO.race(
      progressTicker("invariantB", durationSecs, total, created, duplicate, errors, okLabel = "created", nokLabel = "duplicate"),
      (0 until concurrency).toList.parTraverse_ { vuid =>
        // Each worker cycles through all K keys so every key is hit multiple times.
        def step(i: Int): IO[Unit] =
          val key = keys((vuid + i) % K)
          sendIdempotencyCheck(client, baseUrl, key).flatMap {
            case Right("new")         => IO { total.incrementAndGet(); created.incrementAndGet() }
            case Right("in_progress") => IO { total.incrementAndGet(); duplicate.incrementAndGet() }
            case Right("duplicate")   => IO { total.incrementAndGet(); duplicate.incrementAndGet() }
            case Right("conflict")    => IO { total.incrementAndGet(); conflict.incrementAndGet() }
            case Right(other)         => IO { total.incrementAndGet(); errors.incrementAndGet(); sampler.record(s"unexpected status: $other") }
            case Left(msg)            => IO { total.incrementAndGet(); errors.incrementAndGet(); sampler.record(msg) }
          } *> step(i + 1)
        step(0)
      }
    ) *> IO.defer {
      val c    = created.get
      val e    = errors.get
      val cf   = conflict.get
      val pass = c == K && e == 0 && cf == 0
      val why =
        if pass then s"created=$c == K=$K, conflicts=$cf, errors=$e, duplicates=${duplicate.get}"
        else
          val errSection = if e > 0 then s"\n     Sample errors:\n${sampler.render}" else ""
          s"VIOLATION: created=$c (expected $K), conflicts=$cf (expected 0), errors=$e (expected 0), duplicates=${duplicate.get}$errSection"
      IO.pure(InvariantResult(pass, why))
    }

  private def invariantC_quotaNonOverAdmission(
    client:  Client[IO],
    baseUrl: String,
    runId:   String,
  ): IO[InvariantResult] =
    val concurrency  = 50
    val durationSecs = 20
    val userLimit    = 1_000_000L   // TOKEN_QUOTA_USER_LIMIT default
    val perRequest   = 25_000L      // 40 admissions fill the window exactly
    val maxAdmitted  = userLimit / perRequest
    val userId       = s"correctness:C:$runId"

    val total     = new AtomicLong(0)
    val admitted  = new AtomicLong(0)
    val rejected  = new AtomicLong(0)
    val contended = new AtomicLong(0)
    val errors    = new AtomicLong(0)
    val sampler   = new ErrorSampler(5)

    Console[IO].println(
      s"""-- Invariant C — token-quota non-over-admission --
         |  userId        = $userId
         |  concurrency   = $concurrency
         |  duration      = ${durationSecs}s
         |  userLimit     = $userLimit
         |  perRequest    = $perRequest
         |  maxAdmitted   = $maxAdmitted
         |""".stripMargin
    ) *>
    IO.race(
      progressTicker("invariantC", durationSecs, total, admitted, rejected, errors, okLabel = "admitted", nokLabel = "rejected"),
      (0 until concurrency).toList.parTraverse_ { _ =>
        sendQuotaCheck(client, baseUrl, userId, perRequest).flatMap {
          case Right("allowed")   => IO { total.incrementAndGet(); admitted.incrementAndGet() }
          case Right("exceeded")  => IO { total.incrementAndGet(); rejected.incrementAndGet() }
          case Right("contended") => IO { total.incrementAndGet(); contended.incrementAndGet() }
          case Right(other)       => IO { total.incrementAndGet(); errors.incrementAndGet(); sampler.record(s"unexpected outcome: $other") }
          case Left(msg)          => IO { total.incrementAndGet(); errors.incrementAndGet(); sampler.record(msg) }
        }.loop
      }
    ) *> IO.defer {
      val adm    = admitted.get
      val rej    = rejected.get
      val con    = contended.get
      val e      = errors.get
      val tokens = adm * perRequest
      val pass   = tokens <= userLimit && rej > 0 && e == 0
      val why =
        if pass then s"admitted=$adm ($tokens tokens) <= userLimit=$userLimit, rejected=$rej, contended=$con, errors=$e"
        else if e > 0 then s"errors=$e (admitted=$adm, rejected=$rej, contended=$con)\n     Sample errors:\n${sampler.render}"
        else if rej == 0 then s"VACUOUS: limit never reached (admitted=$adm, contended=$con) — quotas disabled or stack too slow"
        else s"OVER-ADMISSION: admitted=$adm ($tokens tokens) > userLimit=$userLimit (rejected=$rej, contended=$con)"
      IO.pure(InvariantResult(pass, why))
    }

  // ------------------------------------------------------------------
  // Latency scenario: fixed-RPS with HdrHistogram
  // ------------------------------------------------------------------

  /**
   * latency: drive a fixed request rate and record per-request latency with
   * HdrHistogram. Emits a single-row markdown table suitable for
   * docs/PERFORMANCE.md. Run multiple times at different --rps to build the
   * full table.
   *
   * Uses a closed-loop schedule across a pool of workers: inter-request
   * interval per worker = workers * (1s / rps). This matches the default
   * behaviour of k6 and most HTTP benchmark tools.
   */
  def latency(
    client:       Client[IO],
    baseUrl:      String,
    rps:          Int,
    durationSecs: Int,
  ): IO[ExitCode] =
    val workers         = math.min(math.max(rps / 10, 8), 128)
    val perWorkerNanos  = (1_000_000_000L * workers.toLong) / rps.toLong
    val keysetSize      = 1000
    // Unique per-run key prefix so repeat invocations start from fresh buckets.
    val keyPrefix       = s"latency:${System.currentTimeMillis}"

    // ConcurrentHistogram is lock-free (no synchronized needed) and the
    // correct choice under high parallelism.
    val histogram = new ConcurrentHistogram(3_600_000_000L, 3) // 0..3600s range, 3 sig digits
    val errors    = new AtomicLong(0)
    val total     = new AtomicLong(0)
    val successes = new AtomicLong(0)
    val rejected  = new AtomicLong(0)
    val sampler   = new ErrorSampler(5)

    // Error budget: anything above 1% means the numbers are lying — the
    // histogram is dominated by non-latency signal (timeouts, connection
    // failures, 5xx). We fail the run loudly instead of emitting a
    // misleading markdown row.
    val errorBudgetPct = 1.0

    Console[IO].println(
      s"""=== latency — target ${rps} RPS for ${durationSecs}s ===
         |  workers           = $workers
         |  inter-req/worker  = ${perWorkerNanos / 1_000_000}.${(perWorkerNanos % 1_000_000) / 1000} ms
         |  keyset size       = $keysetSize  (low contention)
         |  error budget      = ${errorBudgetPct}%%
         |""".stripMargin
    ) *>
    IO.race(
      IO.sleep(durationSecs.seconds),
      (0 until workers).toList.parTraverse_ { wid =>
        def step(i: Int): IO[Unit] =
          val key = s"$keyPrefix:${(wid * 1_000_003 + i) % keysetSize}"
          for
            t0   <- IO.monotonic
            res  <- sendRateLimitCheck(client, baseUrl, key)
            t1   <- IO.monotonic
            micros = (t1 - t0).toMicros
            _ <- IO {
              total.incrementAndGet()
              res match
                case Right(true) =>
                  successes.incrementAndGet()
                  // Only record honest server decisions in the histogram.
                  // Connection failures, 5xx, parse errors would otherwise
                  // skew the percentiles in either direction depending on
                  // where they fail, making p50/p99 meaningless.
                  histogram.recordValue(math.min(micros, 3_600_000_000L))
                case Right(false) =>
                  rejected.incrementAndGet()
                  histogram.recordValue(math.min(micros, 3_600_000_000L))
                case Left(msg) =>
                  errors.incrementAndGet()
                  sampler.record(msg)
            }
            // Pace to target RPS. If the request itself took longer than the
            // target interval, skip sleeping (closed-loop — RPS will fall
            // short of target and the table shows that honestly via p99 rising).
            remaining = perWorkerNanos - (t1 - t0).toNanos
            _ <- if remaining > 0 then IO.sleep(remaining.nanos) else IO.unit
            _ <- step(i + 1)
          yield ()
        step(0)
      }
    ) *> IO.defer {
      val t         = total.get
      val e         = errors.get
      val s         = successes.get
      val r         = rejected.get
      val errorPct  = if t > 0 then (e.toDouble / t.toDouble) * 100.0 else 0.0
      val p50       = histogram.getValueAtPercentile(50.0) / 1000.0   // ms
      val p95       = histogram.getValueAtPercentile(95.0) / 1000.0
      val p99       = histogram.getValueAtPercentile(99.0) / 1000.0
      val p999      = histogram.getValueAtPercentile(99.9) / 1000.0
      val actualRps = t.toDouble / durationSecs.toDouble
      val ok        = errorPct <= errorBudgetPct

      val summary = f"""latency results ($durationSecs s window, target $rps RPS):
                       |  total requests  = $t
                       |  actual RPS      = $actualRps%.1f  (target $rps)
                       |  successes (200) = $s
                       |  rejected  (429) = $r
                       |  errors          = $e  ($errorPct%.2f%%)
                       |  p50 latency     = $p50%.2f ms      (successes+rejections only)
                       |  p95 latency     = $p95%.2f ms
                       |  p99 latency     = $p99%.2f ms
                       |  p99.9 latency   = $p999%.2f ms
                       |""".stripMargin

      val errSection =
        if e > 0 then
          s"""|
              |  sample errors (first ${math.min(5, e).toInt} distinct):
              |${sampler.render}
              |""".stripMargin
        else ""

      if ok then
        val mdRow = f"""|
                        |Markdown row for docs/PERFORMANCE.md:
                        || $rps | $actualRps%.0f | $p50%.1f | $p95%.1f | $p99%.1f | $p999%.1f | $errorPct%.2f |
                        |""".stripMargin
        Console[IO].println(summary + errSection + mdRow).as(ExitCode.Success)
      else
        val failBanner = f"""|
                             |FAIL: error rate $errorPct%.2f%% exceeds budget $errorBudgetPct%.2f%%.
                             |No markdown row emitted — these numbers would mislead readers.
                             |Fix the errors above and re-run.
                             |""".stripMargin
        Console[IO].errorln(summary + errSection + failBanner).as(ExitCode.Error)
    }

  // ------------------------------------------------------------------
  // Shared helpers
  // ------------------------------------------------------------------

  private val ProgressIntervalSecs = 5

  private def progressTicker(
    label:    String,
    duration: Int,
    total:    AtomicLong,
    ok:       AtomicLong,
    nok:      AtomicLong,
    errors:   AtomicLong,
    okLabel:  String = "allowed",
    nokLabel: String = "blocked",
  ): IO[Unit] =
    def tick(elapsed: Int): IO[Unit] =
      if elapsed >= duration then IO.unit
      else
        IO.sleep(ProgressIntervalSecs.seconds) *>
          IO.defer {
            val e   = elapsed + ProgressIntervalSecs
            val t   = total.get
            val rps = if e > 0 then t / e else 0
            Console[IO].println(
              f"  [$label] ${e}s / ${duration}s  |  total=$t  $okLabel=${ok.get}  $nokLabel=${nok.get}  errors=${errors.get}  (~${rps} RPS)"
            ) *> tick(e)
          }
    tick(0)

  private case class RunConfig(
    concurrency:  Int,
    durationSecs: Int,
    description:  String,
    keyFn:        Int => String,
  )

  private def runRateLimitScenario(
    client:  Client[IO],
    baseUrl: String,
    cfg:     RunConfig,
  ): IO[Unit] =
    val total   = new AtomicLong(0)
    val allowed = new AtomicLong(0)
    val blocked = new AtomicLong(0)
    val errors  = new AtomicLong(0)

    Console[IO].println(s"=== ${cfg.description} ===") *>
    IO.race(
      progressTicker(cfg.description.takeWhile(_ != ' '), cfg.durationSecs, total, allowed, blocked, errors),
      (0 until cfg.concurrency).toList.parTraverse_ { vuid =>
        (sendRateLimitCheck(client, baseUrl, cfg.keyFn(vuid)).flatMap {
          case Right(true)  => IO { total.incrementAndGet(); allowed.incrementAndGet() }
          case Right(false) => IO { total.incrementAndGet(); blocked.incrementAndGet() }
          case Left(_)      => IO { total.incrementAndGet(); errors.incrementAndGet() }
        }).loop
      }
    ) *>
    printRateLimitResults(cfg.description, total, allowed, blocked, errors, None, None)

  private def printRateLimitResults(
    desc:    String,
    total:   AtomicLong,
    allowed: AtomicLong,
    blocked: AtomicLong,
    errors:  AtomicLong,
    avgMs:   Option[Long],
    maxMs:   Option[Long],
  ): IO[Unit] =
    IO {
      val t = total.get
      s"""$desc results:
         |  total     = $t
         |  allowed   = ${allowed.get}  (${if t > 0 then allowed.get * 100 / t else 0}%)
         |  blocked   = ${blocked.get}  (${if t > 0 then blocked.get * 100 / t else 0}%)
         |  errors    = ${errors.get}
         |${avgMs.fold("")(ms => s"  avg lat   = ${ms}ms\n")}${maxMs.fold("")(ms => s"  max lat   = ${ms}ms\n")}""".stripMargin
    }.flatMap(Console[IO].println)

object Http:
  case class RateLimitRequest(key: String, cost: Int = 1)
  case class RateLimitResponse(allowed: Boolean)
  case class IdempotencyRequest(idempotencyKey: String, ttl: Int = 3600)
  case class IdempotencyResponse(status: String)

  /**
   * Preflight liveness probe. Returns Right(()) iff /health returns 200.
   * Every load scenario calls this before doing real work so that a dead or
   * misconfigured service fails loudly up front instead of producing a
   * histogram full of "connection refused" measurements.
   */
  def healthCheck(client: Client[IO], baseUrl: String): IO[Either[String, Unit]] =
    val req = Request[IO](
      method = Method.GET,
      uri    = Uri.unsafeFromString(s"$baseUrl/health"),
    )
    client
      .run(req)
      .use { resp =>
        resp.as[String].map { body =>
          if resp.status.code == 200 then Right(())
          else Left(s"HTTP ${resp.status.code}: ${body.take(200)}")
        }
      }
      .handleError(e => Left(Option(e.getMessage).getOrElse(e.getClass.getSimpleName)))

  def sendRateLimitCheck(client: Client[IO], baseUrl: String, key: String): IO[Either[String, Boolean]] =
    val body = Json.obj("key" := key, "cost" := 1)
    val req  = Request[IO](
      method  = Method.POST,
      uri     = Uri.unsafeFromString(s"$baseUrl/v1/ratelimit/check"),
      headers = Headers(
        "Content-Type"  -> "application/json",
        "Authorization" -> s"Bearer ${LoadSim.defaultApiKey}",
      ),
    ).withEntity(body.noSpaces)

    client.run(req).use { resp =>
      resp.as[String].map { body =>
        if resp.status.code == 200 || resp.status.code == 429 then
          io.circe.parser.parse(body)
            .flatMap(_.hcursor.get[Boolean]("allowed"))
            .left.map(_.getMessage)
        else
          Left(s"HTTP ${resp.status.code}: $body")
      }
    }.handleError(e => Left(e.getMessage))

  def sendIdempotencyCheck(client: Client[IO], baseUrl: String, key: String): IO[Either[String, String]] =
    val body = Json.obj("idempotencyKey" := key, "ttl" := 3600)
    val req  = Request[IO](
      method  = Method.POST,
      uri     = Uri.unsafeFromString(s"$baseUrl/v1/idempotency/check"),
      headers = Headers(
        "Content-Type"  -> "application/json",
        "Authorization" -> s"Bearer ${LoadSim.defaultApiKey}",
      ),
    ).withEntity(body.noSpaces)

    client.run(req).use { resp =>
      resp.as[String].map { body =>
        if resp.status.code == 200 then
          io.circe.parser.parse(body)
            .flatMap(_.hcursor.get[String]("status"))
            .left.map(_.getMessage)
        else if resp.status.code == 409 then
          Right("conflict")
        else
          Left(s"HTTP ${resp.status.code}: $body")
      }
    }.handleError(e => Left(e.getMessage))

  /** Right("allowed" | "exceeded" | "contended") for the three decision outcomes; Left for anything else. */
  def sendQuotaCheck(client: Client[IO], baseUrl: String, userId: String, estimatedInputTokens: Long): IO[Either[String, String]] =
    val body = Json.obj("userId" := userId, "estimatedInputTokens" := estimatedInputTokens)
    val req  = Request[IO](
      method  = Method.POST,
      uri     = Uri.unsafeFromString(s"$baseUrl/v1/quota/check"),
      headers = Headers(
        "Content-Type"  -> "application/json",
        "Authorization" -> s"Bearer ${LoadSim.defaultApiKey}",
      ),
    ).withEntity(body.noSpaces)

    client.run(req).use { resp =>
      resp.as[String].map { body =>
        resp.status.code match
          case 200  => Right("allowed")
          case 429  => Right("exceeded")
          case 503  => Right("contended")
          case code => Left(s"HTTP $code: $body")
      }
    }.handleError(e => Left(e.getMessage))

// IO.loop helper — run an action forever until cancelled
extension [A](io: IO[A])
  def loop: IO[Nothing] = io.foreverM
