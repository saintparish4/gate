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
 *                      C) token-quota non-over-admission
 *                      D) cross-tenant isolation: two clients racing on the
 *                         same idempotency keys, quota user, and reservations
 *                    exits 0 on PASS, 1 on VIOLATION (an invariant was
 *                    broken), 2 on INCONCLUSIVE (the run could not tell:
 *                    errors, degraded answers, or a limit never reached)
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
 *   --concurrency N               workers for the correctness scenario; scales all
 *                                 three invariants (defaults A=20, B=50, C=50).
 *                                 Lower it to match a small target -- a deployment
 *                                 that cannot serve the load measures nothing.
 *   --api-key KEY                 key for the warm-up and invariants B and C
 *                                 (default test-api-key)
 *   --free-key KEY                Free-tier key whose 20-token bucket invariant A
 *                                 drains (default free-api-key)
 *
 * The defaults are the built-in development keys, which only docker-compose
 * serves. A deployed stack loads its keys from Secrets Manager, so pass its
 * keys with the two --*-key flags (`make correctness` does, from API_KEY and
 * FREE_API_KEY).
 */
object LoadSim extends IOApp:

  val defaultBaseUrl = "http://localhost:8080"
  val defaultApiKey  = "test-api-key"
  val defaultFreeKey  = "free-api-key"

  override def run(args: List[String]): IO[ExitCode] =
    val scenario = Args.string(args, "--scenario", "normal")
    val baseUrl  = Args.string(args, "--url", defaultBaseUrl)
    val rps      = Args.intOpt(args, "--rps")
    val duration = Args.intOpt(args, "--duration")
    // Invariant A's contention level. Kept low enough that LocalStack can
    // actually answer, since a target that cannot serve the load degrades and
    // the invariant then measures degradation instead of the token bucket.
    val concurrency = Args.intOpt(args, "--concurrency")
    val apiKey      = Args.string(args, "--api-key", defaultApiKey)
    val freeKey     = Args.string(args, "--free-key", defaultFreeKey)

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
        case "correctness"    => withPreflight(Scenarios.correctness(client, baseUrl, concurrency, apiKey, freeKey))
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
  // Correctness scenario: four invariants, CI-gated
  // ------------------------------------------------------------------

  /**
   * correctness: runs four invariants. Each ends as PASS, VIOLATION or
   * INCONCLUSIVE (see Verdict); the run exits 0, 1 or 2 for the worst of them.
   *
   * Invariant A — token-bucket non-over-issue:
   *   Server must never issue more tokens than capacity + refillRate * window.
   *   Uses free-api-key (Free tier): capacity=20 tokens, refillRate=2 tokens/s.
   *   N=20 parallel clients target a single unique key for T=30s.
   *   The bucket refills on the server's wall clock, so the window is read from
   *   the server: the span of the `Date` headers on its answers. That header
   *   has one-second resolution, which costs a second of allowance, and a
   *   second more covers the first request's latency:
   *     allowed_count <= capacity + refillRate * (date_span + 2s)
   *   The window used to be this machine's monotonic clock plus a 5 s
   *   allowance (10 tokens of 80), added when runs showed 2-3 s of refill the
   *   client had not seen (issue #10) and blamed on clock corrections on the
   *   server. On 1 October 2026 the same excess appeared on every run driven
   *   from one laptop, against a local server and against AWS, and on no CI
   *   run; that laptop's monotonic clock measured 3.2-3.5% slow against an
   *   outside clock. The load generator had under-measured its own window.
   *   Also: allowed_count >= a quarter of the nominal budget (a limiter that
   *   goes dark is broken too), at least one request blocked (otherwise the
   *   run proved nothing), no errors, and no answer from degradation mode.
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
   *
   * Invariant D — cross-tenant isolation (ADR-005):
   *   Two clients, --api-key (A) and --free-key (B), race for 20 s on the same
   *   visible identifiers. Each is its own tenant.
   *     D1: both drive the same K=10 idempotency keys; each must create exactly
   *         K records of its own. Shared keys would give K between them.
   *     D2: both fill the same quota user; each is held to 40 admissions, and
   *         together they must pass 40, which one shared counter cannot.
   *     D3: while D2 runs, B reconciles every reservation A is granted, with
   *         zero usage. Every attempt must answer 404.
   *   Assert all three, no conflicts, no HTTP errors.
   */
  def correctness(
    client:      Client[IO],
    baseUrl:     String,
    concurrency: Option[Int] = None,
    apiKey:      String = LoadSim.defaultApiKey,
    freeKey:     String = LoadSim.defaultFreeKey,
  ): IO[ExitCode] =
    val runId = System.currentTimeMillis.toString
    // One --concurrency scales all three invariants. Absent, each keeps its own
    // default: B and C need more contention than A to prove anything.
    for
      _  <- Console[IO].println(s"=== correctness — run $runId ===")
      _  <- warmUp(client, baseUrl, runId, apiKey)
      a  <- invariantA_tokenBucketNonOverIssue(client, baseUrl, runId, concurrency.getOrElse(20), freeKey)
      b  <- invariantB_idempotencyExactlyOneCreated(client, baseUrl, runId, concurrency.getOrElse(50), apiKey)
      c  <- invariantC_quotaNonOverAdmission(client, baseUrl, runId, concurrency.getOrElse(50), apiKey)
      d  <- invariantD_crossTenantIsolation(client, baseUrl, runId, concurrency.getOrElse(10), apiKey, freeKey)
      overall = Verdict.overall(List(a, b, c, d).map(_.verdict))
      _  <- Console[IO].println(
              s"""
                 |=== Correctness Results ===
                 |  A) Token bucket non-over-issue     : ${a.verdict.label}
                 |     ${a.details}
                 |  B) Idempotency exactly-one-Created : ${b.verdict.label}
                 |     ${b.details}
                 |  C) Token quota non-over-admission  : ${c.verdict.label}
                 |     ${c.details}
                 |  D) Cross-tenant isolation          : ${d.verdict.label}
                 |     ${d.details}
                 |
                 |Overall: ${overall.label}
                 |""".stripMargin
            )
    yield overall.exitCode

  /** The arithmetic of invariant A, kept apart from the driving so it can be
    * tested on its own.
    */
  object InvariantA:

    /** Seconds added to a client-measured window: the tail of requests still
      * answered after the workers were told to stop.
      */
    val ClientAllowanceSecs = 1

    /** Seconds added to a server-measured window: one because a `Date` header
      * has one-second resolution, one because the bucket is created by the
      * first request, a little before its answer is dated.
      */
    val ServerAllowanceSecs = 2

    /** How far this machine's two clocks may disagree before a window measured
      * with them is not trusted.
      */
    val MaxClockGapPct = 1.0

    /** How long the workers ran.
      *
      * @param serverSecs
      *   Last `Date` header minus the first, when every answer carried one
      */
    final case class Window(monotonicSecs: Double, wallSecs: Double, serverSecs: Option[Long]):
      /** How far apart this machine's two clocks put the same window. */
      def clientGapPct: Double =
        val longer = math.max(monotonicSecs, wallSecs)
        if longer <= 0 then 0.0 else math.abs(monotonicSecs - wallSecs) / longer * 100.0

      def clientClocksAgree: Boolean = clientGapPct <= MaxClockGapPct

      def describe: String =
        val server = serverSecs.fold("server=unknown (no Date header)")(s => s"server=${s}s")
        val gap    = if clientClocksAgree then "" else f", THIS MACHINE'S CLOCKS DISAGREE BY $clientGapPct%.1f%%"
        f"window: $server, client monotonic=$monotonicSecs%.1fs wall=$wallSecs%.1fs$gap"

    final case class Ceiling(maxAllowed: Long, windowSecs: Double, source: String)

    /** The most a correct bucket can have issued over `window`, or why that
      * cannot be said.
      *
      * The server's clock is used when it is known: it is the clock the bucket
      * refills on, and it does not depend on the machine running the load.
      * Failing that, this machine's clocks are used only while they agree with
      * each other; a window they disagree on has no ceiling worth asserting.
      */
    def ceiling(capacity: Int, refillPerSec: Int, window: Window): Either[String, Ceiling] =
      def over(secs: Double, source: String): Ceiling =
        Ceiling(capacity + math.ceil(refillPerSec * secs).toLong, secs, source)
      window.serverSecs match
        case Some(span) => Right(over(span.toDouble + ServerAllowanceSecs, "server"))
        case None if window.clientClocksAgree =>
          Right(over(math.max(window.monotonicSecs, window.wallSecs) + ClientAllowanceSecs, "client"))
        case None =>
          Left("CLOCKS DISAGREE: the answers carried no Date header, and this machine's monotonic and wall clocks put the window more than 1% apart, so there is no window to compute a ceiling from")

  /**
   * What one invariant's run showed.
   *
   * There used to be two outcomes, and anything short of a clean pass printed
   * FAIL. A run in which eight calls timed out and no invariant was broken read
   * "VIOLATION: created=10 (expected 10) ... errors=5", and was reported as two
   * failed invariants. "Could not answer" is not "answered wrong":
   *
   *   Pass          the property held, and the run was able to show it
   *   Violation     the property was broken: more issued, admitted or created
   *                 than the limit allows, or state crossed between tenants
   *   Inconclusive  the run cannot say: requests errored, answers came from
   *                 degradation mode, or a limit was never reached
   *
   * A violation is reported even when the run also had errors, because an error
   * cannot make a limiter issue more. Both non-pass outcomes exit non-zero, so
   * CI is no less strict; they differ in what to do next.
   */
  enum Verdict(val label: String, val exitCode: ExitCode):
    case Pass         extends Verdict("PASS", ExitCode.Success)
    case Violation    extends Verdict("VIOLATION", ExitCode(1))
    case Inconclusive extends Verdict("INCONCLUSIVE", ExitCode(2))

  object Verdict:
    /** A violation anywhere outranks an inconclusive run, which outranks a pass. */
    def overall(verdicts: List[Verdict]): Verdict =
      if verdicts.contains(Violation) then Violation
      else if verdicts.contains(Inconclusive) then Inconclusive
      else Pass

  case class InvariantResult(verdict: Verdict, details: String)

  /**
   * Ramps 1 -> 5 -> 20 workers over ~15 s on throwaway keys before any invariant
   * runs. A cold JVM answering a 50-way burst exceeds the server's 2 s check
   * timeout, which trips its circuit breaker and blacks out the whole window; the
   * invariants are about correctness, not cold-start latency, so I warm the hot
   * path first the way real traffic would.
   */
  private def warmUp(client: Client[IO], baseUrl: String, runId: String, apiKey: String): IO[Unit] =
    val stages = List((1, 5), (5, 5), (20, 5)) // (workers, seconds)
    Console[IO].println("-- Warm-up — ramping 1 -> 5 -> 20 workers on throwaway keys --") *>
    stages.zipWithIndex.traverse_ { case ((workers, secs), stage) =>
      val ok  = new AtomicLong(0)
      val bad = new AtomicLong(0)
      IO.race(
        IO.sleep(secs.seconds),
        (0 until workers).toList.parTraverse_ { w =>
          sendRateLimitCheck(client, baseUrl, s"warmup:$runId:$stage:$w", apiKey).flatMap {
            case Right(_) => IO(ok.incrementAndGet()).void
            case Left(_)  => IO(bad.incrementAndGet()).void
          }.loop
        },
      ).flatMap(_ => Console[IO].println(f"  [warmup] stage ${stage + 1} — $workers%2d workers x ${secs}s  |  ok=${ok.get}  errors=${bad.get}"))
    }

  private def invariantA_tokenBucketNonOverIssue(
    client:      Client[IO],
    baseUrl:     String,
    runId:       String,
    concurrency: Int,
    freeKey:     String,
  ): IO[InvariantResult] =
    val durationSecs  = 30
    // A Free-tier key (20 tokens, 2 tokens/s in application.conf). Premium
    // refills faster than this driver can send against LocalStack, so it would
    // never block and prove nothing.
    val apiKey        = freeKey
    val capacity      = 20
    val refillPerSec  = 2
    // maxAllowed is computed after the run, from the measured window (see
    // InvariantA.ceiling).
    val minAllowed    = (capacity + (refillPerSec * durationSecs)) / 4
    val key           = s"correctness:A:$runId"  // fresh key so prior state doesn't bias

    val total   = new AtomicLong(0)
    val allowed = new AtomicLong(0)
    val blocked = new AtomicLong(0)
    val errors  = new AtomicLong(0)
    // Answers that came from degradation mode, by their X-Gate-Degraded header.
    // This used to be the change in gate_degraded_total across the run, read
    // from /metrics. Behind a load balancer each read lands on whichever task
    // answers, so with two tasks the before and after came from different
    // counters and the difference meant nothing.
    val degraded = new AtomicLong(0)
    // The server's clock, as its Date headers show it: earliest and latest
    // second seen, and how many answers carried none.
    val firstDate = new AtomicLong(Long.MaxValue)
    val lastDate  = new AtomicLong(Long.MinValue)
    val undated   = new AtomicLong(0)
    val sampler = new ErrorSampler(5)

    // Returns the window the workers ran for, by each clock.
    def drive: IO[InvariantA.Window] =
      (IO.monotonic, IO.realTime).tupled.flatMap { (monoStart, wallStart) =>
        IO.race(
          progressTicker("invariantA", durationSecs, total, allowed, blocked, errors),
          (0 until concurrency).toList.parTraverse_ { _ =>
            sendRateLimitCheckDetailed(client, baseUrl, key, apiKey).flatMap {
              case Right(answer) => IO {
                  total.incrementAndGet()
                  if answer.degraded then degraded.incrementAndGet()
                  if answer.allowed then allowed.incrementAndGet() else blocked.incrementAndGet()
                  answer.serverEpochSec match
                    case Some(sec) =>
                      firstDate.accumulateAndGet(sec, math.min(_, _))
                      lastDate.accumulateAndGet(sec, math.max(_, _))
                    case None => undated.incrementAndGet()
                }
              case Left(msg)    => IO { total.incrementAndGet(); errors.incrementAndGet(); sampler.record(msg) }
            }.loop
          }
        ) *> (IO.monotonic, IO.realTime).mapN { (monoEnd, wallEnd) =>
          InvariantA.Window(
            monotonicSecs = (monoEnd - monoStart).toMillis / 1000.0,
            wallSecs      = (wallEnd - wallStart).toMillis / 1000.0,
            serverSecs    =
              if undated.get == 0 && lastDate.get >= firstDate.get then Some(lastDate.get - firstDate.get)
              else None,
          )
        }
      }

    def verdict(window: InvariantA.Window): IO[InvariantResult] =
      val d = degraded.get
      val a = allowed.get
      val b = blocked.get
      val t = total.get
      val e = errors.get
      val ceiling     = InvariantA.ceiling(capacity, refillPerSec, window)
      val maxAllowed  = ceiling.map(_.maxAllowed).getOrElse(Long.MaxValue)
      val windowText  = window.describe
      val result =
        // Degradation mode answers without consulting the bucket, so allowed/
        // blocked stop describing the limiter. Reject-all pins allowed and
        // inflates blocked, which reads exactly like a healthy limiter holding
        // the line; allow-all admits past the ceiling by design. Either way
        // the counts measure nothing, so this is checked before the ceiling.
        if d > 0 then InvariantResult(Verdict.Inconclusive, s"DEGRADED: $d answer(s) carried X-Gate-Degraded: they came from degradation mode, not the token bucket (circuit breaker open, bulkhead full, or store errors) — allowed=$a measures nothing. Check gate_degraded_total and gate_circuit_breaker_state.")
        // Without a window there is no ceiling to compare against.
        else if ceiling.isLeft then InvariantResult(Verdict.Inconclusive, s"${ceiling.left.getOrElse("")} ($windowText, allowed=$a, total=$t, blocked=$b, errors=$e)")
        else if a > maxAllowed then InvariantResult(Verdict.Violation, s"OVER-ISSUE: allowed=$a > maxAllowed=$maxAllowed ($windowText, total=$t, blocked=$b, errors=$e)")
        else if e > 0 then InvariantResult(Verdict.Inconclusive, s"errors=$e (total=$t, allowed=$a, blocked=$b)\n     Sample errors:\n${sampler.render}")
        else if b == 0 then InvariantResult(Verdict.Inconclusive, s"VACUOUS: never blocked (total=$t, allowed=$a) — the bucket was never exhausted")
        else if a < minAllowed then InvariantResult(Verdict.Violation, s"UNDER-ISSUE: allowed=$a < minAllowed=$minAllowed (total=$t, blocked=$b) — the limiter went dark")
        else InvariantResult(Verdict.Pass, s"minAllowed=$minAllowed <= allowed=$a <= maxAllowed=$maxAllowed ($windowText, total=$t, blocked=$b, errors=$e)")
      IO.pure(result)

    Console[IO].println(
      s"""-- Invariant A — token-bucket non-over-issue --
         |  key           = $key
         |  apiKey        = ${if apiKey == LoadSim.defaultFreeKey then apiKey else "from --free-key (not printed)"} (Free tier)
         |  concurrency   = $concurrency
         |  duration      = ${durationSecs}s
         |  capacity      = $capacity
         |  refillPerSec  = $refillPerSec
         |  maxAllowed    = capacity + refill*(window + allowance)  (the window is measured, by the server's clock; reported below)
         |  minAllowed    = $minAllowed  (a quarter of the nominal budget; catches a limiter that rejects everything)
         |""".stripMargin
    ) *>
    drive.flatMap(verdict)

  private def invariantB_idempotencyExactlyOneCreated(
    client:      Client[IO],
    baseUrl:     String,
    runId:       String,
    concurrency: Int,
    apiKey:      String,
  ): IO[InvariantResult] =
    val K             = 10
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
          sendIdempotencyCheck(client, baseUrl, key, apiKey).flatMap {
            case Right("new")         => IO { total.incrementAndGet(); created.incrementAndGet() }
            case Right("in_progress") => IO { total.incrementAndGet(); duplicate.incrementAndGet() }
            case Right("duplicate")   => IO { total.incrementAndGet(); duplicate.incrementAndGet() }
            case Right("conflict")    => IO { total.incrementAndGet(); conflict.incrementAndGet() }
            case Right(other)         => IO { total.incrementAndGet(); errors.incrementAndGet(); sampler.record(s"unexpected status: $other") }
            case Left(msg)            => IO { total.incrementAndGet(); errors.incrementAndGet(); sampler.record(msg) }
          } >> step(i + 1)
        step(0)
      }
    ) *> IO.defer {
      val c    = created.get
      val e    = errors.get
      val cf   = conflict.get
      val counts     = s"created=$c (K=$K), conflicts=$cf, errors=$e, duplicates=${duplicate.get}"
      val errSection = if e > 0 then s"\n     Sample errors:\n${sampler.render}" else ""
      val result =
        if c > K then InvariantResult(Verdict.Violation, s"DOUBLE CLAIM: more than one 'new' for a key — $counts$errSection")
        else if cf > 0 then InvariantResult(Verdict.Violation, s"CONFLICT: a 409 for an identical request — $counts$errSection")
        // A missing 'new' with errors may be a claim whose answer was lost.
        else if e > 0 then InvariantResult(Verdict.Inconclusive, s"$counts$errSection")
        else if c < K then InvariantResult(Verdict.Violation, s"NEVER CLAIMED: a key was never answered 'new' — $counts")
        else InvariantResult(Verdict.Pass, counts)
      IO.pure(result)
    }

  private def invariantC_quotaNonOverAdmission(
    client:      Client[IO],
    baseUrl:     String,
    runId:       String,
    concurrency: Int,
    apiKey:      String,
  ): IO[InvariantResult] =
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
        sendQuotaCheck(client, baseUrl, userId, perRequest, apiKey).flatMap {
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
      val result =
        if tokens > userLimit then InvariantResult(Verdict.Violation, s"OVER-ADMISSION: admitted=$adm ($tokens tokens) > userLimit=$userLimit (rejected=$rej, contended=$con, errors=$e)")
        else if e > 0 then InvariantResult(Verdict.Inconclusive, s"errors=$e (admitted=$adm, rejected=$rej, contended=$con)\n     Sample errors:\n${sampler.render}")
        else if rej == 0 then InvariantResult(Verdict.Inconclusive, s"VACUOUS: limit never reached (admitted=$adm, contended=$con) — quotas disabled or stack too slow")
        else InvariantResult(Verdict.Pass, s"admitted=$adm ($tokens tokens) <= userLimit=$userLimit, rejected=$rej, contended=$con, errors=$e")
      IO.pure(result)
    }

  private def invariantD_crossTenantIsolation(
    client:      Client[IO],
    baseUrl:     String,
    runId:       String,
    concurrency: Int,
    keyA:        String,
    keyB:        String,
  ): IO[InvariantResult] =
    val durationSecs = 20
    val K            = 10
    val idemKeys     = (0 until K).map(i => s"correctness:D:$runId:key-$i").toVector
    val userId       = s"correctness:D:$runId"
    val userLimit    = 1_000_000L   // TOKEN_QUOTA_USER_LIMIT default
    val perRequest   = 25_000L
    val maxAdmitted  = userLimit / perRequest

    final class Tenant(val key: String):
      val created   = new AtomicLong(0)
      val conflicts = new AtomicLong(0)
      val admitted  = new AtomicLong(0)

    val a = new Tenant(keyA)
    val b = new Tenant(keyB)
    val total         = new AtomicLong(0)
    val admittedAll   = new AtomicLong(0)
    val rejectedAll   = new AtomicLong(0)
    val errors        = new AtomicLong(0)
    val thefts        = new AtomicLong(0)
    val theftsLanded  = new AtomicLong(0)
    // Reservations granted to A, which B then tries to reconcile.
    val grantedToA    = new java.util.concurrent.CopyOnWriteArrayList[String]()
    val sampler       = new ErrorSampler(5)

    def fail(msg: String): IO[Unit] =
      IO { total.incrementAndGet(); errors.incrementAndGet(); sampler.record(msg) }

    def idempotency(t: Tenant, worker: Int): IO[Unit] =
      def step(i: Int): IO[Unit] =
        sendIdempotencyCheck(client, baseUrl, idemKeys((worker + i) % K), t.key).flatMap {
          case Right("new")                       => IO { total.incrementAndGet(); t.created.incrementAndGet() }
          case Right("in_progress" | "duplicate") => IO(total.incrementAndGet()).void
          case Right("conflict")                  => IO { total.incrementAndGet(); t.conflicts.incrementAndGet() }
          case Right(other)                       => fail(s"unexpected idempotency status: $other")
          case Left(msg)                          => fail(msg)
        } >> step(i + 1)
      step(0)

    def quota(t: Tenant): IO[Unit] =
      sendQuotaCheckReserving(client, baseUrl, userId, perRequest, t.key).flatMap {
        case Right(("allowed", id)) => IO {
            total.incrementAndGet(); admittedAll.incrementAndGet(); t.admitted.incrementAndGet()
            if t eq a then id.foreach(grantedToA.add)
          }.void
        case Right(("exceeded", _))  => IO { total.incrementAndGet(); rejectedAll.incrementAndGet() }.void
        case Right(("contended", _)) => IO(total.incrementAndGet()).void
        case Right((other, _))       => fail(s"unexpected quota outcome: $other")
        case Left(msg)               => fail(msg)
      }.loop

    def steal(id: String): IO[Unit] =
      sendQuotaReconcile(client, baseUrl, id, 0L, b.key).flatMap {
        case Right(404)  => IO(thefts.incrementAndGet()).void
        case Right(code) => IO {
            thefts.incrementAndGet(); theftsLanded.incrementAndGet()
            sampler.record(s"B reconciled A's reservation: HTTP $code")
          }.void
        case Left(msg) => fail(msg)
      }

    val thief: IO[Unit] = IO.defer {
      val n = grantedToA.size
      if n == 0 then IO.sleep(20.millis)
      else steal(grantedToA.get(java.util.concurrent.ThreadLocalRandom.current().nextInt(n)))
    }.loop

    if keyA == keyB then
      IO.pure(InvariantResult(Verdict.Inconclusive, "D needs two different keys: --api-key and --free-key are the same, so there is only one tenant"))
    else
      Console[IO].println(
        s"""-- Invariant D — cross-tenant isolation --
           |  clients       = A (--api-key) and B (--free-key), same identifiers
           |  concurrency   = $concurrency per client per race
           |  duration      = ${durationSecs}s
           |  K (idem keys) = $K
           |  userId        = $userId  (limit $userLimit, $perRequest per check, $maxAdmitted each)
           |""".stripMargin
      ) *>
      IO.race(
        progressTicker("invariantD", durationSecs, total, admittedAll, rejectedAll, errors, okLabel = "admitted", nokLabel = "rejected"),
        List(
          (0 until concurrency).toList.parTraverse_(idempotency(a, _)),
          (0 until concurrency).toList.parTraverse_(idempotency(b, _)),
          List.fill(concurrency)(a).parTraverse_(quota),
          List.fill(concurrency)(b).parTraverse_(quota),
          List.fill(math.max(1, concurrency / 2))(thief).parSequence_,
        ).parSequence_,
      ) *>
      // Every reservation A holds gets one more attempt after the race.
      IO(grantedToA.toArray(Array.empty[String]).toList).flatMap(_.traverse_(steal)) *>
      IO.defer {
        val (ca, cb)   = (a.created.get, b.created.get)
        val (aa, ab)   = (a.admitted.get, b.admitted.get)
        val conflicts  = a.conflicts.get + b.conflicts.get
        val (t, landed) = (thefts.get, theftsLanded.get)
        val e          = errors.get
        val rejected   = rejectedAll.get
        val summary =
          s"created A=$ca B=$cb (K=$K each), admitted A=$aa B=$ab (max $maxAdmitted each, ${aa + ab} together), stolen reconciles refused=${t - landed}/$t, conflicts=$conflicts, errors=$e"
        def because(verdict: Verdict, cause: String): InvariantResult =
          InvariantResult(verdict, s"$cause — $summary\n     Sample errors:\n${sampler.render}")
        val result =
          if landed > 0 then because(Verdict.Violation, "CROSS-TENANT RECONCILE: B changed A's reservation")
          else if ca > K || cb > K then because(Verdict.Violation, "DOUBLE CLAIM: a client was answered 'new' twice for a key")
          else if aa > maxAdmitted || ab > maxAdmitted then because(Verdict.Violation, "OVER-ADMISSION")
          else if conflicts > 0 then because(Verdict.Violation, "CONFLICT: a 409 for an identical request")
          // From here on a shortfall may be an answer that was lost.
          else if e > 0 then because(Verdict.Inconclusive, "errors")
          else if ca < K || cb < K then because(Verdict.Violation, "SHARED IDEMPOTENCY: each client must create its own K records")
          // The limit was reached, and the two clients reached it together.
          else if aa + ab <= maxAdmitted && rejected > 0 then because(Verdict.Violation, "SHARED QUOTA: together the clients were held to one limit")
          else if aa + ab <= maxAdmitted then because(Verdict.Inconclusive, "VACUOUS: together the clients never reached a limit")
          else if t == 0 then because(Verdict.Inconclusive, "VACUOUS: A was never granted a reservation to steal")
          else InvariantResult(Verdict.Pass, summary)
        IO.pure(result)
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
    val degraded  = new AtomicLong(0)
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
            res  <- sendRateLimitCheckDetailed(client, baseUrl, key)
            t1   <- IO.monotonic
            micros = (t1 - t0).toMicros
            _ <- IO {
              total.incrementAndGet()
              res.map(a => if a.degraded then None else Some(a.allowed)) match
                // An answer from degradation mode never reached the store, so
                // its latency is not the limiter's. It was recorded as an
                // honest rejection.
                case Right(None) =>
                  degraded.incrementAndGet()
                case Right(Some(true)) =>
                  successes.incrementAndGet()
                  // Only record honest server decisions in the histogram.
                  // Connection failures, 5xx, parse errors would otherwise
                  // skew the percentiles in either direction depending on
                  // where they fail, making p50/p99 meaningless.
                  histogram.recordValue(math.min(micros, 3_600_000_000L))
                case Right(Some(false)) =>
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
      val dg        = degraded.get
      val errorPct  = if t > 0 then (e.toDouble / t.toDouble) * 100.0 else 0.0
      val p50       = histogram.getValueAtPercentile(50.0) / 1000.0   // ms
      val p95       = histogram.getValueAtPercentile(95.0) / 1000.0
      val p99       = histogram.getValueAtPercentile(99.0) / 1000.0
      val p999      = histogram.getValueAtPercentile(99.9) / 1000.0
      val actualRps = t.toDouble / durationSecs.toDouble
      // One degraded answer means the store was not answering for part of the
      // run, so the percentiles describe an outage, not the limiter.
      val ok        = errorPct <= errorBudgetPct && dg == 0

      val summary = f"""latency results ($durationSecs s window, target $rps RPS):
                       |  total requests  = $t
                       |  actual RPS      = $actualRps%.1f  (target $rps)
                       |  successes (200) = $s
                       |  rejected  (429) = $r
                       |  degraded        = $dg  (answered by degradation mode; not in the histogram)
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
                             |FAIL: error rate $errorPct%.2f%% (budget $errorBudgetPct%.2f%%), degraded answers $dg (budget 0).
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
          // Any web app answers 200 on /health; only Gate answers with this body.
          val isGate = io.circe.parser.parse(body)
            .flatMap(_.hcursor.get[String]("status")).toOption.contains("healthy")
          if resp.status.code == 200 && isGate then Right(())
          else Left(s"HTTP ${resp.status.code}: ${body.take(200)}")
        }
      }
      .handleError(e => Left(Option(e.getMessage).getOrElse(e.getClass.getSimpleName)))

  /** A rate-limit answer: the decision, and whether degradation mode made it.
    * The service marks those with `X-Gate-Degraded: true`, on a 200 and a 429
    * alike.
    */
  case class RateLimitAnswer(allowed: Boolean, degraded: Boolean, serverEpochSec: Option[Long] = None)

  private val DegradedHeader = org.typelevel.ci.CIString("X-Gate-Degraded")

  def sendRateLimitCheck(
    client:  Client[IO],
    baseUrl: String,
    key:     String,
    apiKey:  String = LoadSim.defaultApiKey,
  ): IO[Either[String, Boolean]] =
    sendRateLimitCheckDetailed(client, baseUrl, key, apiKey).map(_.map(_.allowed))

  def sendRateLimitCheckDetailed(
    client:  Client[IO],
    baseUrl: String,
    key:     String,
    apiKey:  String = LoadSim.defaultApiKey,
  ): IO[Either[String, RateLimitAnswer]] =
    val body = Json.obj("key" := key, "cost" := 1)
    val req  = Request[IO](
      method  = Method.POST,
      uri     = Uri.unsafeFromString(s"$baseUrl/v1/ratelimit/check"),
      headers = Headers(
        "Content-Type"  -> "application/json",
        "Authorization" -> s"Bearer $apiKey",
      ),
    ).withEntity(body.noSpaces)

    client.run(req).use { resp =>
      resp.as[String].map { body =>
        if resp.status.code == 200 || resp.status.code == 429 then
          io.circe.parser.parse(body)
            .flatMap(_.hcursor.get[Boolean]("allowed"))
            .map(RateLimitAnswer(
              _,
              resp.headers.get(DegradedHeader).exists(_.head.value == "true"),
              resp.headers.get[org.http4s.headers.Date].map(_.date.epochSecond),
            ))
            .left.map(_.getMessage)
        else
          Left(s"HTTP ${resp.status.code}: $body")
      }
    }.handleError(e => Left(e.getMessage))

  def sendIdempotencyCheck(client: Client[IO], baseUrl: String, key: String, apiKey: String = LoadSim.defaultApiKey): IO[Either[String, String]] =
    val body = Json.obj("idempotencyKey" := key, "ttl" := 3600)
    val req  = Request[IO](
      method  = Method.POST,
      uri     = Uri.unsafeFromString(s"$baseUrl/v1/idempotency/check"),
      headers = Headers(
        "Content-Type"  -> "application/json",
        "Authorization" -> s"Bearer $apiKey",
      ),
    ).withEntity(body.noSpaces)

    client.run(req).use { resp =>
      resp.as[String].map { body =>
        // 200 = new/duplicate, 202 = in_progress; both carry a status field.
        if resp.status.code == 200 || resp.status.code == 202 then
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
  def sendQuotaCheck(client: Client[IO], baseUrl: String, userId: String, estimatedInputTokens: Long, apiKey: String = LoadSim.defaultApiKey): IO[Either[String, String]] =
    val body = Json.obj("userId" := userId, "estimatedInputTokens" := estimatedInputTokens)
    val req  = Request[IO](
      method  = Method.POST,
      uri     = Uri.unsafeFromString(s"$baseUrl/v1/quota/check"),
      headers = Headers(
        "Content-Type"  -> "application/json",
        "Authorization" -> s"Bearer $apiKey",
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

  /** Right((outcome, reservationId)); the ID is present when allowed. */
  def sendQuotaCheckReserving(client: Client[IO], baseUrl: String, userId: String, estimatedInputTokens: Long, apiKey: String): IO[Either[String, (String, Option[String])]] =
    val body = Json.obj("userId" := userId, "estimatedInputTokens" := estimatedInputTokens)
    val req  = Request[IO](
      method  = Method.POST,
      uri     = Uri.unsafeFromString(s"$baseUrl/v1/quota/check"),
      headers = Headers(
        "Content-Type"  -> "application/json",
        "Authorization" -> s"Bearer $apiKey",
      ),
    ).withEntity(body.noSpaces)

    client.run(req).use { resp =>
      resp.as[String].map { body =>
        resp.status.code match
          case 200 =>
            io.circe.parser.parse(body).flatMap(_.hcursor.get[String]("reservationId"))
              .left.map(e => s"200 without a reservationId: ${e.getMessage}")
              .map(id => ("allowed", Some(id)))
          case 429  => Right(("exceeded", None))
          case 503  => Right(("contended", None))
          case code => Left(s"HTTP $code: $body")
      }
    }.handleError(e => Left(e.getMessage))

  /** Right(status) for the answers reconcile gives; Left for anything else. */
  def sendQuotaReconcile(client: Client[IO], baseUrl: String, reservationId: String, actualInputTokens: Long, apiKey: String): IO[Either[String, Int]] =
    val body = Json.obj(
      "reservationId"      := reservationId,
      "actualInputTokens"  := actualInputTokens,
      "actualOutputTokens" := 0L,
    )
    val req = Request[IO](
      method  = Method.POST,
      uri     = Uri.unsafeFromString(s"$baseUrl/v1/quota/reconcile"),
      headers = Headers(
        "Content-Type"  -> "application/json",
        "Authorization" -> s"Bearer $apiKey",
      ),
    ).withEntity(body.noSpaces)

    client.run(req).use { resp =>
      resp.as[String].map { body =>
        resp.status.code match
          case code @ (200 | 404 | 409 | 503) => Right(code)
          case code                           => Left(s"HTTP $code: $body")
      }
    }.handleError(e => Left(e.getMessage))

// IO.loop helper — run an action forever until cancelled
extension [A](io: IO[A])
  def loop: IO[Nothing] = io.foreverM
