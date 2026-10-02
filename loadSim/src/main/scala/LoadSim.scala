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
 *   --results-dir DIR             where the correctness scenario writes its results
 *                                 file (default loadsim-results)
 *   --concurrency N               workers for the correctness scenario; scales all
 *                                 three invariants (defaults A=20, B=50, C=50).
 *                                 Lower it to match a small target -- a deployment
 *                                 that cannot serve the load measures nothing.
 *
 * Keys come from the environment:
 *   API_KEY        key for every scenario, the warm-up, and invariants B and C
 *                  (default test-api-key)
 *   FREE_API_KEY   Free-tier key whose 20-token bucket invariant A drains, and
 *                  the second client of invariant D (default free-api-key)
 *
 * The defaults are the built-in development keys, which only docker-compose
 * serves. A deployed stack loads its keys from Secrets Manager; `source
 * .demo-keys.env` exports them under these names. They were flags, and a flag
 * is echoed by make and by sbt and shows in `ps`, so every run against a deploy
 * printed its keys. --api-key and --free-key still work, for a one-off.
 */
object LoadSim extends IOApp:

  val defaultBaseUrl = "http://localhost:8080"
  /** The public development keys, which only docker-compose serves. */
  val builtInApiKey  = "test-api-key"
  val builtInFreeKey = "free-api-key"

  val defaultApiKey  = sys.env.get("API_KEY").filter(_.nonEmpty).getOrElse(builtInApiKey)
  val defaultFreeKey = sys.env.get("FREE_API_KEY").filter(_.nonEmpty).getOrElse(builtInFreeKey)

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
    val resultsDir  = Args.string(args, "--results-dir", "loadsim-results")

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
      def withPreflight(action: Http.ServerInfo => IO[ExitCode]): IO[ExitCode] =
        Http.healthCheck(client, baseUrl).flatMap {
          case Right(server) =>
            Console[IO].println(s"Preflight OK: $baseUrl/health responded 200 (version ${server.version}, commit ${server.commit}).") *> action(server)
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
        case "normal"         => withPreflight(_ => Scenarios.normal(client, baseUrl).as(ExitCode.Success))
        case "burst"          => withPreflight(_ => Scenarios.burst(client, baseUrl).as(ExitCode.Success))
        case "idempotency"    => withPreflight(_ => Scenarios.idempotency(client, baseUrl).as(ExitCode.Success))
        case "realistic"      => withPreflight(_ => Scenarios.realistic(client, baseUrl).as(ExitCode.Success))
        case "highContention" => withPreflight(_ => Scenarios.highContention(client, baseUrl).as(ExitCode.Success))
        case "correctness"    => withPreflight(server => Scenarios.correctness(client, baseUrl, concurrency, apiKey, freeKey, server, Some(resultsDir)))
        case "latency"        => withPreflight(_ => Scenarios.latency(client, baseUrl, rps.getOrElse(1000), duration.getOrElse(60)))
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
  // Every error by class, with the first message seen for it. A results file
  // keeps these apart from the scores: a timeout is not a wrong answer.
  private val byClass      = new ConcurrentHashMap[String, AtomicLong]()
  private val firstOfClass = new ConcurrentHashMap[String, String]()

  def record(msg: String): Unit =
    count.incrementAndGet()
    val key = normalize(msg)
    val cls = ErrorSampler.classify(key)
    byClass.computeIfAbsent(cls, _ => new AtomicLong(0)).incrementAndGet()
    firstOfClass.putIfAbsent(cls, key)
    // putIfAbsent returns null when the key is new; only then append to order.
    if seen.size < maxDistinct && seen.putIfAbsent(key, java.lang.Boolean.TRUE) == null then
      order.offer(key)
    ()

  def total: Long = count.get

  /** (class, how many, first message), most frequent first. */
  def classes: List[(String, Long, String)] =
    val out = List.newBuilder[(String, Long, String)]
    byClass.forEach((cls, n) => out += ((cls, n.get, firstOfClass.getOrDefault(cls, ""))))
    out.result().sortBy(-_._2)

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

object ErrorSampler:
  /** What kind of failure a message describes. The Http helpers put the status
    * first ("HTTP 503: ..."); anything else is the client's own exception text.
    */
  def classify(msg: String): String =
    val m = msg.toLowerCase
    if m.startsWith("http 429") then "http_429"
    else if m.startsWith("http 5") then "http_5xx"
    else if m.startsWith("http 4") then "http_4xx"
    else if m.startsWith("unexpected") then "unexpected_answer"
    else if m.contains("timed out") || m.contains("timeout") then "timeout"
    else if m.contains("connect") || m.contains("closed") || m.contains("eof") || m.contains("reset") then "connection"
    else "other"

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
   *   N=50 parallel clients all check the same key, and the key changes every
   *   250 ms, so each run races about 120 first claims. Every key must be
   *   answered "new" exactly once. No HTTP errors, no 409 conflicts (same body).
   *   It used to hold 10 keys for the whole 30 s: ten races in the first
   *   instant, then duplicates. It also compared the total of "new" answers
   *   with the number of keys, which one key claimed twice and one never
   *   claimed would have satisfied.
   *
   * Invariant C — token-quota non-over-admission:
   *   N=50 parallel clients each ask for 25,000 tokens against a user whose
   *   limit is 1,000,000 (the default), so at most 40 checks may be admitted.
   *   Once a user has refused 50 checks the clients move to a fresh user, up to
   *   10 users, so the race at the limit is run more than once.
   *   Requires TOKEN_QUOTA_ENABLED=true on the server.
   *   Assert: for every user admitted * 25,000 <= 1,000,000; at least one user
   *   reached its limit; no HTTP errors.
   *
   * Invariant D — cross-tenant isolation (ADR-005):
   *   Two clients, API_KEY (A) and FREE_API_KEY (B), race for 20 s on the same
   *   visible identifiers. Each is its own tenant.
   *     D1: both check the same idempotency key, which changes every 250 ms;
   *         each must be answered "new" exactly once for every key. Shared keys
   *         would give one "new" between them.
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
    server:      ServerInfo = ServerInfo.unknown,
    resultsDir:  Option[String] = None,
    durations:   Durations = Durations(),
  ): IO[ExitCode] =
    val runId = System.currentTimeMillis.toString
    // One --concurrency scales all three invariants. Absent, each keeps its own
    // default: B and C need more contention than A to prove anything.
    for
      startedAt <- IO.realTimeInstant
      _  <- Console[IO].println(s"=== correctness — run $runId ===")
      _  <- warmUp(client, baseUrl, runId, apiKey, durations.warmupStageSecs)
      a  <- invariantA_tokenBucketNonOverIssue(client, baseUrl, runId, concurrency.getOrElse(20), freeKey, durations.a)
      b  <- invariantB_idempotencyExactlyOneCreated(client, baseUrl, runId, concurrency.getOrElse(50), apiKey, durations.b)
      c  <- invariantC_quotaNonOverAdmission(client, baseUrl, runId, concurrency.getOrElse(50), apiKey, durations.c)
      d  <- invariantD_crossTenantIsolation(client, baseUrl, runId, concurrency.getOrElse(10), apiKey, freeKey, durations.d)
      finishedAt <- IO.realTimeInstant
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
      _  <- resultsDir.traverse_(dir =>
              Results.write(
                dir, runId, baseUrl, server, startedAt, finishedAt, overall,
                List("A" -> a, "B" -> b, "C" -> c, "D" -> d),
              ).flatMap(path => Console[IO].println(s"Results written to $path"))
                // The verdict stands whether or not it could be saved.
                .handleErrorWith(err => Console[IO].errorln(s"Could not write the results file: ${err.getMessage}"))
            )
    yield overall.exitCode

  /** One file per run, so a figure quoted from a run can be checked later.
    *
    * The README cited run IDs whose output existed nowhere: the numbers lived
    * in a terminal, and `/health` did not say which commit had answered. The
    * file records what was tested (the server's version and commit, read from
    * the run itself), what tested it, each invariant's verdict and raw counts,
    * and the requests that errored, by class and apart from the counts.
    */
  object Results:
    private val titles = Map(
      "A" -> "token bucket never over-issues",
      "B" -> "idempotency creates exactly once",
      "C" -> "token quota never over-admits",
      "D" -> "tenants never share state",
    )

    def json(
      runId:      String,
      target:     String,
      server:     ServerInfo,
      startedAt:  java.time.Instant,
      finishedAt: java.time.Instant,
      overall:    Verdict,
      invariants: List[(String, InvariantResult)],
    ): Json = Json.obj(
      "scenario"   := "correctness",
      "runId"      := runId,
      "startedAt"  := startedAt.toString,
      "finishedAt" := finishedAt.toString,
      "target"     := target,
      "server"     := Json.obj("version" := server.version, "commit" := server.commit),
      "loadSim"    := Json.obj("commit" := loadSimCommit),
      "overall"    := overall.label,
      "invariants" := invariants.map { (name, r) =>
        Json.obj(
          "name"    := name,
          "title"   := titles.getOrElse(name, name),
          "verdict" := r.verdict.label,
          "details" := r.details,
          // In key order, so two runs' files diff cleanly.
          "counts"  := Json.fromFields(r.counts.toList.sortBy(_._1).map((k, v) => k -> Json.fromLong(v))),
          "errors"  := r.errors.map((cls, n, sample) => Json.obj("class" := cls, "count" := n, "sample" := sample)),
        )
      },
    )

    def write(
      dir:        String,
      runId:      String,
      target:     String,
      server:     ServerInfo,
      startedAt:  java.time.Instant,
      finishedAt: java.time.Instant,
      overall:    Verdict,
      invariants: List[(String, InvariantResult)],
    ): IO[java.nio.file.Path] = IO.blocking {
      val folder = java.nio.file.Paths.get(dir)
      java.nio.file.Files.createDirectories(folder)
      val path = folder.resolve(s"correctness-$runId.json")
      java.nio.file.Files.writeString(path, json(runId, target, server, startedAt, finishedAt, overall, invariants).spaces2 + "\n")
      path.toAbsolutePath
    }

    /** The commit of the checkout the simulator was run from. */
    private def loadSimCommit: String =
      scala.util.Try(scala.sys.process.Process(Seq("git", "describe", "--always", "--dirty")).!!(scala.sys.process.ProcessLogger(_ => ())).trim)
        .toOption.filter(_.nonEmpty).getOrElse("unknown")

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

  /** @param counts
    *   The raw numbers the verdict was computed from, for the results file
    * @param errors
    *   Requests that could not be answered, by class: (class, count, first message)
    */
  case class InvariantResult(
    verdict: Verdict,
    details: String,
    counts:  Map[String, Long] = Map.empty,
    errors:  List[(String, Long, String)] = Nil,
  )

  /** How long each part of the correctness scenario runs. The defaults are
    * what CI and the README's figures use; the tests shorten them.
    */
  final case class Durations(warmupStageSecs: Int = 5, a: Int = 30, b: Int = 30, c: Int = 20, d: Int = 20)

  /**
   * Ramps 1 -> 5 -> 20 workers over ~15 s on throwaway keys before any invariant
   * runs. A cold JVM answering a 50-way burst exceeds the server's 2 s check
   * timeout, which trips its circuit breaker and blacks out the whole window; the
   * invariants are about correctness, not cold-start latency, so I warm the hot
   * path first the way real traffic would.
   */
  private def warmUp(client: Client[IO], baseUrl: String, runId: String, apiKey: String, stageSecs: Int = 5): IO[Unit] =
    val stages = List(1, 5, 20).map(_ -> stageSecs).filter(_._2 > 0) // (workers, seconds)
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

  def invariantA_tokenBucketNonOverIssue(
    client:       Client[IO],
    baseUrl:      String,
    runId:        String,
    concurrency:  Int,
    freeKey:      String,
    durationSecs: Int = 30,
  ): IO[InvariantResult] =
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
      IO.pure(result.copy(
        counts = Map(
          "total" -> t, "allowed" -> a, "blocked" -> b, "degraded" -> d, "errors" -> e,
          "minAllowed" -> minAllowed.toLong,
          "clientMonotonicMillis" -> (window.monotonicSecs * 1000).toLong,
          "clientWallMillis" -> (window.wallSecs * 1000).toLong,
        ) ++ ceiling.toOption.map(c => "maxAllowed" -> c.maxAllowed) ++ window.serverSecs.map("serverWindowSecs" -> _),
        errors = sampler.classes,
      ))

    Console[IO].println(
      s"""-- Invariant A — token-bucket non-over-issue --
         |  key           = $key
         |  apiKey        = ${if apiKey == LoadSim.builtInFreeKey then apiKey else "FREE_API_KEY (not printed)"} (Free tier)
         |  concurrency   = $concurrency
         |  duration      = ${durationSecs}s
         |  capacity      = $capacity
         |  refillPerSec  = $refillPerSec
         |  maxAllowed    = capacity + refill*(window + allowance)  (the window is measured, by the server's clock; reported below)
         |  minAllowed    = $minAllowed  (a quarter of the nominal budget; catches a limiter that rejects everything)
         |""".stripMargin
    ) *>
    drive.flatMap(verdict)

  /** How long one idempotency key is raced for before the next takes over. */
  private val ClaimRoundMillis = 250

  /** `new` answers per key, and every key that was answered at all. */
  final class ClaimTally:
    private val news = new ConcurrentHashMap[String, AtomicInteger]()

    /** An answer for `key` arrived; `isNew` when it was "new". */
    def record(key: String, isNew: Boolean): Unit =
      val count = news.computeIfAbsent(key, _ => new AtomicInteger(0))
      if isNew then count.incrementAndGet()
      ()

    def keys: Int = news.size
    def created: Long =
      var sum = 0L
      news.values.forEach(c => sum += c.get)
      sum

    /** Keys answered "new" more than once, and keys never answered "new". */
    def doubleClaimed: List[String] = matching(_ > 1)
    def neverClaimed: List[String]  = matching(_ == 0)

    private def matching(p: Int => Boolean): List[String] =
      val out = List.newBuilder[String]
      news.forEach((k, c) => if p(c.get) then out += k)
      out.result().sorted

  /** Run `step` again and again until the monotonic clock passes `deadline`.
    * A request in flight at the deadline is answered and counted, where
    * cancelling the workers would have dropped its answer: with one "new" per
    * key, a dropped "new" reads as a key nobody claimed.
    */
  private def untilDeadline(deadline: FiniteDuration)(step: IO[Unit]): IO[Unit] =
    IO.monotonic.flatMap(now => if now >= deadline then IO.unit else step >> untilDeadline(deadline)(step))

  /** The key being raced at this instant. */
  private def claimKey(prefix: String, startedAt: FiniteDuration): IO[String] =
    IO.monotonic.map(now => s"$prefix:r${(now - startedAt).toMillis / ClaimRoundMillis}")

  def invariantB_idempotencyExactlyOneCreated(
    client:       Client[IO],
    baseUrl:      String,
    runId:        String,
    concurrency:  Int,
    apiKey:       String,
    durationSecs: Int = 30,
  ): IO[InvariantResult] =
    val prefix    = s"correctness:B:$runId"
    val tally     = new ClaimTally
    val total     = new AtomicLong(0)
    val created   = new AtomicLong(0)
    val duplicate = new AtomicLong(0)
    val conflict  = new AtomicLong(0)
    val errors    = new AtomicLong(0)
    val sampler   = new ErrorSampler(5)

    def worker(startedAt: FiniteDuration): IO[Unit] =
      untilDeadline(startedAt + durationSecs.seconds) {
        claimKey(prefix, startedAt).flatMap { key =>
          sendIdempotencyCheck(client, baseUrl, key, apiKey).flatMap {
            case Right("new")                       => IO { total.incrementAndGet(); created.incrementAndGet(); tally.record(key, isNew = true) }
            case Right("in_progress" | "duplicate") => IO { total.incrementAndGet(); duplicate.incrementAndGet(); tally.record(key, isNew = false) }
            case Right("conflict")                  => IO { total.incrementAndGet(); conflict.incrementAndGet() }
            case Right(other)                       => IO { total.incrementAndGet(); errors.incrementAndGet(); sampler.record(s"unexpected status: $other") }
            case Left(msg)                          => IO { total.incrementAndGet(); errors.incrementAndGet(); sampler.record(msg) }
          }.void
        }
      }

    Console[IO].println(
      s"""-- Invariant B — idempotency exactly-one-Created --
         |  keys            = one at a time, a new one every ${ClaimRoundMillis} ms
         |  concurrency     = $concurrency (all on the current key)
         |  duration        = ${durationSecs}s
         |""".stripMargin
    ) *>
    IO.monotonic.flatMap { startedAt =>
      (
        progressTicker("invariantB", durationSecs, total, created, duplicate, errors, okLabel = "created", nokLabel = "duplicate"),
        (0 until concurrency).toList.parTraverse_(_ => worker(startedAt)),
      ).parTupled
    } *> IO.defer {
      val e          = errors.get
      val cf         = conflict.get
      val doubles    = tally.doubleClaimed
      val unclaimed  = tally.neverClaimed
      val counts     = s"keys=${tally.keys}, created=${tally.created}, conflicts=$cf, errors=$e, duplicates=${duplicate.get}"
      val errSection = if e > 0 then s"\n     Sample errors:\n${sampler.render}" else ""
      def some(keys: List[String]): String = keys.take(3).mkString(", ")
      val result =
        if doubles.nonEmpty then InvariantResult(Verdict.Violation, s"DOUBLE CLAIM: ${doubles.size} key(s) were answered 'new' more than once (${some(doubles)}) — $counts$errSection")
        else if cf > 0 then InvariantResult(Verdict.Violation, s"CONFLICT: a 409 for an identical request — $counts$errSection")
        // A key with no 'new' may be one whose 'new' answer was lost to an error.
        else if e > 0 then InvariantResult(Verdict.Inconclusive, s"$counts$errSection")
        else if unclaimed.nonEmpty then InvariantResult(Verdict.Violation, s"NEVER CLAIMED: ${unclaimed.size} key(s) were answered, but never 'new' (${some(unclaimed)}) — $counts")
        else if tally.keys < MinClaimRaces then InvariantResult(Verdict.Inconclusive, s"VACUOUS: only ${tally.keys} key(s) were raced (need $MinClaimRaces) — the stack is too slow to show anything — $counts")
        else InvariantResult(Verdict.Pass, s"each of ${tally.keys} keys was answered 'new' exactly once — $counts")
      IO.pure(result.copy(
        counts = Map(
          "total" -> total.get, "keys" -> tally.keys.toLong, "created" -> tally.created,
          "doubleClaimed" -> doubles.size.toLong, "neverClaimed" -> unclaimed.size.toLong,
          "duplicates" -> duplicate.get, "conflicts" -> cf, "errors" -> e,
        ),
        errors = sampler.classes,
      ))
    }

  /** Fewer first-claim races than this in a run shows too little to pass on. */
  private val MinClaimRaces = 5

  def invariantC_quotaNonOverAdmission(
    client:       Client[IO],
    baseUrl:      String,
    runId:        String,
    concurrency:  Int,
    apiKey:       String,
    durationSecs: Int = 20,
  ): IO[InvariantResult] =
    val userLimit    = 1_000_000L   // TOKEN_QUOTA_USER_LIMIT default
    val perRequest   = 25_000L      // 40 admissions fill the window exactly
    val maxAdmitted  = userLimit / perRequest
    // The race this invariant is about happens once per user, as it crosses its
    // limit. One user gave one race per run; the clients now move on to a
    // fresh user once the current one has plainly reached its limit.
    val maxUsers     = 10
    val moveOnAfter  = 50L          // refusals from a user before leaving it
    def userId(i: Int) = s"correctness:C:$runId:u$i"

    val current   = new AtomicInteger(0)
    val admitted  = Vector.fill(maxUsers)(new AtomicLong(0))
    val rejected  = Vector.fill(maxUsers)(new AtomicLong(0))
    val total     = new AtomicLong(0)
    val admittedAll = new AtomicLong(0)
    val rejectedAll = new AtomicLong(0)
    val contended = new AtomicLong(0)
    val errors    = new AtomicLong(0)
    val sampler   = new ErrorSampler(5)

    def worker(deadline: FiniteDuration): IO[Unit] =
      untilDeadline(deadline) {
        IO(current.get).flatMap { u =>
          sendQuotaCheck(client, baseUrl, userId(u), perRequest, apiKey).flatMap {
            case Right("allowed")   => IO { total.incrementAndGet(); admittedAll.incrementAndGet(); admitted(u).incrementAndGet() }
            case Right("exceeded")  => IO {
                total.incrementAndGet(); rejectedAll.incrementAndGet()
                if rejected(u).incrementAndGet() >= moveOnAfter && u + 1 < maxUsers then current.compareAndSet(u, u + 1)
              }
            case Right("contended") => IO { total.incrementAndGet(); contended.incrementAndGet() }
            case Right(other)       => IO { total.incrementAndGet(); errors.incrementAndGet(); sampler.record(s"unexpected outcome: $other") }
            case Left(msg)          => IO { total.incrementAndGet(); errors.incrementAndGet(); sampler.record(msg) }
          }.void
        }
      }

    Console[IO].println(
      s"""-- Invariant C — token-quota non-over-admission --
         |  users         = up to $maxUsers, one at a time (${userId(0)} ...)
         |  concurrency   = $concurrency (all on the current user)
         |  duration      = ${durationSecs}s
         |  userLimit     = $userLimit
         |  perRequest    = $perRequest
         |  maxAdmitted   = $maxAdmitted per user
         |""".stripMargin
    ) *>
    IO.monotonic.flatMap { startedAt =>
      (
        progressTicker("invariantC", durationSecs, total, admittedAll, rejectedAll, errors, okLabel = "admitted", nokLabel = "rejected"),
        (0 until concurrency).toList.parTraverse_(_ => worker(startedAt + durationSecs.seconds)),
      ).parTupled
    } *> IO.defer {
      val perUser = (0 until maxUsers).map(u => (u, admitted(u).get, rejected(u).get)).filter((_, adm, rej) => adm + rej > 0)
      val over    = perUser.filter((_, adm, _) => adm * perRequest > userLimit)
      val filled  = perUser.count((_, _, rej) => rej > 0)
      val con     = contended.get
      val e       = errors.get
      val counts  = s"users filled=$filled of ${perUser.size} used, admitted per user=${perUser.map(_._2).mkString("/")} (max $maxAdmitted), rejected=${rejectedAll.get}, contended=$con, errors=$e"
      val result =
        if over.nonEmpty then InvariantResult(Verdict.Violation, s"OVER-ADMISSION: ${over.map((u, adm, _) => s"${userId(u)} admitted $adm (${adm * perRequest} tokens)").mkString("; ")} > userLimit=$userLimit — $counts")
        else if e > 0 then InvariantResult(Verdict.Inconclusive, s"$counts\n     Sample errors:\n${sampler.render}")
        else if filled == 0 then InvariantResult(Verdict.Inconclusive, s"VACUOUS: no user reached its limit — quotas disabled or stack too slow — $counts")
        else InvariantResult(Verdict.Pass, s"no user admitted past $userLimit tokens — $counts")
      IO.pure(result.copy(
        counts = Map(
          "total" -> total.get, "usersUsed" -> perUser.size.toLong, "usersFilled" -> filled.toLong,
          "usersOverLimit" -> over.size.toLong, "admitted" -> admittedAll.get, "rejected" -> rejectedAll.get,
          "contended" -> con, "errors" -> e,
        ),
        errors = sampler.classes,
      ))
    }

  def invariantD_crossTenantIsolation(
    client:       Client[IO],
    baseUrl:      String,
    runId:        String,
    concurrency:  Int,
    keyA:         String,
    keyB:         String,
    durationSecs: Int = 20,
  ): IO[InvariantResult] =
    val idemPrefix   = s"correctness:D:$runId"
    val userId       = s"correctness:D:$runId"
    val userLimit    = 1_000_000L   // TOKEN_QUOTA_USER_LIMIT default
    val perRequest   = 25_000L
    val maxAdmitted  = userLimit / perRequest

    final class Tenant(val key: String):
      val claims    = new ClaimTally
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

    def idempotency(t: Tenant, startedAt: FiniteDuration): IO[Unit] =
      claimKey(idemPrefix, startedAt).flatMap { key =>
        sendIdempotencyCheck(client, baseUrl, key, t.key).flatMap {
          case Right("new")                       => IO { total.incrementAndGet(); t.claims.record(key, isNew = true) }
          case Right("in_progress" | "duplicate") => IO { total.incrementAndGet(); t.claims.record(key, isNew = false) }
          case Right("conflict")                  => IO { total.incrementAndGet(); t.conflicts.incrementAndGet() }.void
          case Right(other)                       => fail(s"unexpected idempotency status: $other")
          case Left(msg)                          => fail(msg)
        }
      }

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
      }

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
    }

    if keyA == keyB then
      IO.pure(InvariantResult(Verdict.Inconclusive, "D needs two different keys: API_KEY and FREE_API_KEY are the same, so there is only one tenant"))
    else
      Console[IO].println(
        s"""-- Invariant D — cross-tenant isolation --
           |  clients       = A (API_KEY) and B (FREE_API_KEY), same identifiers
           |  concurrency   = $concurrency per client per race
           |  duration      = ${durationSecs}s
           |  idem keys     = one at a time, a new one every ${ClaimRoundMillis} ms, the same for both clients
           |  userId        = $userId  (limit $userLimit, $perRequest per check, $maxAdmitted each)
           |""".stripMargin
      ) *>
      IO.monotonic.flatMap { startedAt =>
        val deadline = startedAt + durationSecs.seconds
        def workers(n: Int)(step: IO[Unit]): IO[Unit] = List.fill(n)(untilDeadline(deadline)(step)).parSequence_
        (
          progressTicker("invariantD", durationSecs, total, admittedAll, rejectedAll, errors, okLabel = "admitted", nokLabel = "rejected"),
          List(
            workers(concurrency)(idempotency(a, startedAt)),
            workers(concurrency)(idempotency(b, startedAt)),
            workers(concurrency)(quota(a)),
            workers(concurrency)(quota(b)),
            workers(math.max(1, concurrency / 2))(thief),
          ).parSequence_,
        ).parTupled
      } *>
      // Every reservation A holds gets one more attempt after the race.
      IO(grantedToA.toArray(Array.empty[String]).toList).flatMap(_.traverse_(steal)) *>
      IO.defer {
        val (aa, ab)   = (a.admitted.get, b.admitted.get)
        val conflicts  = a.conflicts.get + b.conflicts.get
        val (t, landed) = (thefts.get, theftsLanded.get)
        val e          = errors.get
        val rejected   = rejectedAll.get
        val doubles    = a.claims.doubleClaimed.size + b.claims.doubleClaimed.size
        val unclaimed  = a.claims.neverClaimed.size + b.claims.neverClaimed.size
        val summary =
          s"idempotency keys A=${a.claims.keys} B=${b.claims.keys} (created A=${a.claims.created} B=${b.claims.created}), admitted A=$aa B=$ab (max $maxAdmitted each, ${aa + ab} together), stolen reconciles refused=${t - landed}/$t, conflicts=$conflicts, errors=$e"
        def because(verdict: Verdict, cause: String): InvariantResult =
          InvariantResult(verdict, s"$cause — $summary\n     Sample errors:\n${sampler.render}")
        val result =
          if landed > 0 then because(Verdict.Violation, "CROSS-TENANT RECONCILE: B changed A's reservation")
          else if doubles > 0 then because(Verdict.Violation, s"DOUBLE CLAIM: a client was answered 'new' twice for $doubles key(s)")
          else if aa > maxAdmitted || ab > maxAdmitted then because(Verdict.Violation, "OVER-ADMISSION")
          else if conflicts > 0 then because(Verdict.Violation, "CONFLICT: a 409 for an identical request")
          // From here on a shortfall may be an answer that was lost.
          else if e > 0 then because(Verdict.Inconclusive, "errors")
          // A client that was answered for a key, and never 'new', met the
          // other client's record.
          else if unclaimed > 0 then because(Verdict.Violation, s"SHARED IDEMPOTENCY: $unclaimed key(s) were never 'new' for one of the clients, so the two share records")
          // The limit was reached, and the two clients reached it together.
          else if aa + ab <= maxAdmitted && rejected > 0 then because(Verdict.Violation, "SHARED QUOTA: together the clients were held to one limit")
          else if aa + ab <= maxAdmitted then because(Verdict.Inconclusive, "VACUOUS: together the clients never reached a limit")
          else if t == 0 then because(Verdict.Inconclusive, "VACUOUS: A was never granted a reservation to steal")
          else if a.claims.keys < MinClaimRaces || b.claims.keys < MinClaimRaces then because(Verdict.Inconclusive, s"VACUOUS: fewer than $MinClaimRaces idempotency keys were raced")
          else InvariantResult(Verdict.Pass, summary)
        IO.pure(result.copy(
          counts = Map(
            "total" -> total.get, "keysA" -> a.claims.keys.toLong, "keysB" -> b.claims.keys.toLong,
            "doubleClaimed" -> doubles.toLong, "neverClaimed" -> unclaimed.toLong,
            "admittedA" -> aa, "admittedB" -> ab, "rejected" -> rejected,
            "thefts" -> t, "theftsLanded" -> landed, "conflicts" -> conflicts, "errors" -> e,
          ),
          errors = sampler.classes,
        ))
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
        // Never past the end: a 3 s run used to take 5.
        val step = math.min(ProgressIntervalSecs, duration - elapsed)
        IO.sleep(step.seconds) *>
          IO.defer {
            val e   = elapsed + step
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

  /** What `/health` says is running: the build's version and commit. "unknown"
    * for a server too old to report its commit.
    */
  case class ServerInfo(version: String, commit: String)
  object ServerInfo:
    val unknown: ServerInfo = ServerInfo("unknown", "unknown")

  /**
   * Preflight liveness probe. Returns what is running iff /health returns 200.
   * Every load scenario calls this before doing real work so that a dead or
   * misconfigured service fails loudly up front instead of producing a
   * histogram full of "connection refused" measurements.
   */
  def healthCheck(client: Client[IO], baseUrl: String): IO[Either[String, ServerInfo]] =
    val req = Request[IO](
      method = Method.GET,
      uri    = Uri.unsafeFromString(s"$baseUrl/health"),
    )
    client
      .run(req)
      .use { resp =>
        resp.as[String].map { body =>
          // Any web app answers 200 on /health; only Gate answers with this body.
          val health = io.circe.parser.parse(body).toOption.map(_.hcursor)
          val isGate = health.flatMap(_.get[String]("status").toOption).contains("healthy")
          def field(name: String) = health.flatMap(_.get[String](name).toOption).getOrElse("unknown")
          if resp.status.code == 200 && isGate then Right(ServerInfo(field("version"), field("commit")))
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
