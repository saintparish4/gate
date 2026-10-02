import cats.effect.*
import cats.effect.testing.scalatest.AsyncIOSpec
import org.http4s.client.Client
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

import FakeGate.Fault
import Scenarios.{InvariantA, InvariantResult, Verdict}

/** The correctness scenario, pointed at servers that are wrong on purpose.
  *
  * Against a correct server every invariant must pass. Against a server with
  * one bug, the invariant that guards against it must report a violation, and
  * a server that only errors or degrades must be inconclusive, not a violation.
  * If an invariant here stops failing, the scenario has gone blind to that bug.
  */
class CorrectnessHarnessSpec extends AsyncFreeSpec with AsyncIOSpec with Matchers:

  private val url   = "http://fake.gate"
  private val keyA  = "client-a"
  private val keyB  = "client-b"
  private val quick = 2

  private def against(faults: Fault*)(run: Client[IO] => IO[InvariantResult]): IO[InvariantResult] =
    FakeGate.app(faults*).flatMap(app => run(Client.fromHttpApp(app)))

  private def a(faults: Fault*): IO[InvariantResult] = against(faults*)(client =>
    Scenarios.invariantA_tokenBucketNonOverIssue(client, url, "t", concurrency = 8, freeKey = keyB, durationSecs = quick),
  )
  private def b(faults: Fault*): IO[InvariantResult] = against(faults*)(client =>
    Scenarios.invariantB_idempotencyExactlyOneCreated(client, url, "t", concurrency = 8, apiKey = keyA, durationSecs = quick),
  )
  private def c(faults: Fault*): IO[InvariantResult] = against(faults*)(client =>
    Scenarios.invariantC_quotaNonOverAdmission(client, url, "t", concurrency = 8, apiKey = keyA, durationSecs = quick),
  )
  private def d(faults: Fault*): IO[InvariantResult] = against(faults*)(client =>
    Scenarios.invariantD_crossTenantIsolation(client, url, "t", concurrency = 4, keyA = keyA, keyB = keyB, durationSecs = quick),
  )

  /** The verdict and the cause it names, which is the text before the colon. */
  private def verdictOf(result: IO[InvariantResult]): IO[(Verdict, String)] =
    result.map(r => (r.verdict, r.details.takeWhile(_ != ':')))

  "against a correct server" - {
    "A passes" in a().asserting(_.verdict shouldBe Verdict.Pass)
    "B passes" in b().asserting(_.verdict shouldBe Verdict.Pass)
    "C passes" in c().asserting(_.verdict shouldBe Verdict.Pass)
    "D passes" in d().asserting(_.verdict shouldBe Verdict.Pass)
  }

  "A — token bucket never over-issues" - {
    "a limiter that admits everything is a violation" in
      verdictOf(a(Fault.AlwaysAllow)).asserting(_ shouldBe (Verdict.Violation, "OVER-ISSUE"))

    "a limiter that admits nothing is a violation" in
      verdictOf(a(Fault.NeverAllow)).asserting(_ shouldBe (Verdict.Violation, "UNDER-ISSUE"))

    "answers from degradation mode are inconclusive, even past the ceiling" in
      verdictOf(a(Fault.Degraded)).asserting(_ shouldBe (Verdict.Inconclusive, "DEGRADED"))

    "a server that only errors is inconclusive, not a violation" in
      a(Fault.AlwaysError).asserting(_.verdict shouldBe Verdict.Inconclusive)
  }

  "B — idempotency creates exactly once" - {
    "a server that says 'new' to everyone is a violation" in
      verdictOf(b(Fault.AlwaysNew)).asserting(_ shouldBe (Verdict.Violation, "DOUBLE CLAIM"))

    "a server that never says 'new' is a violation" in
      verdictOf(b(Fault.NeverNew)).asserting(_ shouldBe (Verdict.Violation, "NEVER CLAIMED"))

    "a server that only errors is inconclusive, not a violation" in
      b(Fault.AlwaysError).asserting(_.verdict shouldBe Verdict.Inconclusive)
  }

  "C — token quota never over-admits" - {
    "a quota with no limit is a violation" in
      verdictOf(c(Fault.NoQuotaLimit)).asserting(_ shouldBe (Verdict.Violation, "OVER-ADMISSION"))

    "a quota that never reaches its limit is inconclusive" in
      verdictOf(c(Fault.QuotaAlwaysContended)).asserting(_ shouldBe (Verdict.Inconclusive, "VACUOUS"))

    "a server that only errors is inconclusive, not a violation" in
      c(Fault.AlwaysError).asserting(_.verdict shouldBe Verdict.Inconclusive)
  }

  "D — tenants never share state" - {
    "idempotency records shared between clients are a violation" in
      verdictOf(d(Fault.SharedIdempotency)).asserting(_ shouldBe (Verdict.Violation, "SHARED IDEMPOTENCY"))

    "a quota counter shared between clients is a violation" in
      verdictOf(d(Fault.SharedQuota)).asserting(_ shouldBe (Verdict.Violation, "SHARED QUOTA"))

    "a reservation another client can reconcile is a violation" in
      verdictOf(d(Fault.ReconcileAnyone)).asserting(_ shouldBe (Verdict.Violation, "CROSS-TENANT RECONCILE"))

    "a server that only errors is inconclusive, not a violation" in
      d(Fault.AlwaysError).asserting(_.verdict shouldBe Verdict.Inconclusive)

    "one key for both clients is inconclusive: there is only one tenant" in against()(client =>
      Scenarios.invariantD_crossTenantIsolation(client, url, "t", 4, keyA, keyA, quick),
    ).asserting(_.verdict shouldBe Verdict.Inconclusive)
  }

  "the overall verdict" - {
    "is the worst of its parts" in IO {
      Verdict.overall(List(Verdict.Pass, Verdict.Pass)) shouldBe Verdict.Pass
      Verdict.overall(List(Verdict.Pass, Verdict.Inconclusive)) shouldBe Verdict.Inconclusive
      Verdict.overall(List(Verdict.Inconclusive, Verdict.Violation, Verdict.Pass)) shouldBe Verdict.Violation
    }

    "exits 0, 1 and 2" in IO {
      List(Verdict.Pass, Verdict.Violation, Verdict.Inconclusive).map(_.exitCode.code) shouldBe List(0, 1, 2)
    }
  }

  "the results file" - {
    // A whole run, shortened, against a fake, written where a temp folder is.
    def run(faults: Fault*): IO[(ExitCode, io.circe.Json)] =
      for
        dir    <- IO(java.nio.file.Files.createTempDirectory("loadsim-results"))
        app    <- FakeGate.app(faults*)
        client  = Client.fromHttpApp(app)
        // A fake that only errors has no /health to read; the run still happens.
        server <- Http.healthCheck(client, url).map(_.getOrElse(Http.ServerInfo.unknown))
        code   <- Scenarios.correctness(
          client,
          url,
          concurrency = Some(6),
          apiKey = keyA,
          freeKey = keyB,
          server = server,
          resultsDir = Some(dir.toString),
          durations = Scenarios.Durations(warmupStageSecs = 0, a = quick, b = quick, c = quick, d = quick),
        )
        files  <- IO(java.nio.file.Files.list(dir).toArray.toList.map(_.toString))
        text   <- IO(java.nio.file.Files.readString(java.nio.file.Paths.get(files.head)))
        json   <- IO.fromEither(io.circe.parser.parse(text))
      yield (code, json)

    "records a passing run: what was tested, each verdict, and the counts behind it" in
      run().asserting { case (code, json) =>
        val c = json.hcursor
        code shouldBe ExitCode.Success
        c.get[String]("overall") shouldBe Right("PASS")
        c.downField("server").get[String]("commit") shouldBe Right("fake123")
        c.downField("server").get[String]("version") shouldBe Right("0.0.0-fake")
        c.downField("loadSim").get[String]("commit").exists(_.nonEmpty) shouldBe true
        val invariants = c.downField("invariants").values.get.toList
        invariants.map(_.hcursor.get[String]("name").toOption.get) shouldBe List("A", "B", "C", "D")
        invariants.map(_.hcursor.get[String]("verdict").toOption.get).distinct shouldBe List("PASS")
        invariants.head.hcursor.downField("counts").get[Long]("allowed").exists(_ > 0) shouldBe true
        invariants.head.hcursor.downField("counts").get[Long]("maxAllowed").isRight shouldBe true
      }

    "records a violation with exit code 1" in
      run(Fault.AlwaysAllow).asserting { case (code, json) =>
        code.code shouldBe 1
        json.hcursor.get[String]("overall") shouldBe Right("VIOLATION")
      }

    "keeps errors apart from the counts, by class, and exits 2" in
      run(Fault.AlwaysError).asserting { case (code, json) =>
        code.code shouldBe 2
        json.hcursor.get[String]("overall") shouldBe Right("INCONCLUSIVE")
        val a = json.hcursor.downField("invariants").values.get.head.hcursor
        a.downField("counts").get[Long]("allowed") shouldBe Right(0L)
        a.downField("errors").values.get.head.hcursor.get[String]("class") shouldBe Right("http_5xx")
        a.downField("errors").values.get.head.hcursor.get[Long]("count").exists(_ > 0) shouldBe true
      }
  }

  "error classes" - {
    "tell a status from a timeout from a dropped connection" in IO {
      ErrorSampler.classify("HTTP 503: {\"error\":\"storage_unavailable\"}") shouldBe "http_5xx"
      ErrorSampler.classify("HTTP 429: {\"error\":\"rate_limited\"}") shouldBe "http_429"
      ErrorSampler.classify("HTTP 401: {}") shouldBe "http_4xx"
      ErrorSampler.classify("Request to fake.gate timed out after 30 seconds") shouldBe "timeout"
      ErrorSampler.classify("Connection refused") shouldBe "connection"
      ErrorSampler.classify("unexpected status: gone") shouldBe "unexpected_answer"
      ErrorSampler.classify("boom") shouldBe "other"
    }
  }

  "invariant A's ceiling" - {
    "uses the server's window when the answers were dated" in IO {
      // 30 s by the server; this machine's own clocks are not consulted.
      InvariantA.ceiling(20, 2, InvariantA.Window(29.0, 31.0, Some(30))) shouldBe
        Right(InvariantA.Ceiling(20 + 2 * 32, 32.0, "server"))
    }

    "falls back to this machine's clocks while they agree" in IO {
      InvariantA.ceiling(20, 2, InvariantA.Window(30.0, 30.1, None)).map(_.maxAllowed) shouldBe
        Right(20L + math.ceil(2 * 31.1).toLong)
    }

    "has no ceiling when the answers were undated and this machine's clocks disagree" in IO {
      // The laptop this was found on: 30.0 s monotonic, 31.0 s wall.
      InvariantA.ceiling(20, 2, InvariantA.Window(30.0, 31.0, None)).isLeft shouldBe true
    }

    "without dated answers, a run on agreeing clocks still passes" in
      // Which clocks agree depends on the machine, so only the verdict's kind
      // is asserted: a pass, or inconclusive for the clocks, never a violation.
      a(Fault.NoDate).asserting(_.verdict should not be Verdict.Violation)
  }
