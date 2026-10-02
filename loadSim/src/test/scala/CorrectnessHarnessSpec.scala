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
