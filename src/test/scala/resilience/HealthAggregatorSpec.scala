package resilience

import scala.concurrent.duration.*

import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.testkit.TestControl
import io.circe.syntax.*

/** /ready used to fail on any component, Kinesis included, and the ALB routes
  * on /ready, so one Kinesis fault took every task out of service although the
  * request path never waits on Kinesis (ADR-003). Only a component needed to
  * serve decisions may do that now.
  */
class HealthAggregatorSpec extends AsyncFreeSpec with AsyncIOSpec with Matchers:

  private def dynamo(name: String, up: Boolean) = HealthAggregator
    .dynamoDbSource[IO](name, IO.pure(if up then Right(()) else Left("down")))

  private def kinesis(up: Boolean) = HealthAggregator
    .kinesisSource[IO](IO.pure(if up then Right(()) else Left("stream gone")))

  private val probeTimeout = 3.seconds

  "everything reachable is ok and serving" in HealthAggregator.aggregate(
    List(dynamo("dynamodb_ratelimit", up = true), kinesis(up = true)),
    probeTimeout,
  ).asserting { h =>
    h.status shouldBe "ok"
    h.isServing shouldBe true
  }

  "a Kinesis fault is degraded but still serving" in HealthAggregator.aggregate(
    List(dynamo("dynamodb_ratelimit", up = true), kinesis(up = false)),
    probeTimeout,
  ).asserting { h =>
    h.status shouldBe "degraded"
    h.isServing shouldBe true
    h.components.find(_.name == "kinesis").map(_.required) shouldBe Some(false)
  }

  "a required table down takes the service out, Kinesis or not" in
    HealthAggregator.aggregate(
      List(
        dynamo("dynamodb_ratelimit", up = true),
        dynamo("dynamodb_quota", up = false),
        kinesis(up = true),
      ),
      probeTimeout,
    ).asserting { h =>
      h.status shouldBe "unavailable"
      h.isServing shouldBe false
    }

  "the body says which components are required" in
    HealthAggregator.aggregate(List(kinesis(up = false)), probeTimeout)
      .asserting(h =>
        h.asJson.noSpaces should
          include("""{"name":"kinesis","status":"error","required":false"""),
      )

  // The ALB's check times out after 5 s. The probes were sequential and bounded
  // only by the SDK's 10 s timeout, so a slow table got no answer at all.
  "hanging sources answer unavailable within one probe timeout, not one each" in
    TestControl.executeEmbed(
      for
        start <- IO.monotonic
        hang = HealthAggregator
          .dynamoDbSource[IO]("dynamodb_ratelimit", IO.never)
        hangToo = HealthAggregator
          .dynamoDbSource[IO]("dynamodb_idempotency", IO.never)
        health <- HealthAggregator
          .aggregate(List(hang, hangToo, kinesis(up = true)), probeTimeout)
        end <- IO.monotonic
      yield (health, end - start),
    ).asserting { case (health, elapsed) =>
      health.status shouldBe "unavailable"
      health.components.map(c => c.name -> c.status) shouldBe List(
        "dynamodb_ratelimit" -> "error",
        "dynamodb_idempotency" -> "error",
        "kinesis" -> "ok",
      )
      elapsed shouldBe probeTimeout
    }
