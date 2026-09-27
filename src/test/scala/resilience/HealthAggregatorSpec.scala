package resilience

import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
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

  "everything reachable is ok and serving" in HealthAggregator.aggregate(
    List(dynamo("dynamodb_ratelimit", up = true), kinesis(up = true)),
  ).asserting { h =>
    h.status shouldBe "ok"
    h.isServing shouldBe true
  }

  "a Kinesis fault is degraded but still serving" in HealthAggregator.aggregate(
    List(dynamo("dynamodb_ratelimit", up = true), kinesis(up = false)),
  ).asserting { h =>
    h.status shouldBe "degraded"
    h.isServing shouldBe true
    h.components.find(_.name == "kinesis").map(_.required) shouldBe Some(false)
  }

  "a required table down takes the service out, Kinesis or not" in
    HealthAggregator.aggregate(List(
      dynamo("dynamodb_ratelimit", up = true),
      dynamo("dynamodb_quota", up = false),
      kinesis(up = true),
    )).asserting { h =>
      h.status shouldBe "unavailable"
      h.isServing shouldBe false
    }

  "the body says which components are required" in
    HealthAggregator.aggregate(List(kinesis(up = false))).asserting(h =>
      h.asJson.noSpaces should
        include("""{"name":"kinesis","status":"error","required":false"""),
    )
