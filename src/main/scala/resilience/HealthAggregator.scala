package resilience

import scala.concurrent.duration.FiniteDuration

import cats.effect.*
import cats.effect.syntax.all.*
import cats.syntax.all.*
import io.circe.*
import io.circe.syntax.*

case class ComponentHealth(
    name: String,
    status: String,
    details: Option[Json] = None,
    required: Boolean = true,
)

/** `status` is "ok" when every component is, "degraded" when only optional ones
  * fail, and "unavailable" when a required one does. Only "unavailable" takes
  * the service out of rotation.
  */
case class AggregateHealth(status: String, components: List[ComponentHealth]):
  def isServing: Boolean = status != "unavailable"

object AggregateHealth:
  given Encoder[ComponentHealth] = Encoder
    .forProduct4("name", "status", "required", "details")(c =>
      (c.name, c.status, c.required, c.details),
    )
  given Encoder[AggregateHealth] = Encoder
    .forProduct2("status", "components")(a => (a.status, a.components))

trait HealthSource[F[_]]:
  def name: String
  def check: F[ComponentHealth]

  /** Whether the service can serve decisions without this component. Kinesis is
    * not required: events are fire-and-forget (ADR-003) and the request path
    * never waits on them. /ready used to fail on Kinesis too, and the ALB
    * routes on /ready, so one Kinesis fault took every task out of service.
    */
  def required: Boolean = true

object HealthAggregator:

  /** Probes every source at once, each bounded by `probeTimeout`; a source that
    * does not answer in time is an error. The probes were sequential and
    * bounded only by the SDK's 10 s request timeout, longer than the ALB's 5 s
    * health check, so a slow DynamoDB failed health checks with no answer at
    * all instead of a 503 naming the table.
    */
  def aggregate[F[_]: Temporal](
      sources: List[HealthSource[F]],
      probeTimeout: FiniteDuration,
  ): F[AggregateHealth] = sources.parTraverseN(math.max(1, sources.size))(s =>
    s.check.timeoutTo(
      probeTimeout,
      Temporal[F].pure(ComponentHealth(
        s.name,
        "error",
        Some(Json.fromString(s"no answer within $probeTimeout")),
      )),
    ).map(_.copy(required = s.required)),
  ).map { components =>
    val failing = components.filterNot(_.status == "ok")
    val status =
      if failing.isEmpty then "ok"
      else if failing.exists(_.required) then "unavailable"
      else "degraded"
    AggregateHealth(status, components)
  }

  def dynamoDbSource[F[_]: Temporal](
      name: String,
      healthCheck: F[Either[String, Unit]],
  ): HealthSource[F] =
    val sourceName = name
    new HealthSource[F]:
      def name: String = sourceName
      def check: F[ComponentHealth] = healthCheck
        .handleError(e => Left(e.getMessage)).map {
          case Right(_) => ComponentHealth(sourceName, "ok")
          case Left(err) =>
            ComponentHealth(sourceName, "error", Some(Json.fromString(err)))
        }

  def kinesisSource[F[_]: Temporal](
      healthCheck: F[Either[String, Unit]],
  ): HealthSource[F] = new HealthSource[F]:
    val name = "kinesis"
    override val required = false
    def check: F[ComponentHealth] = healthCheck
      .handleError(e => Left(e.getMessage)).map {
        case Right(_) => ComponentHealth(name, "ok")
        case Left(err) =>
          ComponentHealth(name, "error", Some(Json.fromString(err)))
      }
