package resilience

import cats.effect.*
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

  def aggregate[F[_]: Temporal](
      sources: List[HealthSource[F]],
  ): F[AggregateHealth] = sources
    .traverse(s => s.check.map(_.copy(required = s.required)))
    .map { components =>
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

  def circuitBreakerSource[F[_]: Temporal](
      cb: Option[CircuitBreaker[F]],
  ): HealthSource[F] = new HealthSource[F]:
    val name = "circuitBreaker"
    def check: F[ComponentHealth] = cb match
      case Some(breaker) => breaker.state.map(s =>
          ComponentHealth(
            name,
            s.toString.toLowerCase,
            Some(Json.fromString(s.toString)),
          ),
        )
      case None => Temporal[F].pure(ComponentHealth(name, "disabled"))

  def cacheSource[F[_]: Temporal](
      cache: Option[LocalCache[F, ?, ?]],
  ): HealthSource[F] = new HealthSource[F]:
    val name = "cache"
    def check: F[ComponentHealth] = cache match
      case Some(c) => c.stats.map(s =>
          ComponentHealth(
            name,
            "ok",
            Some(Json.obj(
              "hitRate" -> Json.fromDoubleOrNull(s.hitRate),
              "size" -> Json.fromLong(s.estimatedSize),
            )),
          ),
        )
      case None => Temporal[F].pure(ComponentHealth(name, "disabled"))

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
