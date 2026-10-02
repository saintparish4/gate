import java.util.UUID

import cats.effect.*
import cats.syntax.all.*
import org.http4s.*
import org.http4s.dsl.io.*
import org.http4s.headers.{Authorization, Date}
import org.typelevel.ci.CIString

import io.circe.Json
import io.circe.parser.parse

/** A stand-in for the service, small enough to be obviously right, with
  * switches that make it wrong in one way at a time.
  *
  * The correctness scenario had only ever been pointed at the real service. It
  * had failed on errors, and never on a broken invariant, so nothing showed
  * that it could. Each `Fault` is a bug the scenario exists to catch.
  */
object FakeGate:

  enum Fault:
    /** Every rate-limit check is admitted. */
    case AlwaysAllow
    /** No rate-limit check is admitted. */
    case NeverAllow
    /** Every idempotency check is answered "new". */
    case AlwaysNew
    /** No idempotency check is answered "new". */
    case NeverNew
    /** Quota checks are admitted without limit. */
    case NoQuotaLimit
    /** Every quota check answers 503. */
    case QuotaAlwaysContended
    /** Idempotency records are shared by every client. */
    case SharedIdempotency
    /** Quota counters are shared by every client. */
    case SharedQuota
    /** Any client may reconcile any reservation. */
    case ReconcileAnyone
    /** Every request answers 500. */
    case AlwaysError
    /** Rate-limit answers are marked as coming from degradation mode. */
    case Degraded
    /** Answers carry no Date header. */
    case NoDate

  private val Capacity   = 20.0
  private val RefillRate = 2.0
  private val UserLimit  = 1_000_000L

  private final case class Bucket(tokens: Double, lastMs: Long)

  private final case class State(
      buckets: Map[(String, String), Bucket] = Map.empty,
      claimed: Set[(String, String)] = Set.empty,
      used: Map[(String, String), Long] = Map.empty,
      reservations: Map[String, String] = Map.empty,
  )

  def app(faults: Fault*): IO[HttpApp[IO]] = Ref.of[IO, State](State()).map { state =>
    val on = faults.toSet

    def tenant(request: Request[IO]): String = request.headers.get[Authorization]
      .map(_.credentials.renderString).getOrElse("anonymous")

    def field(request: Request[IO], name: String): IO[Json] = request.bodyText.compile.string
      .map(parse(_).toOption.flatMap(_.hcursor.downField(name).focus).getOrElse(Json.Null))

    def json(status: Status, fields: (String, Json)*): Response[IO] =
      Response[IO](status).withEntity(Json.obj(fields*).noSpaces)

    def rateLimit(request: Request[IO]): IO[Response[IO]] =
      for
        key <- field(request, "key").map(_.asString.getOrElse(""))
        now <- IO.realTime.map(_.toMillis)
        allowed <-
          if on(Fault.AlwaysAllow) || on(Fault.Degraded) then IO.pure(true)
          else if on(Fault.NeverAllow) then IO.pure(false)
          else
            state.modify { s =>
              val id      = (tenant(request), key)
              val bucket  = s.buckets.getOrElse(id, Bucket(Capacity, now))
              val tokens  = math.min(Capacity, bucket.tokens + (now - bucket.lastMs) / 1000.0 * RefillRate)
              if tokens >= 1 then (s.copy(buckets = s.buckets.updated(id, Bucket(tokens - 1, now))), true)
              else (s, false)
            }
      yield
        val answer = json(if allowed then Status.Ok else Status.TooManyRequests, "allowed" -> Json.fromBoolean(allowed))
        if on(Fault.Degraded) then answer.putHeaders(Header.Raw(CIString("X-Gate-Degraded"), "true")) else answer

    def idempotency(request: Request[IO]): IO[Response[IO]] =
      field(request, "idempotencyKey").map(_.asString.getOrElse("")).flatMap { key =>
        val id = (if on(Fault.SharedIdempotency) then "everyone" else tenant(request), key)
        state.modify { s =>
          if on(Fault.AlwaysNew) then (s, true)
          else if on(Fault.NeverNew) || s.claimed(id) then (s, false)
          else (s.copy(claimed = s.claimed + id), true)
        }.map(isNew =>
          if isNew then json(Status.Ok, "status" -> Json.fromString("new"))
          else json(Status.Accepted, "status" -> Json.fromString("in_progress")),
        )
      }

    def quotaCheck(request: Request[IO]): IO[Response[IO]] =
      for
        body <- request.bodyText.compile.string.map(parse(_).getOrElse(Json.Null))
        user    = body.hcursor.get[String]("userId").getOrElse("")
        tokens  = body.hcursor.get[Long]("estimatedInputTokens").getOrElse(0L)
        owner   = tenant(request)
        id      = (if on(Fault.SharedQuota) then "everyone" else owner, user)
        reservation = UUID.randomUUID().toString
        admitted <-
          if on(Fault.QuotaAlwaysContended) then IO.pure(None)
          else
            state.modify { s =>
              val used = s.used.getOrElse(id, 0L)
              if on(Fault.NoQuotaLimit) || used + tokens <= UserLimit then
                (
                  s.copy(
                    used = s.used.updated(id, used + tokens),
                    reservations = s.reservations.updated(reservation, owner),
                  ),
                  Some(true),
                )
              else (s, Some(false))
            }
      yield admitted match
        case None        => json(Status.ServiceUnavailable, "error" -> Json.fromString("contended"))
        case Some(true)  => json(Status.Ok, "reservationId" -> Json.fromString(reservation))
        case Some(false) => json(Status.TooManyRequests, "error" -> Json.fromString("quota_exceeded"))

    def reconcile(request: Request[IO]): IO[Response[IO]] =
      field(request, "reservationId").map(_.asString.getOrElse("")).flatMap { id =>
        state.get.map(s =>
          if s.reservations.get(id).exists(owner => on(Fault.ReconcileAnyone) || owner == tenant(request)) then
            json(Status.Ok, "status" -> Json.fromString("reconciled"))
          else json(Status.NotFound, "error" -> Json.fromString("reservation_not_found")),
        )
      }

    HttpApp[IO] { request =>
      val answer =
        if on(Fault.AlwaysError) then IO.pure(json(Status.InternalServerError, "error" -> Json.fromString("internal_error")))
        else
          request match
            case GET -> Root / "health"                         => IO.pure(json(Status.Ok, "status" -> Json.fromString("healthy"), "version" -> Json.fromString("0.0.0-fake"), "commit" -> Json.fromString("fake123")))
            case POST -> Root / "v1" / "ratelimit" / "check"    => rateLimit(request)
            case POST -> Root / "v1" / "idempotency" / "check"  => idempotency(request)
            case POST -> Root / "v1" / "quota" / "check"        => quotaCheck(request)
            case POST -> Root / "v1" / "quota" / "reconcile"    => reconcile(request)
            case _                                              => IO.pure(Response[IO](Status.NotFound))
      // An Ember server dates every answer; an app run in process does not.
      if on(Fault.NoDate) then answer
      else (answer, HttpDate.current[IO]).mapN((response, now) => response.putHeaders(Date(now)))
    }
  }
