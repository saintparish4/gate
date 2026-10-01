package api

import org.http4s.*
import org.http4s.dsl.io.*
import org.http4s.implicits.*
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.typelevel.log4cats.noop.NoOpLogger

import cats.effect.*
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all.*
import io.circe.parser.parse
import observability.CorrelationIdMiddleware

/** The app Main builds: `Routes.toHttpApp` with the correlation middleware
  * around it.
  *
  * The middleware used to wrap the routes alone, inside the error handling. A
  * request whose handler raised never produced a response for it to stamp, so
  * the 400, 422 and 500 answers went out without `X-Request-Id`: the answers a
  * caller most needs to quote back.
  */
class RoutesErrorBodySpec extends AsyncFreeSpec with AsyncIOSpec with Matchers:

  private val routes: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case GET -> Root / "ok" => Ok("fine")
    case GET -> Root / "malformed" => IO
        .raiseError(MalformedMessageBodyFailure("Invalid JSON"))
    case GET -> Root / "invalid" => IO
        .raiseError(InvalidMessageBodyFailure("Could not decode JSON"))
    case GET -> Root / "boom" => IO.raiseError(new RuntimeException("boom"))
  }

  private def app: IO[HttpApp[IO]] = CorrelationIdMiddleware.makeLocal
    .map(local =>
      Routes.toHttpApp(
        routes,
        NoOpLogger[IO],
        CorrelationIdMiddleware.middleware(local),
      ),
    )

  private def run(
      path: String,
      requestId: Option[String],
  ): IO[(Status, Option[String], Option[String])] =
    val request = Request[IO](Method.GET, Uri.unsafeFromString(path))
    for
      httpApp <- app
      response <- httpApp.run(requestId.fold(request)(id =>
        request
          .putHeaders(Header.Raw(CorrelationIdMiddleware.RequestIdHeader, id)),
      ))
      body <- response.bodyText.compile.string
    yield (
      response.status,
      response.headers.get(CorrelationIdMiddleware.RequestIdHeader)
        .map(_.head.value),
      parse(body).toOption.flatMap(_.hcursor.get[String]("error").toOption),
    )

  "Routes.toHttpApp with the correlation middleware around it" - {

    List(
      ("/malformed", Status.BadRequest, Some("invalid_request")),
      ("/invalid", Status.UnprocessableEntity, Some("invalid_request")),
      ("/boom", Status.InternalServerError, Some("internal_error")),
      ("/no-such-path", Status.NotFound, Some("not_found")),
      ("/ok", Status.Ok, None),
    ).foreach { case (path, status, error) =>
      s"$path answers ${status.code} and echoes X-Request-Id" in
        run(path, Some("req-42"))
          .asserting(_ shouldBe (status, Some("req-42"), error))
    }

    "an error answer gets a generated X-Request-Id when the request had none" in
      run("/boom", None).asserting { case (status, requestId, _) =>
        status shouldBe Status.InternalServerError
        requestId.exists(_.nonEmpty) shouldBe true
      }

    "without a middleware it still answers in the error shape" in
      Routes.toHttpApp(routes, NoOpLogger[IO])
        .run(Request[IO](Method.GET, uri"/boom"))
        .asserting(_.status shouldBe Status.InternalServerError)
  }
