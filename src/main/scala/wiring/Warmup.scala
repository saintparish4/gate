package wiring

import java.util.UUID

import scala.concurrent.duration.*

import org.http4s.*
import org.http4s.server.AuthMiddleware
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.noop.NoOpLogger
import org.typelevel.otel4s.trace.Tracer

import cats.data.{Kleisli, OptionT}
import cats.effect.*
import cats.effect.syntax.all.*
import cats.syntax.all.*
import io.circe.Json
import io.circe.parser.parse
import api.{Routes, TokenQuotaApi}
import config.{IdempotencyConfig, RateLimitConfig, TokenQuotaConfig}
import core.*
import events.EventPublisher
import observability.MetricsPublisher
import resilience.AggregateHealth
import security.{AuthenticatedClient, ClientTier, Permission}

/** Exercises every request path before the server binds.
  *
  * A task that takes load within seconds of starting pays class loading, SDK
  * start-up, TLS handshakes and an interpreted hot path on every request at
  * once. On 1 October 2026 a 1-vCPU task that was 27 seconds old ran at 100%
  * CPU and about a quarter of its warm throughput, and eight calls missed the
  * 2-second store timeout: three rate-limit checks were answered by the
  * degradation mode and five idempotency checks answered 503. The same task
  * passed four minutes later.
  *
  * The warm-up used to be five rate-limit calls against the raw store. That
  * covered SDK start-up on one path, and nothing on the idempotency or quota
  * paths, the JSON codecs, or the routes. It now sends whole requests, in
  * process, through the same `Routes` the server uses, over the raw stores (no
  * breaker, bulkhead or request timeout, which a cold call would trip), so that
  * binding the port means each path has been run.
  */
object Warmup:

  /** How much to run before binding.
    *
    * @param rounds
    *   Rounds to run; each is one pass over every path, six or eight requests
    * @param parallelism
    *   Rounds in flight at once, which also opens that many connections to
    *   DynamoDB before the first real burst needs them
    * @param budget
    *   When to stop, however many rounds are done. A cold task is slow and must
    *   still bind before its health checks give up on it.
    */
  final case class Settings(
      rounds: Int = 300,
      parallelism: Int = 16,
      budget: FiniteDuration = 20.seconds,
  )

  object Settings:
    /** Against LocalStack: enough to load the classes and open the clients. The
      * emulator is slow, so the full warm-up always ran to its budget and added
      * 20 seconds to every local start, to warm a JVM nobody measures.
      */
    val localStack: Settings = Settings(rounds = 16, parallelism = 4)

  /** What a warm-up did. `failed` counts rounds whose request raised; a 4xx or
    * 5xx answer is still an exercised path.
    */
  final case class Report(completed: Int, failed: Int, elapsed: FiniteDuration)

  // Hot keys, so parallel rounds contend and the OCC retry path runs too.
  private val HotKeys = 4

  /** Warm the paths over these stores, and log what was done. It never fails
    * start-up: a failed warm-up is a slow first minute, not a broken task.
    */
  def run[F[_]: Async](
      rateLimitStore: RateLimitStore[F],
      idempotencyStore: IdempotencyStore[F],
      tokenQuotaStore: Option[TokenQuotaStore[F]],
      rateLimitConfig: RateLimitConfig,
      idempotencyConfig: IdempotencyConfig,
      tokenQuotaConfig: TokenQuotaConfig,
      logger: Logger[F],
      settings: Settings = Settings(),
  ): F[Report] =
    val app = silentApp(
      rateLimitStore,
      idempotencyStore,
      tokenQuotaStore,
      rateLimitConfig,
      idempotencyConfig,
      tokenQuotaConfig,
    )
    requestPaths(app, tokenQuotaStore.isDefined, settings).flatTap(report =>
      logger.info(s"Request paths warmed: ${report.completed} of ${settings
          .rounds} rounds in ${report.elapsed.toMillis} ms (${report
          .failed} failed)"),
    )

  /** The server's routes over the given stores, for a client that exists only
    * here, with nothing that records a decision attached: no events, no
    * request, idempotency or quota metrics, no traces, no audit lines. Warm-up
    * traffic must not show up as decisions anyone made.
    *
    * The stores are the server's own, so their diagnostics still see it: a
    * start logs a few OCC retry warnings for the warm-up's keys and counts
    * those retries.
    *
    * The client ID is random per start. Its keys are scoped like any client's
    * (ADR-005), so nothing it writes can meet a real client's state, and all of
    * it expires by TTL.
    */
  private[wiring] def silentApp[F[_]: Async](
      rateLimitStore: RateLimitStore[F],
      idempotencyStore: IdempotencyStore[F],
      tokenQuotaStore: Option[TokenQuotaStore[F]],
      rateLimitConfig: RateLimitConfig,
      idempotencyConfig: IdempotencyConfig,
      tokenQuotaConfig: TokenQuotaConfig,
  ): HttpApp[F] =
    given Tracer[F] = Tracer.noop[F]
    val silent = NoOpLogger[F]
    val clientId = s"gate-warmup-${UUID.randomUUID()}"
    val client = AuthenticatedClient(
      apiKeyId = clientId,
      clientId = clientId,
      clientName = "Warm-up",
      // The smallest bucket, so a round's checks are refused as well as
      // admitted.
      tier = ClientTier.Free,
      permissions = Permission.standard,
    )
    val requestId = () => Async[F].pure("warmup")
    new Routes[F](
      rateLimitStore,
      idempotencyStore,
      EventPublisher.noop[F],
      MetricsPublisher.noop[F],
      AuthMiddleware[F, AuthenticatedClient](Kleisli(_ => OptionT.pure(client))),
      rateLimitConfig,
      idempotencyConfig,
      silent,
      dashboardApi = None,
      tokenQuotaApi = tokenQuotaStore.map(store =>
        TokenQuotaApi[F](
          TokenQuotaService[F](
            store,
            tokenQuotaConfig,
            MetricsPublisher.noop[F],
            silent,
          ),
          EventPublisher.noop[F],
          MetricsPublisher.noop[F],
          silent,
          requestId,
        ),
      ),
      prometheusMetrics = None,
      healthCheck = Async[F].pure(AggregateHealth("ok", Nil)),
      getRequestId = requestId,
    ).httpApp

  /** Run `settings.rounds` rounds against `app`, `settings.parallelism` at a
    * time, stopping at `settings.budget`.
    */
  private[wiring] def requestPaths[F[_]: Async](
      app: HttpApp[F],
      quota: Boolean,
      settings: Settings,
  ): F[Report] =
    for
      start <- Clock[F].monotonic
      completed <- Ref.of[F, Int](0)
      failed <- Ref.of[F, Int](0)
      rounds = (0 until settings.rounds).toList
        .parTraverseN(math.max(1, settings.parallelism))(i =>
          round(app, i, quota).attempt.flatMap {
            case Right(_) => completed.update(_ + 1)
            case Left(_) => failed.update(_ + 1)
          },
        ).void
      // Cancels whatever is in flight: a store that hangs must not hold the
      // port closed.
      _ <- Async[F].timeoutTo(rounds, settings.budget, Async[F].unit)
      end <- Clock[F].monotonic
      done <- completed.get
      lost <- failed.get
    yield Report(done, lost, end - start)

  /** One pass over every path: a check and a status read on a hot bucket; an
    * idempotency key claimed, seen in progress, completed with its claim ID,
    * and replayed; and, when quotas are on, a reservation and its reconcile.
    */
  private def round[F[_]: Async](
      app: HttpApp[F],
      i: Int,
      quota: Boolean,
  ): F[Unit] =
    val bucket = s"warmup-${i % HotKeys}"
    val key = s"warmup-$i"
    val check = post[F](
      "/v1/idempotency/check",
      s"""{"idempotencyKey":"$key","ttl":60,"requestBody":"warmup"}""",
    )
    for
      _ <- send(app, post("/v1/ratelimit/check", s"""{"key":"$bucket"}"""))
      _ <- send(app, Request[F](Method.GET, uri(s"/v1/ratelimit/status/$bucket")))
      claimed <- send(app, check)
      _ <- send(app, check)
      claimId = claimed.hcursor.get[String]("claimId").toOption
      _ <- send(
        app,
        post(
          s"/v1/idempotency/$key/complete",
          Json.obj(
            "statusCode" -> Json.fromInt(200),
            "body" -> Json.fromString("{}"),
            "claimId" -> claimId.fold(Json.Null)(Json.fromString),
          ).noSpaces,
        ),
      )
      _ <- send(app, check)
      _ <-
        if !quota then Async[F].unit
        else
          send(
            app,
            post(
              "/v1/quota/check",
              s"""{"userId":"$bucket","estimatedInputTokens":1}""",
            ),
          ).flatMap(reserved =>
            reserved.hcursor.get[String]("reservationId").toOption
              .traverse_(id =>
                send(
                  app,
                  post("/v1/quota/reconcile", s"""{"reservationId":"$id","actualInputTokens":1,"actualOutputTokens":0}"""),
                ),
              ),
          )
    yield ()

  private def uri(path: String): Uri = Uri.unsafeFromString(path)

  private def post[F[_]](path: String, body: String): Request[F] =
    Request[F](Method.POST, uri(path)).withEntity(body)

  // The body is read whatever the status, so the response encoders run too.
  private def send[F[_]: Async](app: HttpApp[F], request: Request[F]): F[Json] =
    app.run(request).flatMap(_.bodyText.compile.string)
      .map(parse(_).getOrElse(Json.Null))
