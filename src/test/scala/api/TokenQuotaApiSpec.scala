package api

import scala.concurrent.duration.*

import org.http4s.*
import org.http4s.circe.*
import org.http4s.circe.CirceEntityDecoder.*
import org.http4s.circe.CirceEntityEncoder.*
import org.http4s.implicits.*
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.typelevel.ci.*
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.noop.NoOpLogger
import org.typelevel.otel4s.trace.Tracer

import cats.effect.*
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all.*
import io.circe.generic.auto.*
import io.circe.syntax.*
import core.*
import config.{IdempotencyConfig, RateLimitConfig, TokenQuotaConfig}
import events.*
import observability.MetricsPublisher
import security.*
import testutil.*

class TokenQuotaApiSpec extends AsyncFreeSpec with AsyncIOSpec with Matchers:

  given Logger[IO] = NoOpLogger[IO]
  given Tracer[IO] = Tracer.noop[IO]

  /** A tight quota config: 100 tokens per user per second. */
  val tightConfig: TokenQuotaConfig = TokenQuotaConfig(
    enabled = true,
    userLimit = 100L,
    agentLimit = 80L,
    orgLimit = 500L,
    userWindowSeconds = 1L,
    agentWindowSeconds = 1L,
    orgWindowSeconds = 1L,
  )

  def makeApi(
      config: TokenQuotaConfig = tightConfig,
      events: EventPublisher[IO] = EventPublisher.noop[IO],
      metrics: MetricsPublisher[IO] = MetricsPublisher.noop[IO],
  ): IO[TokenQuotaApi[IO]] = TokenQuotaStore.inMemory[IO].map { store =>
    val service = TokenQuotaService(store, config, metrics, Logger[IO])
    TokenQuotaApi[IO](
      service,
      events,
      metrics,
      Logger[IO],
      () => IO.pure("test-request-id"),
    )
  }

  /** A service whose answers are fixed, for exercising the HTTP mapping. */
  def stubService(
      decision: QuotaDecision,
      reconciled: ReconcileResult,
  ): TokenQuotaService[IO] = new TokenQuotaService[IO]:
    def checkQuota(
        clientId: String,
        identifier: QuotaIdentifier,
        estimatedInputTokens: Long,
        estimatedOutputTokens: Long,
    ): IO[QuotaDecision] = IO.pure(decision)
    def reconcile(
        clientId: String,
        reservationId: String,
        actualInputTokens: Long,
        actualOutputTokens: Long,
    ): IO[ReconcileResult] = IO.pure(reconciled)

  def makeApiWith(service: TokenQuotaService[IO]): TokenQuotaApi[IO] =
    TokenQuotaApi[IO](
      service,
      EventPublisher.noop[IO],
      MetricsPublisher.noop[IO],
      Logger[IO],
      () => IO.pure("test-request-id"),
    )

  def checkRequest(
      userId: String,
      estimatedInput: Long,
      estimatedOutput: Long = 0,
  ): Request[IO] = Request[IO](method = Method.POST, uri = uri"/v1/quota/check")
    .withEntity(
      TokenQuotaCheckRequest(
        userId = userId,
        estimatedInputTokens = estimatedInput,
        estimatedOutputTokens = estimatedOutput,
      ).asJson,
    )

  def reconcileRequest(
      reservationId: String,
      actualInput: Long,
      actualOutput: Long,
  ): Request[IO] =
    Request[IO](method = Method.POST, uri = uri"/v1/quota/reconcile").withEntity(
      TokenQuotaReconcileRequest(
        reservationId = reservationId,
        actualInputTokens = actualInput,
        actualOutputTokens = actualOutput,
      ).asJson,
    )

  /** Checks, then returns the reservation ID from the 200 body. */
  def reserve(
      api: TokenQuotaApi[IO],
      userId: String,
      tokens: Long,
  ): IO[String] = api
    .check(checkRequest(userId, estimatedInput = tokens), testClient)
    .flatMap(_.as[TokenQuotaCheckResponse]).flatMap(body =>
      IO.fromOption(body.reservationId)(new AssertionError(
        s"no reservationId in $body",
      )),
    )

  "TokenQuotaApi" - {

    "returns 200 with remaining tokens when under quota" in makeApi().flatMap(
      api => api.check(checkRequest("user-a", estimatedInput = 50), testClient),
    ).asserting(response => response.status shouldBe Status.Ok)

    "returns 429 with Retry-After when quota is exceeded" in
      makeApi().flatMap(api =>
        for
          // Use 90 tokens, leaving 10 remaining
          _ <- api.check(checkRequest("user-b", estimatedInput = 90), testClient)
          // Request 50 more — exceeds the 100-token limit
          r <- api.check(checkRequest("user-b", estimatedInput = 50), testClient)
        yield r,
      ).asserting { response =>
        response.status shouldBe Status.TooManyRequests
        response.headers.get(ci"Retry-After") shouldBe defined
      }

    // It was a 429 with a Retry-After that could never come true.
    "an estimate above a level's limit is 400 and reserves nothing" in
      makeApi().flatMap(api =>
        for
          r <- api
            .check(checkRequest("user-big", estimatedInput = 101), testClient)
          message <- r.bodyText.compile.string
          whole <- api
            .check(checkRequest("user-big", estimatedInput = 100), testClient)
        yield (r.status, r.headers.get(ci"Retry-After"), message, whole.status),
      ).asserting { case (status, retryAfter, message, whole) =>
        status shouldBe Status.BadRequest
        retryAfter shouldBe None
        message should include("exceeds the user limit of 100")
        whole shouldBe Status.Ok
      }

    "returns 400 for negative token estimates" in makeApi().flatMap(api =>
      api.check(checkRequest("user-c", estimatedInput = -1), testClient),
    ).asserting(_.status shouldBe Status.BadRequest)

    "POST /v1/quota/reconcile returns 200 with the delta against the stored estimate" in
      makeApi().flatMap(api =>
        for
          id <- reserve(api, "user-d", 100)
          resp <- api.reconcile(reconcileRequest(id, 80, 10), testClient)
          body <- resp.as[TokenQuotaReconcileResponse]
        yield (resp.status, body),
      ).asserting { case (status, body) =>
        status shouldBe Status.Ok
        body.inputDelta shouldBe -20L // 80 actual - 100 estimated
        body.outputDelta shouldBe 10L // 10 actual - 0 estimated
      }

    "POST /v1/quota/reconcile returns 404 for a reservation it does not know" in
      makeApi().flatMap(api =>
        api.reconcile(reconcileRequest("unknown", 0, 0), testClient),
      ).asserting(_.status shouldBe Status.NotFound)

    "POST /v1/quota/reconcile returns 409 for a second reconcile with different usage" in
      makeApi().flatMap(api =>
        for
          id <- reserve(api, "user-i", 100)
          _ <- api.reconcile(reconcileRequest(id, 80, 0), testClient)
          again <- api.reconcile(reconcileRequest(id, 0, 0), testClient)
          body <- again.bodyText.compile.string
        yield (again.status, body),
      ).asserting { case (status, body) =>
        status shouldBe Status.Conflict
        body should include("already_reconciled")
      }

    "puts the same Retry-After in the header and the body when quota is exceeded" in
      makeApi().flatMap(api =>
        for
          _ <- api.check(checkRequest("user-e", estimatedInput = 90), testClient)
          resp <- api
            .check(checkRequest("user-e", estimatedInput = 50), testClient)
          body <- resp.as[TokenQuotaCheckResponse]
        yield (resp, body),
      ).asserting { case (resp, body) =>
        resp.status shouldBe Status.TooManyRequests
        body.allowed shouldBe false
        body.exceededLevel shouldBe Some("user")
        // The window is one second, so the wait can only ever be one second.
        body.retryAfter shouldBe Some(1)
        resp.headers.get(ci"Retry-After").map(_.head.value) shouldBe Some("1")
      }

    "returns 503 with Retry-After when the reservation is contended" in {
      val api = makeApiWith(
        stubService(QuotaDecision.Contended(25), ReconcileResult.Reconciled(0, 0)),
      )
      for
        resp <- api
          .check(checkRequest("user-f", estimatedInput = 10), testClient)
        body <- resp.bodyText.compile.string
      yield
        resp.status shouldBe Status.ServiceUnavailable
        resp.headers.get(ci"Retry-After").map(_.head.value) shouldBe Some("1")
        // The error shape, as reconcile's contended 503 has: it answered in
        // the check's own shape, a second 503 body on one route.
        body should startWith("""{"error":"contended","message":""")
    }

    "returns 400 for negative reconcile counts" in makeApi()
      .flatMap(api => api.reconcile(reconcileRequest("any", -1, 0), testClient))
      .asserting(_.status shouldBe Status.BadRequest)

    "POST /v1/quota/reconcile returns 503 when the adjustment cannot be recorded" in {
      val api = makeApiWith(stubService(
        QuotaDecision.Available(Map.empty, "reservation-1"),
        ReconcileResult.Contended(25),
      ))
      for
        resp <- api
          .reconcile(reconcileRequest("reservation-1", 80, 10), testClient)
        body <- resp.bodyText.compile.string
      yield
        resp.status shouldBe Status.ServiceUnavailable
        resp.headers.get(ci"Retry-After").map(_.head.value) shouldBe Some("1")
        body should include("contended")
    }

    // Through Routes itself, with no quota API wired. This used to assert on an
    // inline copy of the route's `match`, so it passed whatever Routes did.
    "both quota routes answer 404 when token-quota is disabled" in {
      val keys = ApiKeyStore.inMemory[IO](Map("quota-off-key" -> testClient))
      val bearer = headers
        .Authorization(Credentials.Token(ci"Bearer", "quota-off-key"))
      for
        rateLimitStore <- RateLimitStore.inMemory[IO]
        idempotencyStore <- IdempotencyStore.inMemory[IO]
        routes <- Routes[IO](
          rateLimitStore,
          idempotencyStore,
          EventPublisher.noop[IO],
          MetricsPublisher.noop[IO],
          ApiKeyAuth.middleware[IO](keys),
          RateLimitConfig(
            defaultCapacity = 10,
            defaultRefillRatePerSecond = 1.0,
            defaultTtlSeconds = 3600,
          ),
          IdempotencyConfig(),
          Logger[IO],
          tokenQuotaApi = None,
          healthCheck = IO.pure(resilience.AggregateHealth("ok", Nil)),
          getRequestId = () => IO.pure("test-request-id"),
        )
        answers <- List(
          checkRequest("user-off", estimatedInput = 10),
          reconcileRequest("reservation-off", 1, 0),
        ).traverse(request =>
          routes.httpApp.run(request.putHeaders(bearer))
            .flatMap(r => r.bodyText.compile.string.map((r.status, _))),
        )
      yield answers.foreach { case (status, body) =>
        status shouldBe Status.NotFound
        body should include(""""error":"not_found"""")
      }
    }

    "a store failure is a 503 the caller can act on, not a 500" in {
      val failing = new TokenQuotaService[IO]:
        def checkQuota(
            clientId: String,
            identifier: QuotaIdentifier,
            estimatedInputTokens: Long,
            estimatedOutputTokens: Long,
        ) = IO.raiseError(core.GateError.StoreTimeout("quota", 5.seconds))
        def reconcile(
            clientId: String,
            reservationId: String,
            actualInputTokens: Long,
            actualOutputTokens: Long,
        ) = IO.raiseError(new RuntimeException("dynamo down"))
      val api = makeApiWith(failing)
      for
        check <- api
          .check(checkRequest("user-z", estimatedInput = 10), testClient)
        checkBody <- check.bodyText.compile.string
        rec <- api.reconcile(reconcileRequest("r", 1, 0), testClient)
      yield
        check.status shouldBe Status.ServiceUnavailable
        check.headers.get(ci"Retry-After") shouldBe defined
        checkBody should include("storage_unavailable")
        rec.status shouldBe Status.ServiceUnavailable
    }
  }
