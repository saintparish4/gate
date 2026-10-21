package observability

import org.http4s.*
import org.typelevel.ci.*

import cats.data.{Kleisli, OptionT}
import cats.effect.*
import cats.syntax.all.*

/** Splits each answer's time into time in gate and time waiting on the store.
  *
  * The store calls are already timed, under the names below, around the raw
  * store call (OCC retries included, bulkhead wait excluded on the rate-limit
  * path). This adds each of those timings to a per-request counter that the
  * middleware reads when the answer is ready, and reports it in a standard
  * `Server-Timing` header and in `gate_http_store_seconds`. Without it a slow
  * answer could not say whether gate or its store was slow, and on LocalStack
  * that is the whole question.
  *
  * The counter is a `Ref` held in an `IOLocal`: a fiber forked during the
  * request copies the local but shares the `Ref`, so its store time counts.
  */
object RequestTiming:

  type Local = IOLocal[Option[Ref[IO, Long]]]

  val StoreTimings: Set[String] = Set(
    "RateLimitCheckLatency",
    "RateLimitStatusLatency",
    "IdempotencyStoreLatency",
    "TokenQuotaStoreLatency",
  )

  val InstanceHeader: CIString = ci"X-Gate-Instance"

  def makeLocal: IO[Local] = IOLocal(Option.empty[Ref[IO, Long]])

  /** The same publisher, but its store timings also add to the request's. */
  def publisher(
      underlying: MetricsPublisher[IO],
      local: Local,
  ): MetricsPublisher[IO] = new MetricsPublisher[IO]:
    def increment(name: String, dimensions: Map[String, String]) = underlying
      .increment(name, dimensions)
    def count(name: String, amount: Double, dimensions: Map[String, String]) =
      underlying.count(name, amount, dimensions)
    def gauge(name: String, value: Double, dimensions: Map[String, String]) =
      underlying.gauge(name, value, dimensions)
    def recordLatency(
        name: String,
        latencyMs: Double,
        dimensions: Map[String, String],
    ) = underlying.recordLatency(name, latencyMs, dimensions)
    def recordRateLimitDecision(
        allowed: Boolean,
        clientId: String,
        tier: String,
    ) = underlying.recordRateLimitDecision(allowed, clientId, tier)
    def recordCircuitBreakerState(
        name: String,
        state: String,
        failureCount: Int,
    ) = underlying.recordCircuitBreakerState(name, state, failureCount)
    def flush = underlying.flush
    override def timed[A](name: String, dimensions: Map[String, String])(
        fa: IO[A],
    ): IO[A] =
      if !StoreTimings(name) then underlying.timed(name, dimensions)(fa)
      else
        IO.monotonic.flatMap(start =>
          underlying.timed(name, dimensions)(fa)
            .guarantee(IO.monotonic.flatMap(end =>
              local.get.flatMap(_.traverse_(_.update(_ + (end - start).toNanos))),
            )),
        )

  /** Starts each request's counter, and on the answer writes
    * `Server-Timing: store;dur=<ms>, gate;dur=<ms>` and the replica's name.
    */
  def middleware(
      local: Local,
      prom: Option[PrometheusMetrics[IO]],
      instance: String,
  )(routes: HttpRoutes[IO]): HttpRoutes[IO] = Kleisli { (req: Request[IO]) =>
    val template = HttpMetrics.route(req.uri.path)
    OptionT {
      for
        counter <- Ref.of[IO, Long](0L)
        _ <- local.set(Some(counter))
        started <- IO.monotonic
        answer <- routes(req).value.guarantee(local.set(None))
        ended <- IO.monotonic
        store <- counter.get
        _ <- prom.traverse_(p =>
          IO(p.httpStoreDuration.labels(template).observe(store / 1e9)),
        )
      yield answer.map { resp =>
        val total = (ended - started).toNanos
        val gate = math.max(0L, total - store)
        resp.putHeaders(
          Header.Raw(
            ci"Server-Timing",
            f"store;dur=${store / 1e6}%.3f, gate;dur=${gate / 1e6}%.3f",
          ),
          Header.Raw(InstanceHeader, instance),
        )
      }
    }
  }
