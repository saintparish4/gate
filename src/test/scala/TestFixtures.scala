package object testutil:

  import org.typelevel.log4cats.Logger

  import cats.effect.*
  import observability.MetricsPublisher
  import core.*
  import security.{AuthenticatedClient, ClientTier, Permission}

  val testProfile: RateLimitProfile =
    RateLimitProfile(capacity = 10, refillRatePerSecond = 1.0, ttlSeconds = 3600)

  val testClient: AuthenticatedClient = AuthenticatedClient(
    apiKeyId = "test-client",
    clientId = "test-client",
    clientName = "Test",
    tier = ClientTier.Free,
    permissions = Permission.standard,
  )

  def capturingMetrics(ref: Ref[IO, List[String]]): MetricsPublisher[IO] =
    new MetricsPublisher[IO]:
      def increment(
          name: String,
          dims: Map[String, String] = Map.empty,
      ): IO[Unit] = ref.update(_ :+ name)
      def count(
          name: String,
          amount: Double,
          dims: Map[String, String] = Map.empty,
      ): IO[Unit] = ref.update(_ :+ name)
      def gauge(
          name: String,
          value: Double,
          dims: Map[String, String] = Map.empty,
      ): IO[Unit] = IO.unit
      def recordLatency(
          name: String,
          latencyMs: Double,
          dims: Map[String, String] = Map.empty,
      ): IO[Unit] = IO.unit
      def recordRateLimitDecision(
          allowed: Boolean,
          clientId: String,
          tier: String = "unknown",
      ): IO[Unit] = IO.unit
      def recordCircuitBreakerState(
          name: String,
          state: String,
          failures: Int,
      ): IO[Unit] = IO.unit
      def recordCacheMetrics(
          cacheName: String,
          hitRate: Double,
          size: Long,
      ): IO[Unit] = IO.unit
      def recordDegradedOperation(operation: String): IO[Unit] = IO.unit
      override def timed[A](name: String, dims: Map[String, String] = Map.empty)(
          fa: IO[A],
      ): IO[A] = fa
      def flush: IO[Unit] = IO.unit

  def capturingLogger(ref: Ref[IO, List[String]]): Logger[IO] = new Logger[IO]:
    def error(t: Throwable)(msg: => String): IO[Unit] = ref.update(_ :+ msg)
    def error(msg: => String): IO[Unit] = ref.update(_ :+ msg)
    def warn(t: Throwable)(msg: => String): IO[Unit] = ref.update(_ :+ msg)
    def warn(msg: => String): IO[Unit] = ref.update(_ :+ msg)
    def info(t: Throwable)(msg: => String): IO[Unit] = IO.unit
    def info(msg: => String): IO[Unit] = IO.unit
    def debug(t: Throwable)(msg: => String): IO[Unit] = IO.unit
    def debug(msg: => String): IO[Unit] = IO.unit
    def trace(t: Throwable)(msg: => String): IO[Unit] = IO.unit
    def trace(msg: => String): IO[Unit] = IO.unit

  def failingStore(ex: Throwable): RateLimitStore[IO] = new RateLimitStore[IO]:
    def checkAndConsume(
        key: String,
        cost: Int,
        profile: RateLimitProfile,
    ): IO[RateLimitDecision] = IO.raiseError(ex)
    def getStatus(
        key: String,
        profile: RateLimitProfile,
    ): IO[Option[RateLimitDecision.Allowed]] = IO.raiseError(ex)
    def healthCheck: IO[Either[String, Unit]] = IO.pure(Left(ex.getMessage))
