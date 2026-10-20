package aggregator

import java.net.URI

import scala.concurrent.duration.*

import org.http4s.*
import org.http4s.dsl.io.*
import org.http4s.ember.server.EmberServerBuilder
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import com.comcast.ip4s.*

import cats.effect.*
import cats.syntax.all.*
import software.amazon.awssdk.auth.credentials.{
  AwsBasicCredentials, DefaultCredentialsProvider, StaticCredentialsProvider,
}
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.retry.RetryPolicy as SdkRetryPolicy
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient

/** Settings, all from the environment. A missing required one stops startup.
  */
final case class AggregatorConfig(
    region: String,
    stream: String,
    table: String,
    // LocalStack's gateway; unset on AWS.
    endpoint: Option[String],
    port: Int,
    ttlDays: Long,
    consumer: Consumer.Settings,
)

object AggregatorConfig:
  def fromEnv(env: Map[String, String]): Either[String, AggregatorConfig] =
    def required(name: String) = env.get(name).filter(_.nonEmpty)
      .toRight(s"$name is required")
    def int(name: String, default: Int) = env.get(name)
      .fold[Either[String, Int]](Right(default))(v =>
        v.toIntOption.filter(_ > 0)
          .toRight(s"$name must be a positive integer, got '$v'"),
      )
    for
      region <- required("AWS_REGION")
      stream <- required("KINESIS_STREAM")
      table <- required("USAGE_TABLE")
      port <- int("AGGREGATOR_PORT", 9095)
      ttlDays <- int("USAGE_TTL_DAYS", 90)
      batch <- int("AGGREGATOR_BATCH_LIMIT", 1000)
      idle <- int("AGGREGATOR_IDLE_POLL_MS", 1000)
    yield AggregatorConfig(
      region,
      stream,
      table,
      env.get("AWS_ENDPOINT").filter(_.nonEmpty),
      port,
      ttlDays.toLong,
      Consumer.Settings(batchLimit = batch, idlePoll = idle.millis),
    )

object Main extends IOApp:

  given Logger[IO] = Slf4jLogger.getLogger[IO]

  // Retries are the consumer's: any failure sends a shard back to its
  // checkpoint. SDK retries would hide them, as they would in the service.
  private def overrides = ClientOverrideConfiguration.builder()
    .retryPolicy(SdkRetryPolicy.none()).build()

  private def credentials(config: AggregatorConfig) =
    if config.endpoint.isDefined then
      StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"))
    else DefaultCredentialsProvider.create()

  def run(args: List[String]): IO[ExitCode] =
    AggregatorConfig.fromEnv(sys.env) match
      case Left(problem) => Logger[IO].error(s"Refusing to start: $problem")
          .as(ExitCode.Error)
      case Right(config) => serve(config).useForever

  private def serve(config: AggregatorConfig): Resource[IO, Unit] =
    val region = Region.of(config.region)
    for
      kinesis <- Resource.fromAutoCloseable(IO {
        val b = KinesisAsyncClient.builder().region(region)
          .credentialsProvider(credentials(config))
          .overrideConfiguration(overrides)
        config.endpoint.fold(b)(e => b.endpointOverride(URI.create(e))).build()
      })
      dynamo <- Resource.fromAutoCloseable(IO {
        val b = DynamoDbAsyncClient.builder().region(region)
          .credentialsProvider(credentials(config))
          .overrideConfiguration(overrides)
        config.endpoint.fold(b)(e => b.endpointOverride(URI.create(e))).build()
      })
      metrics = AggregatorMetrics.create(withJvm = true)
      store = DynamoDBUsageStore[IO](
        dynamo,
        config.table,
        config.stream,
        config.ttlDays * 86400,
      )
      source = StreamSource.kinesis[IO](kinesis, config.stream)
      consumer = Consumer[IO](source, store, metrics, config.consumer)
      _ <- consumer.run.background
      routes = HttpRoutes.of[IO] {
        case GET -> Root / "health" => Ok("""{"status":"ok"}""")
        // Ready when its table answers. The stream is not probed: a shard that
        // cannot be read shows as lag, which is what the SLO watches.
        case GET -> Root / "ready" => store.healthCheck.flatMap {
            case Right(_) => Ok("""{"status":"ready"}""")
            case Left(reason) => ServiceUnavailable(s"usage table: $reason")
          }
        case GET -> Root / "metrics" => Ok(metrics.scrape)
      }
      _ <- EmberServerBuilder.default[IO].withHost(host"0.0.0.0")
        .withPort(Port.fromInt(config.port).getOrElse(port"9095"))
        .withHttpApp(routes.orNotFound).build
      _ <- Resource.eval(Logger[IO].info(s"Aggregating ${config
          .stream} into ${config.table}; metrics on :${config.port}"))
    yield ()
