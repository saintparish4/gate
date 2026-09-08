package storage

import java.net.URI
import java.time.Duration as JavaDuration

import scala.concurrent.duration.FiniteDuration

import cats.effect.{Async, Resource}
import config.{AwsConfig, DynamoDBConfig}
import software.amazon.awssdk.auth.credentials.{
  AwsBasicCredentials, DefaultCredentialsProvider, StaticCredentialsProvider,
}
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.retry.RetryPolicy as SdkRetryPolicy
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient

/** Factory for AWS SDK async clients with proper resource management
  *
  * All clients are wrapped in Resource to ensure proper cleanup on shutdown
  * Supports both real AWS and Localstack for development
  */
object AwsClients:

  /** Create a DynamoDB async client as a managed Resource
    *
    * The client will be properly closed when the Resource is released, ensuring
    * no connection leaks. Configured with proper timeouts and connection pool
    * settings.
    */
  def dynamoDbClient[F[_]: Async](
      awsConfig: AwsConfig,
      dynamoDBConfig: DynamoDBConfig,
  ): Resource[F, DynamoDbAsyncClient] = Resource.make {
    Async[F].delay {
      val c = awsConfig.client
      val httpClient = NettyNioAsyncHttpClient.builder().connectionTimeout(
        JavaDuration.ofMillis(dynamoDBConfig.connectionTimeout.toMillis),
      ).readTimeout(JavaDuration.ofMillis(dynamoDBConfig.requestTimeout.toMillis))
        .writeTimeout(JavaDuration.ofMillis(
          dynamoDBConfig.requestTimeout.toMillis,
        )).maxConcurrency(c.maxConnections)
        .maxPendingConnectionAcquires(c.maxPendingAcquires)
        .connectionAcquisitionTimeout(JavaDuration.ofSeconds(
          c.connectionAcquisitionTimeoutSeconds,
        )).connectionTimeToLive(JavaDuration.ofSeconds(
          c.connectionTimeToLiveSeconds,
        ))
        .connectionMaxIdleTime(JavaDuration.ofSeconds(c.connectionMaxIdleSeconds))
        .build()

      // Disable SDK-level retries: Gate handles retries via RetryPolicy.
      val overrideConfig =
        if c.disableSdkRetries then
          ClientOverrideConfiguration.builder().retryPolicy(SdkRetryPolicy.none())
            .build()
        else ClientOverrideConfiguration.builder().build()

      val builder = DynamoDbAsyncClient.builder()
        .region(Region.of(awsConfig.region)).httpClient(httpClient)
        .overrideConfiguration(overrideConfig)

      val withCredentials =
        if awsConfig.localstack then
          builder.credentialsProvider(StaticCredentialsProvider.create(
            AwsBasicCredentials.create("test", "test"),
          ))
        else builder.credentialsProvider(DefaultCredentialsProvider.create())

      val withEndpoint = awsConfig.dynamodbEndpoint match
        case Some(endpoint) => withCredentials
            .endpointOverride(URI.create(endpoint))
        case None if awsConfig.localstack =>
          withCredentials.endpointOverride(URI.create("http://localhost:4566"))
        case None => withCredentials

      withEndpoint.build()
    }
  }(client => Async[F].delay(client.close()))

  /** Create a Kinesis async client as a managed Resource
    *
    * Configured with proper timeouts and connection pool settings.
    */
  def kinesisClient[F[_]: Async](
      awsConfig: AwsConfig,
      dynamoDBConfig: DynamoDBConfig, // Reuse DynamoDB timeout config for consistency
  ): Resource[F, KinesisAsyncClient] = Resource.make {
    Async[F].delay {
      val c = awsConfig.client
      val httpClient = NettyNioAsyncHttpClient.builder().connectionTimeout(
        JavaDuration.ofMillis(dynamoDBConfig.connectionTimeout.toMillis),
      ).readTimeout(JavaDuration.ofMillis(dynamoDBConfig.requestTimeout.toMillis))
        .writeTimeout(JavaDuration.ofMillis(
          dynamoDBConfig.requestTimeout.toMillis,
        )).maxConcurrency(c.maxConnections)
        .maxPendingConnectionAcquires(c.maxPendingAcquires)
        .connectionAcquisitionTimeout(JavaDuration.ofSeconds(
          c.connectionAcquisitionTimeoutSeconds,
        )).connectionTimeToLive(JavaDuration.ofSeconds(
          c.connectionTimeToLiveSeconds,
        ))
        .connectionMaxIdleTime(JavaDuration.ofSeconds(c.connectionMaxIdleSeconds))
        .build()

      // Disable SDK-level retries: Gate handles retries via RetryPolicy.
      val overrideConfig =
        if c.disableSdkRetries then
          ClientOverrideConfiguration.builder().retryPolicy(SdkRetryPolicy.none())
            .build()
        else ClientOverrideConfiguration.builder().build()

      val builder = KinesisAsyncClient.builder()
        .region(Region.of(awsConfig.region)).httpClient(httpClient)
        .overrideConfiguration(overrideConfig)

      val withCredentials =
        if awsConfig.localstack then
          builder.credentialsProvider(StaticCredentialsProvider.create(
            AwsBasicCredentials.create("test", "test"),
          ))
        else builder.credentialsProvider(DefaultCredentialsProvider.create())

      val withEndpoint = awsConfig.kinesisEndpoint match
        case Some(endpoint) => withCredentials
            .endpointOverride(URI.create(endpoint))
        case None if awsConfig.localstack =>
          withCredentials.endpointOverride(URI.create("http://localhost:4566"))
        case None => withCredentials

      withEndpoint.build()
    }
  }(client => Async[F].delay(client.close()))
