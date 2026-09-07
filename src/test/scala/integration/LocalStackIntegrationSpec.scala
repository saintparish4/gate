package integration

import scala.jdk.CollectionConverters.*

import org.scalatest.{BeforeAndAfterAll, Suite}
import org.testcontainers.containers.localstack.LocalStackContainer
import org.testcontainers.containers.localstack.LocalStackContainer.Service
import org.testcontainers.utility.DockerImageName

import config.{AwsConfig, DynamoDBConfig, KinesisConfig}
import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Resource}
import software.amazon.awssdk.auth.credentials.{
  AwsBasicCredentials, StaticCredentialsProvider,
}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.*
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.{
  CreateStreamRequest, DescribeStreamRequest as KinesisDescribeStreamRequest,
  StreamStatus,
}

/** Base trait for integration tests using LocalStack
  *
  * Provides:
  *   - LocalStack container (started once per test class)
  *   - DynamoDB and Kinesis async clients
  *   - Helper methods to create tables and streams
  *   - Automatic cleanup after tests
  */
trait LocalStackIntegrationSpec extends BeforeAndAfterAll {
  self: Suite =>

  // I pin the image because `latest` (2026.x) refuses to start without a paid auth token.
  protected lazy val localstack: LocalStackContainer = {
    val container = new LocalStackContainer(
      DockerImageName.parse(LocalStackIntegrationSpec.Image),
    )
    container.withServices(Service.DYNAMODB, Service.KINESIS)
    container
  }

  // Test configuration
  protected lazy val testAwsConfig: AwsConfig = AwsConfig(
    region = localstack.getRegion,
    localstack = true,
    dynamodbEndpoint =
      Some(localstack.getEndpointOverride(Service.DYNAMODB).toString),
    kinesisEndpoint =
      Some(localstack.getEndpointOverride(Service.KINESIS).toString),
  )

  protected lazy val testDynamoDBConfig: DynamoDBConfig = DynamoDBConfig(
    rateLimitTable = "test-rate-limits",
    idempotencyTable = "test-idempotency",
  )

  protected lazy val testKinesisConfig: KinesisConfig =
    KinesisConfig(streamName = "test-rate-limit-events", enabled = true)

  // AWS Clients
  protected lazy val dynamoDbClient: DynamoDbAsyncClient = DynamoDbAsyncClient
    .builder().region(Region.of(localstack.getRegion))
    .endpointOverride(localstack.getEndpointOverride(Service.DYNAMODB))
    .credentialsProvider(StaticCredentialsProvider.create(
      AwsBasicCredentials
        .create(localstack.getAccessKey, localstack.getSecretKey),
    )).build()

  protected lazy val kinesisClient: KinesisAsyncClient = KinesisAsyncClient
    .builder().region(Region.of(localstack.getRegion))
    .endpointOverride(localstack.getEndpointOverride(Service.KINESIS))
    .credentialsProvider(StaticCredentialsProvider.create(
      AwsBasicCredentials
        .create(localstack.getAccessKey, localstack.getSecretKey),
    )).build()

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    localstack.start()
    setupResources()
  }

  override protected def afterAll(): Unit = {
    cleanupResources()
    dynamoDbClient.close()
    kinesisClient.close()
    localstack.stop()
    super.afterAll()
  }

  /** Set up DynamoDB tables and Kinesis streams for testing.
    */
  protected def setupResources(): Unit = {
    // Create rate-limits table
    createDynamoDBTable(testDynamoDBConfig.rateLimitTable)

    // Create idempotency table
    createDynamoDBTable(testDynamoDBConfig.idempotencyTable)

    // Create Kinesis stream
    createKinesisStream(testKinesisConfig.streamName)
  }

  /** Clean up resources after tests.
    */
  protected def cleanupResources(): Unit = {
    // Tables and streams are automatically cleaned up when container stops
  }

  protected def createDynamoDBTable(tableName: String): Unit = {
    val request = CreateTableRequest.builder().tableName(tableName)
      .attributeDefinitions(
        AttributeDefinition.builder().attributeName("pk")
          .attributeType(ScalarAttributeType.S).build(),
      ).keySchema(
        KeySchemaElement.builder().attributeName("pk").keyType(KeyType.HASH)
          .build(),
      ).billingMode(BillingMode.PAY_PER_REQUEST).build()

    dynamoDbClient.createTable(request).get()
    waitForTableActive(tableName)

    val ttlRequest = UpdateTimeToLiveRequest.builder().tableName(tableName)
      .timeToLiveSpecification(
        TimeToLiveSpecification.builder().attributeName("ttl").enabled(true)
          .build(),
      ).build()
    dynamoDbClient.updateTimeToLive(ttlRequest).get()
  }

  protected def waitForTableActive(tableName: String): Unit = {
    var attempts = 0
    val maxAttempts = 30

    while (attempts < maxAttempts) {
      val request = DescribeTableRequest.builder().tableName(tableName).build()
      val response = dynamoDbClient.describeTable(request).get()

      if (response.table().tableStatus() == TableStatus.ACTIVE) return

      Thread.sleep(1000)
      attempts += 1
    }

    throw new RuntimeException(s"Table $tableName did not become active")
  }

  protected def createKinesisStream(streamName: String): Unit = {
    val request = CreateStreamRequest.builder().streamName(streamName)
      .shardCount(1).build()
    kinesisClient.createStream(request).get()
    waitForStreamActive(streamName)
  }

  protected def waitForStreamActive(streamName: String): Unit = {
    var attempts = 0
    val maxAttempts = 30

    while (attempts < maxAttempts) {
      val request = KinesisDescribeStreamRequest.builder().streamName(streamName)
        .build()
      val response = kinesisClient.describeStream(request).get()

      if (response.streamDescription().streamStatus() == StreamStatus.ACTIVE)
        return

      Thread.sleep(1000)
      attempts += 1
    }

    throw new RuntimeException(s"Stream $streamName did not become active")
  }

  /** Clear all items from a DynamoDB table. Useful for test isolation between
    * test cases.
    */
  protected def clearTable(tableName: String): Unit = {
    val scanRequest = ScanRequest.builder().tableName(tableName).build()

    val items = dynamoDbClient.scan(scanRequest).get().items()

    items.asScala.foreach { item =>
      val deleteRequest = DeleteItemRequest.builder().tableName(tableName)
        .key(Map("pk" -> item.get("pk")).asJava).build()

      dynamoDbClient.deleteItem(deleteRequest).get()
    }
  }
}

object LocalStackIntegrationSpec {
  val Image = "localstack/localstack:4.14.0"
}
