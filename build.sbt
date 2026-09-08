val scala3Version = "3.7.4"
val Http4sVersion = "0.23.32"
val CirceVersion = "0.14.15"
val CatsEffectVersion = "3.6.3"
val AwsSdkVersion = "2.38.7"
val PrometheusVersion = "0.16.0" 
val Otel4sVersion = "0.15.2"
val OtelSdkVersion = "1.60.1" 

libraryDependencies ++= Seq(
  // HTTP4s
  "org.http4s" %% "http4s-ember-server" % Http4sVersion,
  "org.http4s" %% "http4s-ember-client" % Http4sVersion,
  "org.http4s" %% "http4s-circe" % Http4sVersion,
  "org.http4s" %% "http4s-dsl" % Http4sVersion,

  // JSON
  "io.circe" %% "circe-generic" % CirceVersion,
  "io.circe" %% "circe-parser" % CirceVersion,

  // Cats Effect
  "org.typelevel" %% "cats-effect" % CatsEffectVersion,
  "org.typelevel" %% "cats-effect-std" % CatsEffectVersion,

  // AWS SDK
  "software.amazon.awssdk" % "dynamodb" % AwsSdkVersion,
  "software.amazon.awssdk" % "kinesis" % AwsSdkVersion,
  "software.amazon.awssdk" % "cloudwatch" % AwsSdkVersion,
  "software.amazon.awssdk" % "secretsmanager" % AwsSdkVersion,

  // Config
  "com.github.pureconfig" %% "pureconfig-core" % "0.17.9",

  // Caching
  "com.github.ben-manes.caffeine" % "caffeine" % "3.1.8",

  // Logging
  "ch.qos.logback" % "logback-classic" % "1.5.19",
  "org.typelevel" %% "log4cats-slf4j" % "2.7.1",

  // Testing
  "org.scalatest" %% "scalatest" % "3.2.19" % Test,
  "org.scalatestplus" %% "scalacheck-1-18" % "3.2.19.0" % Test,
  "org.scalacheck" %% "scalacheck" % "1.18.1" % Test, 
  "org.typelevel" %% "cats-effect-testing-scalatest" % "1.7.0" % Test,
  "org.typelevel" %% "cats-effect-testkit"            % CatsEffectVersion % Test,
  "org.testcontainers" % "localstack" % "1.21.4" % Test,

   // Prometheus metrics
  "io.prometheus" % "simpleclient" % PrometheusVersion,
  "io.prometheus" % "simpleclient_common" % PrometheusVersion,
  "io.prometheus" % "simpleclient_hotspot" % PrometheusVersion,

  // OpenTelemetry / otel4s
  "org.typelevel" %% "otel4s-oteljava" % Otel4sVersion,
  "io.opentelemetry" % "opentelemetry-exporter-otlp" % OtelSdkVersion % Runtime,
  "io.opentelemetry" % "opentelemetry-sdk-extension-autoconfigure" % OtelSdkVersion % Runtime,
)
javaOptions += "-Dotel.java.global-autoconfigure.enabled=true"
// Docker plugin
enablePlugins(JavaAppPackaging, DockerPlugin)

dockerBaseImage := "eclipse-temurin:17-jre"
dockerExposedPorts := Seq(8080)

lazy val root = project.in(file(".")).settings(
  name := "gate",
  version := "0.1.0-SNAPSHOT",
  scalaVersion := scala3Version,
  libraryDependencies += "org.scalameta" %% "munit" % "1.0.0" % Test,
  scalacOptions += "-Xkind-projector",
  semanticdbEnabled := true,
)

// Run unit tests only (excludes integration tests that require Docker/LocalStack).
// Note: for the rest of the sbt session, "test" will also exclude integration until you reload.
addCommandAlias(
  "unitTest",
  """set Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-l", "integration.Integration") ; test""",
)

lazy val loadSim = project.in(file("loadSim")).settings(
  name := "gate-load-sim",
  scalaVersion := scala3Version,
  // Forked so the simulator's exit code reaches sbt. Unforked, an invariant
  // that reports ExitCode.Error still ends the task with [success], which
  // makes the CI correctness job green no matter what it measured.
  Compile / run / fork := true,
  libraryDependencies ++= Seq(
    "org.typelevel" %% "cats-effect" % CatsEffectVersion,
    "org.typelevel" %% "cats-effect-std" % CatsEffectVersion,
    "org.http4s" %% "http4s-ember-client" % Http4sVersion,
    "org.http4s" %% "http4s-circe" % Http4sVersion,
    "io.circe" %% "circe-generic" % CirceVersion,
    "io.circe" %% "circe-parser" % CirceVersion,
    "ch.qos.logback" % "logback-classic" % "1.5.19" % Runtime,
    "org.hdrhistogram" % "HdrHistogram" % "2.2.2",
  ),
)
