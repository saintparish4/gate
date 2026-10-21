import java.io.{BufferedWriter, FileWriter}
import java.lang.management.ManagementFactory
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import scala.concurrent.duration.*

import cats.effect.*
import cats.effect.std.{Console, Queue}
import cats.syntax.all.*
import com.comcast.ip4s.*
import org.http4s.*
import org.http4s.client.Client
import org.http4s.dsl.io.*
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.implicits.*
import org.typelevel.ci.*

/** Open-loop load: each request is sent at its scheduled time whether or not
  * the earlier ones have been answered, and its latency is counted from that
  * scheduled time.
  *
  * Every other scenario in this simulator is closed-loop: a worker waits for
  * an answer before it sends the next request. When the server slows, a
  * closed loop slows with it and stops measuring the delay its own waiting
  * hides (coordinated omission). That is how "100 RPS" once delivered 75 and
  * still reported a p99 of 262 ms. Here the schedule does not wait, so an
  * overloaded server shows as latency and errors, not as a lower rate.
  *
  * One JSON line per scheduled request, times in milliseconds from the start:
  *   i, stage, rate, intended, sent, done, latency (done - intended),
  *   service (done - sent), status (0 when no answer), error, instance,
  *   degraded, store and gate (the server's Server-Timing split).
  * A request the generator could not send, because `--max-in-flight` were
  * already outstanding, is written with error "generator_capped": a run with
  * any of those measured the generator, not the server.
  *
  * The meta file records the run's span on both the monotonic and the wall
  * clock. On this WSL2 laptop CLOCK_MONOTONIC ran 4.7% slow against an
  * outside clock (20 October 2026), which scales every duration measured here,
  * and gate's own, by the same factor. The analysis rescales by each run's
  * wall/monotonic ratio rather than trusting either clock blindly.
  */
object OpenLoop:

  final case class Stage(rate: Double, seconds: Double)

  /** "25x30,50x30": 25 requests a second for 30 s, then 50 for 30 s. */
  def parseStages(spec: String): Either[String, List[Stage]] = spec.split(',')
    .toList.traverse { part =>
      part.split('x') match
        case Array(r, s) => (r.toDoubleOption, s.toDoubleOption) match
            case (Some(rate), Some(sec)) if rate > 0 && sec > 0 =>
              Right(Stage(rate, sec))
            case _ => Left(s"bad stage '$part'")
        case _ => Left(s"bad stage '$part' (want RATExSECONDS)")
    }

  /** Each request's offset from the start, in nanoseconds, and its stage. */
  def schedule(stages: List[Stage]): Vector[(Long, Int, Double)] =
    val out = Vector.newBuilder[(Long, Int, Double)]
    var stageStart = 0.0
    stages.zipWithIndex.foreach { (st, idx) =>
      val n = math.round(st.rate * st.seconds).toInt
      (0 until n).foreach(k =>
        out += ((((stageStart + k / st.rate) * 1e9).toLong, idx, st.rate)),
      )
      stageStart += st.seconds
    }
    out.result()

  enum Target:
    case GateCheck, DynamoPair, Plain

  private def json(s: String) = "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\""

  private def serverTiming(resp: Response[IO], name: String): Option[Double] =
    resp.headers.get(ci"Server-Timing").flatMap(_.head.value.split(',').map(_.trim)
      .collectFirst { case p if p.startsWith(name + ";dur=") =>
        p.stripPrefix(name + ";dur=").toDoubleOption
      }.flatten)

  def run(args: List[String], apiKey: String): IO[ExitCode] =
    val url = Args.string(args, "--url", "http://localhost:8080")
    val out = Args.string(args, "--out", "loadsim-results/openloop.jsonl")
    val maxInFlight = Args.intOpt(args, "--max-in-flight").getOrElse(4000)
    val keys = Args.intOpt(args, "--keys").getOrElse(10_000)
    val runId = Args.string(args, "--run-id", System.currentTimeMillis().toString)
    val timeoutS = Args.intOpt(args, "--timeout-seconds").getOrElse(30)
    val target = Args.string(args, "--target", "gate") match
      case "dynamodb" => Target.DynamoPair
      case "plain" => Target.Plain
      case _ => Target.GateCheck
    parseStages(Args.string(args, "--stages", "50x30")) match
      case Left(err) => Console[IO].errorln(err).as(ExitCode.Error)
      case Right(stages) =>
        val plan = schedule(stages)
        EmberClientBuilder.default[IO].withMaxTotal(maxInFlight)
          .withMaxPerKey(_ => maxInFlight)
          .withIdleConnectionTime(60.seconds).withTimeout(timeoutS.seconds).build
          .use(client => execute(client, url, target, plan, out, maxInFlight, keys, runId, apiKey))

  private def request(target: Target, url: String, key: String, apiKey: String, table: String): List[Request[IO]] =
    val base = Uri.unsafeFromString(url)
    target match
      case Target.GateCheck => List(
          Request[IO](Method.POST, base / "v1" / "ratelimit" / "check")
            .withEntity(s"""{"key":${json(key)},"cost":1}""")
            .putHeaders(
              Header.Raw(ci"Content-Type", "application/json"),
              Header.Raw(ci"Authorization", s"Bearer $apiKey"),
            ),
        )
      case Target.Plain => List(Request[IO](Method.GET, base / "work"))
      // What a rate-limit check asks of its store: a consistent read, then a
      // conditional write. LocalStack does not check signatures.
      case Target.DynamoPair =>
        def ddb(op: String, body: String) = Request[IO](Method.POST, base)
          .withEntity(body).putHeaders(
            Header.Raw(ci"Content-Type", "application/x-amz-json-1.0"),
            Header.Raw(ci"X-Amz-Target", s"DynamoDB_20120810.$op"),
            Header.Raw(ci"Authorization", "AWS4-HMAC-SHA256 Credential=test/20261020/us-east-1/dynamodb/aws4_request, SignedHeaders=host, Signature=0"),
          )
        val pk = s"""{"pk":{"S":${json(key)}}}"""
        List(
          ddb("GetItem", s"""{"TableName":"$table","Key":$pk,"ConsistentRead":true}"""),
          ddb("UpdateItem", s"""{"TableName":"$table","Key":$pk,"UpdateExpression":"ADD n :one","ExpressionAttributeValues":{":one":{"N":"1"}}}"""),
        )

  private def execute(
      client: Client[IO],
      url: String,
      target: Target,
      plan: Vector[(Long, Int, Double)],
      out: String,
      maxInFlight: Int,
      keys: Int,
      runId: String,
      apiKey: String,
  ): IO[ExitCode] =
    val inFlight = AtomicInteger(0)
    val capped = AtomicLong(0)
    val maxLagNs = AtomicLong(0)
    val cpu = ManagementFactory.getOperatingSystemMXBean
      .asInstanceOf[com.sun.management.OperatingSystemMXBean]
    for
      lines <- Queue.unbounded[IO, Option[String]]
      writer <- Resource.fromAutoCloseable(IO(BufferedWriter(FileWriter(out), 1 << 20)))
        .use(w => lines.take.flatMap {
          case Some(l) => IO.blocking { w.write(l); w.write('\n') }.as(true)
          case None => IO.pure(false)
        }.iterateWhile(identity)).start
      cpu0 <- IO(cpu.getProcessCpuTime)
      wall0 <- IO.realTime
      t0 <- IO.monotonic.map(_.toNanos)
      _ <- Console[IO].println(s"open-loop: ${plan.size} requests over ${plan.lastOption.fold(0L)(_._1) / 1e9} s to $url (run $runId, wall start ${wall0.toMillis})")
      _ <- plan.zipWithIndex.traverse_ { case ((offset, stage, rate), i) =>
        IO.monotonic.map(_.toNanos - t0).flatMap { now =>
          val wait = offset - now
          (if wait > 0 then IO.sleep(wait.nanos) else IO.unit) *> IO.monotonic
            .map(_.toNanos - t0).flatMap { sent =>
              maxLagNs.accumulateAndGet(sent - offset, math.max)
              def line(done: Long, status: Int, error: Option[String], inst: Option[String], degraded: Boolean, store: Option[Double], gate: Option[Double]) =
                f"""{"i":$i,"stage":$stage,"rate":$rate,"intended":${offset / 1e6}%.3f,"sent":${sent / 1e6}%.3f,"done":${done / 1e6}%.3f,"latency":${(done - offset) / 1e6}%.3f,"service":${(done - sent) / 1e6}%.3f,"status":$status,"error":${error.fold("null")(json)},"instance":${inst.fold("null")(json)},"degraded":$degraded,"store":${store.fold("null")(d => f"$d%.3f")},"gate":${gate.fold("null")(d => f"$d%.3f")}}"""
              if inFlight.get >= maxInFlight then
                IO(capped.incrementAndGet()) *> lines.offer(Some(line(sent, 0, Some("generator_capped"), None, false, None, None)))
              else
                val key = s"perf:$runId:${i % keys}"
                val call = request(target, url, key, apiKey, "rate-limits")
                  .foldLeft(IO.pure(Option.empty[Response[IO]] -> 0)) { (prev, req) =>
                    prev.flatMap(_ => client.run(req).use(r => r.body.compile.drain.as(Some(r) -> r.status.code)))
                  }
                (IO(inFlight.incrementAndGet()) *> call.attempt.flatMap { result =>
                  IO.monotonic.map(_.toNanos - t0).flatMap { done =>
                    val l = result match
                      case Right((resp, code)) => line(done, code, None,
                          resp.flatMap(_.headers.get(ci"X-Gate-Instance").map(_.head.value)),
                          resp.exists(_.headers.get(ci"X-Gate-Degraded").isDefined),
                          resp.flatMap(serverTiming(_, "store")),
                          resp.flatMap(serverTiming(_, "gate")))
                      case Left(e) =>
                        val cls = e match
                          case _: java.util.concurrent.TimeoutException => "client_timeout"
                          case _: java.net.ConnectException => "connect_refused"
                          case _ => e.getClass.getSimpleName
                        line(done, 0, Some(cls), None, false, None, None)
                    lines.offer(Some(l))
                  }
                }.guarantee(IO(inFlight.decrementAndGet()).void)).start.void
            }
        }
      }
      // Let the last requests finish, bounded by the client timeout.
      _ <- IO.sleep(200.millis).whileM_(IO(inFlight.get > 0))
      _ <- lines.offer(None) *> writer.join
      cpu1 <- IO(cpu.getProcessCpuTime)
      elapsed <- IO.monotonic.map(_.toNanos - t0)
      wall1 <- IO.realTime
      cores = Runtime.getRuntime.availableProcessors
      meta = f"""{"run_id":${json(runId)},"url":${json(url)},"target":${json(target.toString)},"requests":${plan.size},"wall_start_ms":${wall0.toMillis},"elapsed_s":${elapsed / 1e9}%.3f,"wall_end_ms":${wall1.toMillis},"wall_elapsed_s":${(wall1 - wall0).toMillis / 1e3}%.3f,"generator_cpu_s":${(cpu1 - cpu0) / 1e9}%.3f,"generator_cores":$cores,"max_send_lag_ms":${maxLagNs.get / 1e6}%.3f,"generator_capped":${capped.get}}"""
      _ <- IO.blocking(java.nio.file.Files.writeString(java.nio.file.Path.of(out + ".meta.json"), meta + "\n"))
      _ <- Console[IO].println(s"open-loop done: $meta")
    yield if capped.get > 0 then ExitCode(2) else ExitCode.Success

  /** A server that answers every request after a fixed delay, to prove the
    * generator measures what it claims (`--scenario fake-server`).
    */
  def fakeServer(args: List[String]): IO[ExitCode] =
    val port = Args.intOpt(args, "--port").getOrElse(9099)
    val delay = Args.intOpt(args, "--delay-ms").getOrElse(50)
    val routes = HttpRoutes.of[IO] { case _ => IO.sleep(delay.millis) *> Ok("{}") }
    EmberServerBuilder.default[IO].withHost(host"0.0.0.0")
      .withPort(Port.fromInt(port).get).withMaxConnections(8192)
      .withHttpApp(routes.orNotFound).build
      .use(_ => Console[IO].println(s"fake server on :$port, $delay ms per request") *> IO.never)
