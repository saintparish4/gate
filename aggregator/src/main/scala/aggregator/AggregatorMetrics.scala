package aggregator

import java.io.StringWriter

import io.prometheus.client.*
import io.prometheus.client.exporter.common.TextFormat
import io.prometheus.client.hotspot.DefaultExports

/** The aggregator's own Prometheus metrics. The Grafana SLO dashboard reads
  * `gate_usage_millis_behind_latest` for the pipeline freshness objective
  * (docs/SLO.md).
  */
final class AggregatorMetrics(val registry: CollectorRegistry):

  val records: Counter = Counter.build().name("gate_usage_records_total")
    .help("Stream records committed, by event type (unreadable for records that are not events)")
    .labelNames("event_type").register(registry)

  val commits: Counter = Counter.build().name("gate_usage_commits_total").help(
    "Commit attempts: committed, fenced (the checkpoint had moved), or error",
  ).labelNames("result").register(registry)

  val millisBehind: Gauge = Gauge.build().name("gate_usage_millis_behind_latest")
    .help(
      "How far behind the tip of each shard the last read was, in milliseconds",
    ).labelNames("shard").register(registry)

  val lastCommit: Gauge = Gauge.build()
    .name("gate_usage_last_commit_timestamp_seconds")
    .help("Unix time of the last committed batch").register(registry)

  val tokens: Counter = Counter.build().name("gate_usage_tokens_total")
    .help("Reconciled quota tokens committed to usage, by direction")
    .labelNames("direction").register(registry)

  val decisions: Counter = Counter.build().name("gate_usage_decisions_total")
    .help("Rate-limit decisions committed to usage, by result")
    .labelNames("result").register(registry)

  List("committed", "fenced", "error").foreach(commits.labels(_))
  List("input", "output").foreach(tokens.labels(_))
  List("allowed", "rejected").foreach(decisions.labels(_))

  /** Counted only after the commit, so these never run ahead of the table. */
  def recordCommitted(
      parsed: List[Parsed],
      deltas: Map[BucketKey, Counts],
  ): Unit =
    parsed.foreach {
      case Parsed.Usage(t, _, _) => records.labels(t).inc()
      case Parsed.Ignored(t) => records.labels(t).inc()
      case Parsed.Unreadable(_) => records.labels("unreadable").inc()
    }
    val total = deltas.values.foldLeft(Counts.zero)(_ + _)
    tokens.labels("input").inc(total.quotaInputTokens.toDouble)
    tokens.labels("output").inc(total.quotaOutputTokens.toDouble)
    decisions.labels("allowed").inc(total.allowed.toDouble)
    decisions.labels("rejected").inc(total.rejected.toDouble)
    lastCommit.set(System.currentTimeMillis() / 1000.0)

  def scrape: String =
    val writer = new StringWriter(8192)
    TextFormat.write004(writer, registry.metricFamilySamples())
    writer.toString

object AggregatorMetrics:
  def create(withJvm: Boolean): AggregatorMetrics =
    val registry = new CollectorRegistry(true)
    if withJvm then DefaultExports.register(registry)
    new AggregatorMetrics(registry)
