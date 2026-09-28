# Gate Compliance Documentation — PCI DSS 4.0.1

## Audit Trail Architecture

Three denials produce an `AuditEvent`:

| Denial | Status | `decision` | Log line |
|--------|--------|------------|----------|
| Rate-limit check rejected | 429 | `rejected` | `AUDIT decision=rejected client=<clientId> key=<key> tier=<tier>` |
| Token quota exceeded | 429 | `quota_exceeded` | `AUDIT decision=quota_exceeded user=<userId> level=<level> used=<used>/<limit>` |
| Idempotency request-hash conflict | 409 | `conflict` | `AUDIT decision=conflict client=<apiKeyId> idempotencyKey=<key>` |

Each one is:

1. Logged as a plain-text INFO line on stdout (CloudWatch Logs when deployed). The log
   pattern (`logback.xml`) is `HH:mm:ss.SSS LEVEL logger - message`, not JSON.
2. Enqueued for Kinesis as a JSON event (partition key = the event's `clientId`). Delivery
   is at-most-once: a full queue evicts its oldest event, and a publish that fails twice
   is dropped. Both are counted as `DroppedKinesisEvent` ([ADR-003](adr/003-fire-and-forget-event-publishing.md)).

Other refusals are not audit events:

- A profile above the client's tier (403 `profile_not_permitted`) logs
  `AUDIT decision=profile_refused client=<clientId> profile=<name> tier=<tier>` at WARN,
  with no Kinesis event.
- An unknown API key (401) logs `Invalid API key attempted: <masked key>` at WARN.
- A missing API key (401) is logged at DEBUG only, so not at the default level.
- A key without the route's permission (403) and the auth throttle (429) are not recorded.

**Planned but not yet implemented:**

3. S3 archival via Kinesis Firehose in Parquet format (Snappy compressed). Terraform modules exist (`terraform/modules/kinesis/main.tf`, gated behind `enable_kinesis_firehose` and `enable_audit_compliance` flags) but the pipeline has not been deployed or validated end-to-end.
4. 7-year retention in S3 (90 days Standard → Deep Archive until expiry). The S3 lifecycle configuration is defined in Terraform but has not been activated.

### Audit Event Schema

Encoded by `events/Events.scala` with circe generic derivation, so field names are the
case-class names (camelCase). `event_type` and, when a trace is active, `trace_id` are
merged in. An absent optional field encodes as `null`.

| Field       | Type            | Description                               |
|-------------|-----------------|-------------------------------------------|
| timestamp   | ISO-8601 string | When the decision was made                |
| event_type  | string          | Always `"audit"`                          |
| requestId   | string          | The request's ID                          |
| apiKey      | string          | The API key's ID (`apiKeyId`), never the key itself |
| clientId    | string          | The client's ID on rate-limit events; the API key's ID on quota and idempotency events |
| decision    | string          | `rejected`, `conflict`, or `quota_exceeded` |
| reason      | string          | Human-readable denial reason              |
| endpoint    | string or null  | Request endpoint if the caller sent one; `/v1/quota/check` for quota |
| sourceIp    | null            | Not populated                             |
| tier        | string or null  | Client tier, on rate-limit events only    |
| traceId     | string or null  | OpenTelemetry trace ID, also merged as `trace_id` |

### S3 Partitioning (Planned)

The Terraform configuration defines the following S3 layout for when Firehose delivery is enabled:

```
s3://bucket/audit/year=2026/month=03/day=09/part-00000.snappy.parquet
```

This is not currently active. To enable, set `enable_kinesis_firehose = true` and `enable_audit_compliance = true` in Terraform variables, and deploy.

### What Works Today

- A rejected rate-limit check, an exceeded quota, and an idempotency conflict each log an
  `AUDIT` line and enqueue an audit event for Kinesis, at most once.
- The Kinesis stream keeps records for its retention period (24 hours by default); nothing
  consumes them into durable storage yet.
- CloudWatch Logs keeps the `AUDIT` lines for the log group's retention: 7 days, or 30 in `prod`.

### What Does Not Work Yet

- S3 export via Kinesis Firehose (Terraform exists, not deployed)
- Parquet format conversion via Glue catalog (Terraform exists, not deployed)
- Athena queryability over S3 data
- 7-year S3 retention lifecycle enforcement
- Deep Archive transition after 90 days
- Audit records for failed authentication and permission refusals

## CloudWatch Insights Queries

These run against CloudWatch Logs and do **not** require the S3/Firehose/Athena pipeline.
The log lines are plain text, so each query parses the fields it needs out of `@message`.
They follow the line formats above, but have not yet been run against a deployed stack.
The log lines carry no trace ID; to correlate by trace, use the Kinesis event's `traceId`.

### All denials in the last 24 hours

```
fields @timestamp, @message
| filter @message like /AUDIT decision=/
| parse @message /AUDIT decision=(?<decision>\S+)/
| sort @timestamp desc
| limit 1000
```

### Denials by client (rate-limit rejections, profile refusals, idempotency conflicts)

```
fields @timestamp
| filter @message like /AUDIT decision=/ and @message like / client=/
| parse @message /AUDIT decision=(?<decision>\S+) client=(?<client>\S+)/
| stats count(*) as denials by client, decision
| sort denials desc
```

### Rate limit rejections by tier

```
fields @timestamp
| filter @message like /AUDIT decision=rejected /
| parse @message /tier=(?<tier>\S+)/
| stats count(*) as rejections by tier
| sort rejections desc
```

### Idempotency conflicts

```
fields @timestamp
| filter @message like /AUDIT decision=conflict /
| parse @message /client=(?<client>\S+) idempotencyKey=(?<key>\S+)/
| sort @timestamp desc
| limit 500
```

### Token quota exceeded events by user and level

```
fields @timestamp
| filter @message like /AUDIT decision=quota_exceeded /
| parse @message /user=(?<user>\S+) level=(?<level>\S+)/
| stats count(*) as exceeded by user, level
| sort exceeded desc
```

### Daily denial rate trend (last 30 days)

```
fields @timestamp
| filter @message like /AUDIT decision=/
| stats count(*) as denials by bin(1d) as day
| sort day asc
```

## PCI DSS 4.0.1 Evidence

| Requirement | Control                          | Evidence                                                                    | Status |
|-------------|----------------------------------|-----------------------------------------------------------------------------|--------|
| 10.2.2     | Log all access to cardholder data | Gate does not handle cardholder data. It records its own denials: an `AUDIT` log line plus an at-most-once Kinesis event for rate-limit rejections, quota exhaustion, and idempotency conflicts | Partial |
| 10.2.4     | Invalid logical access attempts  | An unknown API key logs a WARN line with the key masked. Missing keys (DEBUG only), permission refusals, and auth throttling are not recorded, and none produce an audit event | Partial |
| 10.3       | Record audit trail entries        | The audit event schema above; delivery is at-most-once                      | Partial |
| 10.5       | Secure audit trails               | S3 SSE-AES256 + versioning defined in Terraform; **not yet deployed**      | Planned |
| 10.7       | Retain audit trail >= 1 year     | 7-year S3 lifecycle defined in Terraform; **not yet deployed**. Today retention is the CloudWatch Logs retention (7 days, 30 in `prod`) and Kinesis stream retention (24 hours by default). | Planned |
| 10.7.b    | Available for analysis >= 3 mo   | 90 days Standard before Glacier defined in Terraform; **not yet deployed** | Planned |
