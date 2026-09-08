# Gate Compliance Documentation — PCI DSS 4.0.1

## Audit Trail Architecture

Every denied request (rate limit rejected, idempotency key conflict, token quota
exceeded) produces an `AuditEvent` that is:

1. Logged as a structured INFO line (for CloudWatch Logs / stdout)
2. Published to Kinesis as a JSON event (partition key = clientId)

**Planned but not yet implemented:**

3. S3 archival via Kinesis Firehose in Parquet format (Snappy compressed). Terraform modules exist (`terraform/modules/kinesis/main.tf`, gated behind `enable_kinesis_firehose` and `enable_audit_compliance` flags) but the pipeline has not been deployed or validated end-to-end.
4. 7-year retention in S3 (90 days Standard → Deep Archive until expiry). The S3 lifecycle configuration is defined in Terraform but has not been activated.

### Audit Event Schema

| Field       | Type            | Description                               |
|-------------|-----------------|-------------------------------------------|
| timestamp   | ISO-8601 string | When the decision was made                |
| event_type  | string          | Always `"audit"`                          |
| request_id  | UUID string     | Unique ID for this audit record           |
| api_key     | string          | API key that made the request             |
| client_id   | string          | Client identifier                         |
| decision    | string          | `rejected`, `conflict`, `quota_exceeded`  |
| reason      | string          | Human-readable denial reason              |
| endpoint    | string (opt)    | Request endpoint if available             |
| source_ip   | string (opt)    | Client IP if available                    |
| tier        | string (opt)    | Client tier (free/basic/premium/etc)      |
| trace_id    | string (opt)    | OpenTelemetry trace ID for correlation    |

### S3 Partitioning (Planned)

The Terraform configuration defines the following S3 layout for when Firehose delivery is enabled:

```
s3://bucket/audit/year=2026/month=03/day=09/part-00000.snappy.parquet
```

This is not currently active. To enable, set `enable_kinesis_firehose = true` and `enable_audit_compliance = true` in Terraform variables, and deploy.

### What Works Today

- Every 429 response produces an audit event published to Kinesis within 1 second
- Audit events are logged as structured INFO lines to stdout (CloudWatch Logs when deployed)
- Events include all schema fields above for post-hoc querying

### What Does Not Work Yet

- S3 export via Kinesis Firehose (Terraform exists, not deployed)
- Parquet format conversion via Glue catalog (Terraform exists, not deployed)
- Athena queryability over S3 data
- 7-year S3 retention lifecycle enforcement
- Deep Archive transition after 90 days

## CloudWatch Insights Queries

These queries work against CloudWatch Logs (structured log output). They do **not** require the S3/Firehose/Athena pipeline.

### All denied requests in the last 24 hours

```
fields @timestamp, decision, client_id, reason, endpoint, trace_id
| filter @message like /AUDIT decision=/
| sort @timestamp desc
| limit 1000
```

### Denied requests by client

```
fields @timestamp, decision, client_id, reason
| filter @message like /AUDIT decision=/
| stats count(*) as denials by client_id
| sort denials desc
```

### Rate limit rejections by tier

```
fields @timestamp, client_id, tier
| filter @message like /AUDIT decision=rejected/
| stats count(*) as rejections by tier
| sort rejections desc
```

### Idempotency conflicts

```
fields @timestamp, client_id, reason
| filter @message like /AUDIT decision=conflict/
| sort @timestamp desc
| limit 500
```

### Token quota exceeded events by user

```
fields @timestamp, client_id, reason
| filter @message like /AUDIT decision=quota_exceeded/
| stats count(*) as exceeded by client_id
| sort exceeded desc
```

### Correlation: trace a single denied request end-to-end

```
fields @timestamp, @message
| filter trace_id = "TRACE_ID_HERE"
| sort @timestamp asc
```

### Daily denial rate trend (last 30 days)

```
fields @timestamp, decision
| filter @message like /AUDIT decision=/
| stats count(*) as denials by bin(1d) as day
| sort day asc
```

## PCI DSS 4.0.1 Evidence

| Requirement | Control                          | Evidence                                                                    | Status |
|-------------|----------------------------------|-----------------------------------------------------------------------------|--------|
| 10.2.2     | Log all access to cardholder data | AuditEvent on every denied req (Kinesis + structured log)                  | Implemented |
| 10.2.4     | Invalid logical access attempts  | `decision=rejected/conflict` in audit events                               | Implemented |
| 10.3       | Record audit trail entries        | JSON schema with all fields (see schema above)                             | Implemented |
| 10.5       | Secure audit trails               | S3 SSE-AES256 + versioning defined in Terraform; **not yet deployed**      | Planned |
| 10.7       | Retain audit trail >= 1 year     | 7-year S3 lifecycle defined in Terraform; **not yet deployed**. Currently, retention depends on CloudWatch Logs retention settings and Kinesis stream retention (default 24h). | Planned |
| 10.7.b    | Available for analysis >= 3 mo   | 90 days Standard before Glacier defined in Terraform; **not yet deployed** | Planned |
