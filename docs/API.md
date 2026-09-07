# API Reference

## Overview

The Rate Limiter Platform provides RESTful HTTP APIs for distributed rate limiting and idempotency checking. All endpoints return JSON responses and use standard HTTP status codes.

**Base URL (Local):** `http://localhost:8080`  
**Base URL (AWS):** `http://<load-balancer-dns>` (not yet tested)  
**API Version:** v1  
**Content-Type:** `application/json`

**Note:** Currently only local development (docker-compose) is tested. AWS deployment documentation exists but has not been validated.

---

## Authentication

All API requests require authentication via API key.

**Header:**
```
Authorization: Bearer <api-key>
```

**Error Response (401):**
```json
{
  "error": "unauthorized",
  "message": "Invalid or missing API key"
}
```

---

## Rate Limit Endpoints

### Check Rate Limit

Checks if a request is allowed under the configured rate limit.

**Endpoint:** `POST /v1/ratelimit/check`

**Request:**
```json
{
  "key": "user:12345",
  "cost": 1
}
```

**Request Fields:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `key` | string | Yes | Unique identifier for rate limit bucket (e.g., user ID, API key) |
| `cost` | integer | No | Number of tokens to consume (default: 1) |
| `profile` | string | No | Rate limit profile name (optional, uses tier-based defaults) |
| `endpoint` | string | No | Endpoint identifier for tracking (optional) |

**Note:** The `algorithm` field is not currently used - only token bucket algorithm is implemented.

**Success Response (200):**
```json
{
  "allowed": true,
  "tokensRemaining": 95,
  "limit": 100,
  "resetAt": "2024-01-15T10:30:00Z",
  "retryAfter": null
}
```

**Rate Limited Response (429):**
```json
{
  "allowed": false,
  "tokensRemaining": null,
  "limit": 100,
  "resetAt": "2024-01-15T10:30:00Z",
  "retryAfter": 5,
  "message": "Rate limit exceeded"
}
```

**Response Headers (429):**
```
HTTP/1.1 429 Too Many Requests
Retry-After: 5
```

**Response Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `allowed` | boolean | Whether the request is allowed |
| `tokensRemaining` | integer | Number of tokens remaining in bucket (null if rejected) |
| `limit` | integer | Maximum capacity of the rate limit bucket |
| `resetAt` | string (ISO 8601) | When the rate limit will reset |
| `retryAfter` | integer | Seconds to wait before retrying (null if allowed) |
| `message` | string | Optional message (typically present when rejected) |

**Example cURL (Local Development):**
```bash
curl -X POST http://localhost:8080/v1/ratelimit/check \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer test-api-key" \
  -d '{
    "key": "user:12345",
    "cost": 1
  }'
```

---

### Get Rate Limit Status

Retrieves current status of a rate limit bucket without consuming tokens.

**Endpoint:** `GET /v1/ratelimit/status/{key}`

**Path Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `key` | string | Rate limit bucket identifier |

**Success Response (200):**
```json
{
  "key": "user:12345",
  "tokensRemaining": 95,
  "limit": 100,
  "resetAt": "2024-01-15T10:30:00Z"
}
```

**Example cURL (Local Development):**
```bash
curl http://localhost:8080/v1/ratelimit/status/user:12345 \
  -H "Authorization: Bearer test-api-key"
```

---

## Idempotency Endpoints

### Check Idempotency Key

Checks if an operation has been performed before using idempotency keys. Implements first-writer-wins semantics.

**Endpoint:** `POST /v1/idempotency/check`

**Request:**
```json
{
  "idempotencyKey": "payment:abc-123",
  "ttl": 86400
}
```

**Request Fields:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `idempotencyKey` | string | Yes | Unique key for the operation |
| `ttl` | integer | No | Time-to-live in seconds (default: 86400 / 24 hours) |

**Note:** Metadata field is not currently supported in the implementation.

**New Operation Response (200):**
```json
{
  "status": "new",
  "idempotencyKey": "payment:abc-123"
}
```

**Duplicate Operation Response (200):**
```json
{
  "status": "duplicate",
  "idempotencyKey": "payment:abc-123",
  "originalResponse": {
    "statusCode": 201,
    "body": "{\"paymentId\": \"pay_xyz789\", \"amount\": 100.00}",
    "headers": {
      "Content-Type": "application/json"
    }
  },
  "firstSeenAt": "2024-01-15T10:20:00Z"
}
```

**In Progress Response (202):**
```json
{
  "status": "in_progress",
  "idempotencyKey": "payment:abc-123",
  "firstSeenAt": "2024-01-15T10:20:00Z",
  "message": "Operation is currently being processed"
}
```

**Response Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `status` | string | `new`, `duplicate`, or `in_progress` |
| `idempotencyKey` | string | The idempotency key |
| `originalResponse` | object | Previous response (only for duplicates) |
| `firstSeenAt` | string (ISO 8601) | When the key was first seen (for duplicates/in_progress) |
| `message` | string | Optional message (for in_progress status) |

**Example cURL:**
```bash
curl -X POST http://localhost:8080/v1/idempotency/check \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer test-api-key" \
  -d '{
    "idempotencyKey": "payment:abc-123",
    "ttl": 86400
  }'
```

---

### Complete Idempotency Operation

Stores the response for a completed idempotent operation. Call this after successfully completing an operation.

**Endpoint:** `POST /v1/idempotency/{key}/complete`

**Path Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `key` | string | The idempotency key |

**Request:**
```json
{
  "statusCode": 201,
  "body": "{\"paymentId\": \"pay_xyz789\", \"amount\": 100.00}",
  "headers": {
    "Content-Type": "application/json"
  }
}
```

**Request Fields:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `statusCode` | integer | Yes | HTTP status code of the response |
| `body` | string | Yes | Response body as JSON string |
| `headers` | object | No | Response headers (optional) |

**Success Response (200):**
```json
{
  "status": "completed",
  "idempotencyKey": "payment:abc-123"
}
```

**Example cURL:**
```bash
curl -X POST http://localhost:8080/v1/idempotency/payment:abc-123/complete \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer test-api-key" \
  -d '{
    "statusCode": 201,
    "body": "{\"paymentId\": \"pay_xyz789\"}",
    "headers": {"Content-Type": "application/json"}
  }'
```

---

## Health & Monitoring Endpoints

### Health Check

Returns service health status.

**Endpoint:** `GET /health`

**Success Response (200):**
```json
{
  "status": "healthy",
  "version": "0.2.0"
}
```

**Note:** The health endpoint always returns 200 if the service is running. It does not check dependencies.

---

### Readiness Check

Checks if service is ready to accept traffic (validates dependencies).

**Endpoint:** `GET /ready`

**Success Response (200):**
```json
{
  "status": "ready",
  "checks": {
    "dynamodb_ratelimit": true,
    "dynamodb_idempotency": true,
    "kinesis": true
  }
}
```

**Not Ready Response (503):**
```json
{
  "status": "not ready",
  "checks": {
    "dynamodb_ratelimit": true,
    "dynamodb_idempotency": true,
    "kinesis": false
  }
}
```

---

### Prometheus Metrics

Scrape endpoint for Prometheus-compatible metrics.

**Endpoint:** `GET /metrics`

Returns metrics in Prometheus text exposition format. Includes counters, gauges, and histograms for rate-limit decisions, idempotency checks, token quotas, DynamoDB latency, circuit breaker state, and event publishing. See the main README for the full metric list.

**Response:** `200 OK` with `Content-Type: text/plain`

```bash
curl http://localhost:8080/metrics
```

**Note:** Requires `PROMETHEUS_ENABLED=true` (default). Returns 404 if Prometheus is disabled.

---

## Error Responses

All error responses follow this format:

```json
{
  "error": "error_code",
  "message": "Human-readable error message",
  "details": {
    "field": "Additional context"
  }
}
```

### Standard Error Codes

| Status | Error Code | Description |
|--------|------------|-------------|
| 400 | `bad_request` | Invalid request format or parameters |
| 401 | `unauthorized` | Missing or invalid authentication |
| 404 | `not_found` | Resource not found |
| 429 | `rate_limit_exceeded` | Rate limit exceeded |
| 500 | `internal_error` | Internal server error |
| 503 | `service_unavailable` | Service temporarily unavailable |

**Example Error (400):**
```json
{
  "error": "bad_request",
  "message": "Invalid request body",
  "details": {
    "field": "cost",
    "issue": "must be a positive integer"
  }
}
```

---

## Rate Limit Algorithm

**Response headers and `resetAt`:** The API passes the `resetAt` value from the rate-limit store (token bucket, leaky-bucket, or sliding-window) into the JSON body and the `X-RateLimit-Reset` header. Each store returns the correct reset instant for its algorithm; the API does not need algorithm-specific handling — "epoch seconds at which capacity resets" is algorithm-agnostic.

### Token Bucket

Currently implemented algorithm. Tokens refill at a constant rate.

**Characteristics:**
- Allows bursts up to bucket capacity
- Smooth refill over time
- Best for: APIs with bursty traffic patterns

**Configuration:**
- `capacity`: Maximum tokens (e.g., 100)
- `refillRate`: Tokens per second (e.g., 10.0)

**Behavior:**
- Initial state: Bucket is full
- Each request consumes `cost` tokens
- Tokens refill at `refillRate` per second
- Maximum tokens never exceeds `capacity`

**Note:** Sliding window algorithm is not yet implemented. The `algorithm` field in requests is accepted but currently only token bucket is used.

---

## Best Practices

### 1. Idempotency Keys

Use UUIDs or cryptographically random strings:
```
payment:550e8400-e29b-41d4-a716-446655440000
order:123e4567-e89b-12d3-a456-426614174000
```

### 2. Rate Limit Keys

Structure keys hierarchically:
```
user:12345              # Per-user limit
api_key:abc123          # Per API key
ip:192.168.1.1          # Per IP address
endpoint:/api/search    # Per endpoint
```

### 3. Error Handling

Always check the `allowed` field:
```javascript
const response = await checkRateLimit({
  key: `user:${userId}`,
  cost: 1
});

if (!response.allowed) {
  // Wait for retryAfter seconds
  await sleep(response.retryAfter * 1000);
  // Retry request
}
```

### 4. Retry Logic

Use exponential backoff for retries:
```javascript
async function withRetry(fn, maxRetries = 3) {
  for (let i = 0; i < maxRetries; i++) {
    try {
      return await fn();
    } catch (error) {
      if (error.status === 429) {
        const delay = Math.min(1000 * Math.pow(2, i), 10000);
        await sleep(delay);
      } else {
        throw error;
      }
    }
  }
  throw new Error('Max retries exceeded');
}
```

### 5. Monitoring

Track these metrics:
- Rate limit hit rate (rejected / total)
- P50/P95/P99 latency
- Idempotency duplicate rate
- Error rates by type

**Note:** SDKs and client libraries are not yet implemented. Use HTTP client libraries (curl, http4s, requests, etc.) to interact with the API.

---

## Versioning

The API uses URL-based versioning (e.g., `/v1/`). Breaking changes will increment the version number.

**Current Version:** v1  
**Stability:** Stable

---

## Support

**Issues:** [GitHub Issues](https://github.com/saintparish4/keyra/issues)  

**Note:** Status page and production documentation are not yet available. This API is currently in development and tested locally only.