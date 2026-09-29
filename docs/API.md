# API Reference

Every route, field, status code and header below is taken from the code under
`src/main/scala`, with defaults from `src/main/resources/application.conf`.

## Overview

- **Base URL (docker compose):** `http://localhost:8080`. The port is
  `SERVER_PORT` (default 8080), and compose publishes it on 8080.
- **Bodies:** request bodies are parsed as JSON whatever the `Content-Type`
  says, but send `Content-Type: application/json`. JSON responses are
  `application/json`.
- **Fields:** outside the demo dashboard, every response field shown below is
  always present. An optional value that does not apply is `null` (responses
  are printed with nulls kept).
- **`X-Request-Id`:** a response produced by a route (including the 401, 403
  and 429 answers from authentication) echoes the request's `X-Request-Id`, or
  a generated UUID when the request had none. The 400/422 body-decoding
  answers, the 404 for an unknown path and any 500 do not carry it.
- **Tenancy:** every key a caller names (rate-limit key, idempotency key,
  quota user/agent/org, reservation ID) is scoped to the authenticated client
  ([ADR-005](adr/005-tenant-namespaced-storage-keys.md)). Two clients using the
  same key string never share state and cannot see each other's.

| Method | Path | Permission (name in the keys secret) | Purpose |
|--------|------|--------------------------------------|---------|
| `POST` | `/v1/ratelimit/check` | `RateLimitCheck` (`ratelimit_check`) | Consume tokens from a bucket |
| `GET` | `/v1/ratelimit/status/{key}` | `RateLimitStatus` (`ratelimit_status`) | Read a bucket without consuming |
| `POST` | `/v1/idempotency/check` | `IdempotencyCheck` (`idempotency_check`) | Claim a key, or learn its state |
| `POST` | `/v1/idempotency/{key}/complete` | `IdempotencyComplete` (`idempotency_complete`) | Store the response to replay |
| `POST` | `/v1/idempotency/{key}/fail` | `IdempotencyComplete` (`idempotency_complete`) | Release a pending key for retry |
| `POST` | `/v1/quota/check` | `QuotaCheck` (`quota_check`) | Reserve estimated LLM tokens |
| `POST` | `/v1/quota/reconcile` | `QuotaReconcile` (`quota_reconcile`) | Replace a reservation's estimate with actual usage |
| `GET` | `/health` | none | Liveness |
| `GET` | `/ready` | none | Readiness |
| `GET` | `/metrics` | `AdminMetrics` (`admin_metrics`) | Prometheus scrape |
| various | `/dashboard`, `/dashboard/api/*`, `/v1/ratelimit/dashboard/stats` | none | Demo dashboard, only with `DASHBOARD_ENABLED=true` ([below](#demo-dashboard)) |

The quota routes exist only with `TOKEN_QUOTA_ENABLED=true` (compose sets it;
the default is `false`).

---

## Authentication

### Sending a key

The middleware reads the key from, in order:

1. `Authorization: Bearer <key>` or `Authorization: ApiKey <key>` (scheme
   matched case-insensitively);
2. `X-Api-Key: <key>`, used when there is no `Authorization` header with one of
   those schemes.

`/health`, `/ready` and the dashboard routes are matched before
authentication and need no key. Every other request goes through it, including
requests for paths that do not exist.

### Outcomes

| Case | Status | Body |
|------|--------|------|
| No key, or an unknown key | `401` | empty |
| Valid key, sending faster than the auth throttle allows | `429` + `Retry-After` | `{"error": "Rate limited", "retryAfter": 42}` |
| Valid key without the route's permission | `403` | `{"error": "forbidden", "message": "Insufficient permissions: QuotaCheck required"}` |
| Valid key, path that no route matches (or wrong method) | `404` | `Not found` (text) |

The permission check runs before the route touches any state. The 403 message
names the permission by its code name (`QuotaCheck`), not its secret name
(`quota_check`).

**Auth throttle.** Each key may authenticate `security.authentication.rate-limit-per-minute`
times per minute (`AUTH_RATE_LIMIT_PER_MINUTE`, default 1000; compose sets
10,000,000 for load tests). The count is per key ID, in memory on each
instance, over a one-minute window that starts at the key's first request.
Past it, the answer is 429, not 401: back off for `Retry-After` seconds, do not
fix credentials. It counts every authenticated request, including ones that
end in 403 or 404.

### Permissions

Each authenticated route needs one permission; the [route table](#overview)
names it and its name in the keys secret. Secret names are matched
case-insensitively, the forms without the underscore (`ratelimitcheck`) are
accepted too, and an unrecognized name is ignored.

### Where keys come from

The service needs one key source, or it refuses to start.

**Secrets Manager** (`SECRETS_MANAGER_ENABLED=true`; Terraform always sets it).
The secret is named `<secret-prefix>/<environment>/<api-keys-secret-name>`
(`SECRETS_PREFIX`, `SECRETS_ENVIRONMENT`, `API_KEYS_SECRET_NAME`; default
`rate-limiter/dev/api-keys`) and holds a JSON list. Every field is required:

```json
[
  {
    "apiKey": "<random secret>",
    "apiKeyId": "key_acme_001",
    "clientName": "Acme",
    "tier": "premium",
    "permissions": ["ratelimit_check", "ratelimit_status", "idempotency_check",
                    "idempotency_complete", "quota_check", "quota_reconcile"],
    "active": true
  }
]
```

- `tier` is `free`, `basic`, `premium` or `enterprise`. An entry with
  `"active": false` is skipped. An unknown tier or permission name, in any
  entry, is an error that names the entry's `apiKeyId` and the value.
- The tenant is the entry's `apiKeyId`. Keep it when you rotate `apiKey` to
  keep the client's state.
- Startup fails if the secret is missing, is not a list in this shape, has an
  unknown tier or permission, or has no active key.
- The keys are re-read about every 5 minutes (`cache-ttl`), so adding or
  revoking a key takes effect within minutes, not at once. If a re-read fails,
  the current keys stay. If it finds an unknown tier or permission, the valid
  entries take effect (so revocations do), the invalid ones cannot
  authenticate, and the error is logged.

**Built-in development keys** (`ALLOW_BUILT_IN_KEYS=true`, set only by docker
compose and `make run`). They are public, so never use them outside local
development. When Secrets Manager is enabled it wins, even with this flag set.

| Key | Tier | Permissions |
|-----|------|-------------|
| `test-api-key` | premium | the six standard permissions (all but `AdminMetrics`) |
| `free-api-key` | free | the six standard permissions |
| `admin-api-key` | enterprise | the six standard permissions and `AdminMetrics` |

---

## Rate limiting

### `POST /v1/ratelimit/check`

**Permission:** `RateLimitCheck`. Consumes `cost` tokens from the caller's
bucket for `key`, or refuses without consuming.

```json
{ "key": "user:12345", "cost": 1 }
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `key` | string | yes | Bucket identifier, scoped to your client |
| `cost` | integer | no, default `1` | Tokens to consume; must be at least 1 |
| `profile` | string | no | A named profile to apply instead of your tier's (see [Profiles](#profiles-and-tiers)) |
| `endpoint` | string | no | A label copied onto the decision event; it does not affect the decision |

**Allowed (200):**

```json
{ "allowed": true, "tokensRemaining": 999, "retryAfter": null, "limit": 1000,
  "resetAt": "2026-09-27T12:00:00.010Z", "message": null }
```

Headers: `X-RateLimit-Limit` (the profile's capacity), `X-RateLimit-Remaining`,
`X-RateLimit-Reset` (`resetAt` as epoch seconds).

**Refused (429):** nothing was consumed. Header: `Retry-After` (seconds, same
as `retryAfter`). No `X-RateLimit-*` headers.

```json
{ "allowed": false, "tokensRemaining": null, "retryAfter": 1, "limit": 1000,
  "resetAt": "2026-09-27T12:00:10.000Z", "message": "Rate limit exceeded" }
```

| Field | Type | Description |
|-------|------|-------------|
| `allowed` | boolean | Whether the tokens were consumed |
| `tokensRemaining` | integer or null | Tokens left after this check; `null` when refused |
| `retryAfter` | integer or null | Seconds until `cost` tokens should be available; `null` when allowed |
| `limit` | integer | Capacity of the profile applied |
| `resetAt` | string (ISO-8601) | When the bucket would be full again, per the configured algorithm |
| `message` | string or null | `"Rate limit exceeded"` when refused |

**Other answers:**

| Status | Body | Cause |
|--------|------|-------|
| `400` | `{"error": "validation_error", "message": "cost must be positive"}` | `cost` below 1 |
| `400` | `{"error": "validation_error", "message": "unknown profile 'gold'"}` | `profile` is not configured |
| `403` | `{"error": "profile_not_permitted", "message": "profile 'enterprise' exceeds the free tier's limits"}` | `profile` is above your tier; nothing consumed |
| `400` / `422` | text | Body not JSON / `key` missing ([Errors](#errors)) |

This route does not answer 503. When the store cannot answer, the result
follows the degradation mode ([below](#when-the-store-cannot-answer)). A
`cost` larger than the profile's capacity is always refused, with a
`retryAfter` that cannot come true.

```bash
curl -s -X POST http://localhost:8080/v1/ratelimit/check \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer test-api-key" \
  -d '{"key": "user:12345", "cost": 1}'
```

### `GET /v1/ratelimit/status/{key}`

**Permission:** `RateLimitStatus`. Reads the caller's bucket for `key` without
consuming. Percent-encode reserved characters in `key`; the path segment is
decoded before use.

**200:**

```json
{ "key": "user:12345", "tokensRemaining": 997, "limit": 1000, "resetAt": "2026-09-27T12:00:01Z" }
```

| Field | Type | Description |
|-------|------|-------------|
| `key` | string | The key as you sent it |
| `tokensRemaining` | integer | Tokens in the bucket now |
| `limit` | integer | Your tier's capacity |
| `resetAt` | string (ISO-8601) | Now plus the seconds to refill to capacity at your tier's rate |

Status always uses your tier's profile, even if checks on this key named a
narrower one. A key never seen reads as full, with `resetAt` 60 seconds from
now. No `X-RateLimit-*` headers.

**503:** the store could not be read. The service does not guess "full":

```json
{ "error": "storage_unavailable", "message": "The rate-limit store could not be read; the status is unknown, not full" }
```

```bash
curl -s http://localhost:8080/v1/ratelimit/status/user:12345 \
  -H "Authorization: Bearer test-api-key"
```

### Profiles and tiers

Your key's tier picks your profile: the entry under `rate-limit.profiles` named
after the tier, or `rate-limit.default-capacity` / `default-refill-rate-per-second`
(100, 10/s) if there is none. The shipped profiles:

| Profile | `capacity` | `refillRatePerSecond` |
|---------|-----------:|----------------------:|
| `free` | 20 | 2 |
| `basic` | 100 | 10 |
| `premium` | 1,000 | 100 |
| `enterprise` | 10,000 | 1,000 |

A request's `profile` may only narrow your limits: it is accepted when its
capacity and its refill rate are both at most your tier's. A premium key may
name `free` or `basic`; a free key only `free`. The bucket itself is identified
by client and `key` alone, so naming a profile applies different limits to the
same bucket, not a new one.

The algorithm is set per deployment by `RATE_LIMIT_ALGORITHM`: `token-bucket`
(default), `leaky-bucket` (the refill rate is the leak rate) or
`sliding-window` (`capacity` per window of the profile's `ttlSeconds`, 3600 in
the shipped profiles). The request and response shapes are the same for all
three; `resetAt` and `retryAfter` come from the algorithm in use.

### When the store cannot answer

Each attempt at the rate-limit store has a timeout (`TIMEOUT_RATE_LIMIT_CHECK`,
default 2 s; compose sets 10 s). DynamoDB service errors are retried up to 3
times; a timeout is not. The store sits behind a bulkhead and a circuit
breaker, which opens after 20 consecutive failed calls
(`CIRCUIT_BREAKER_MAX_FAILURES`) and stays open 30 s. OCC conflicts do not
count toward it: a check that loses every OCC retry is refused with a 429, not
degraded.

When the breaker is open, the bulkhead is full, or the call fails, a check is
answered by `DEGRADATION_MODE`:

| Mode | Answer |
|------|--------|
| `reject-all` (default) | `429`, `retryAfter: 60`, `Retry-After: 60`, `resetAt` 60 s from now, `message: "Rate limit exceeded"`. The body does not say it was degraded. |
| `allow-all` | `200`, `tokensRemaining: 100` whatever the profile, `limit` the profile's capacity. Nothing is counted, so spend is unbounded while degraded. |

Any other value stops startup. Degraded answers are counted in the
`gate_degraded_total` metric.

---

## Idempotency

The flow:

1. `check` the key. `new` means you now own it: run the operation.
2. When it succeeds, `complete` the key with the response to replay. When it
   fails without effect, `fail` the key so a retry can claim it.
3. Later checks answer `in_progress` while the key is pending, `duplicate`
   with the stored response once it is completed, and `conflict` when the
   `requestBody` differs from the one that claimed it.

A pending key stays `in_progress` until it is completed, failed, or its TTL
passes. Only the client that claimed a key can complete or fail it; for any
other client the key does not exist.

Idempotency store calls (check, complete, fail) have a timeout
(`TIMEOUT_IDEMPOTENCY_CHECK`, default 2 s; compose sets 10 s) and a bulkhead;
a failed call is not retried. A timeout, a full bulkhead or a store error
answers 503 `storage_unavailable`.

### `POST /v1/idempotency/check`

**Permission:** `IdempotencyCheck`.

```json
{ "idempotencyKey": "payment:abc-123", "ttl": 86400, "requestBody": "{\"amount\":100}" }
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `idempotencyKey` | string | yes | The operation's key, scoped to your client |
| `ttl` | integer | no | Seconds the record should live. Default `IDEMPOTENCY_DEFAULT_TTL` (86400); capped at `IDEMPOTENCY_MAX_TTL_SECONDS` (86400). Zero or negative values are not rejected. |
| `requestBody` | string | no | Any string that identifies the request, usually its body. Only its SHA-256 is stored. The hash is over the exact UTF-8 bytes, so reordered or reformatted JSON hashes differently. |

The record's TTL is a DynamoDB TTL attribute. DynamoDB removes expired items
some time after they expire, so the service checks expiry itself: once the TTL
passes, the key answers `new` to the next check, whether or not the item has
been removed.

**Answers:**

| Status | `status` | Meaning |
|--------|----------|---------|
| `200` | `new` | You claimed the key (or reclaimed one that was failed). Run the operation. |
| `202` | `in_progress` | The key is pending. Do not run the operation. |
| `200` | `duplicate` | The operation completed; `originalResponse` holds what was stored. |
| `409` | `conflict` | This check's `requestBody` hash differs from the stored one. |

A conflict needs both hashes: a check without `requestBody`, or a key claimed
without one, never conflicts.

```json
{
  "status": "duplicate",
  "idempotencyKey": "payment:abc-123",
  "originalResponse": {
    "statusCode": 201,
    "body": "{\"paymentId\":\"pay_xyz789\"}",
    "headers": { "Content-Type": "application/json" }
  },
  "firstSeenAt": "2026-09-27T11:58:02.114Z",
  "message": null
}
```

| Field | Type | Description |
|-------|------|-------------|
| `status` | string | `new`, `in_progress`, `duplicate` or `conflict` |
| `idempotencyKey` | string | The key as you sent it |
| `originalResponse` | object or null | For `duplicate`: `statusCode` (integer), `body` (string), `headers` (object, `{}` if none were stored). `null` otherwise. |
| `firstSeenAt` | string (ISO-8601) or null | When the key was claimed; set for `in_progress` and `duplicate` |
| `message` | string or null | `in_progress`: `"Operation is currently being processed"`. `conflict`: `"Request body does not match the original request for this idempotency key"`. |

A `new` answer is `{"status": "new", "idempotencyKey": "...", "originalResponse": null, "firstSeenAt": null, "message": null}`.

**503:** `storage_unavailable` (the store failed or timed out, or the claim
lost its retries to concurrent changes of the record) or `storage_corruption`
(the stored record cannot be decoded; whether the operation ran is unknown, so
do not proceed):

```json
{ "error": "storage_corruption", "message": "Idempotency record corrupted: unknown status value: 'Done'" }
```

```bash
curl -s -X POST http://localhost:8080/v1/idempotency/check \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer test-api-key" \
  -d '{"idempotencyKey": "payment:abc-123", "ttl": 3600}'
```

### `POST /v1/idempotency/{key}/complete`

**Permission:** `IdempotencyComplete`. Stores the response to replay, and moves
the key from pending to completed. The service stores and replays these values
as given; it does not interpret them.

```json
{ "statusCode": 201, "body": "{\"paymentId\":\"pay_xyz789\"}", "headers": { "Content-Type": "application/json" } }
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `statusCode` | integer | yes | Status to replay |
| `body` | string | yes | Body to replay |
| `headers` | object of strings | no | Headers to replay; `{}` if omitted |

| Status | Body | Cause |
|--------|------|-------|
| `200` | `{"idempotencyKey": "payment:abc-123", "status": "completed", "message": null}` | Stored |
| `409` | `{"idempotencyKey": "...", "status": "conflict", "message": "Could not store response - key may not exist or is not pending"}` | No pending key of yours by that name: never claimed, already completed, failed, expired, or another client's |
| `413` | `{"error": "response_too_large", "message": "The response to store is 412345 bytes encoded; ..."}` | The stored response would exceed 358,400 bytes (350 KiB). The key stays pending: store a smaller response, such as a reference to the result, or fail the key. |
| `503` | `{"error": "storage_unavailable", ...}` | Store failure or timeout |

The size limit counts the response as the store encodes it: JSON with
`statusCode`, `body`, `headers` and a completion time, so escaping counts (a
`"` in `body` costs two bytes). The stored response is replayed inline in the
check answer, not streamed. The size is checked before the store is touched,
so an oversized response answers 413 whatever the key's state.

```bash
curl -s -X POST http://localhost:8080/v1/idempotency/payment:abc-123/complete \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer test-api-key" \
  -d '{"statusCode": 201, "body": "{\"paymentId\":\"pay_xyz789\"}"}'
```

### `POST /v1/idempotency/{key}/fail`

**Permission:** `IdempotencyComplete`. No body. Releases a pending key after its
operation failed without effect, so the next check answers `new` and the
caller can retry. A completed key is never reopened: its operation ran.

| Status | Body | Cause |
|--------|------|-------|
| `200` | `{"idempotencyKey": "payment:abc-123", "status": "failed", "message": null}` | Released |
| `409` | `{"idempotencyKey": "...", "status": "conflict", "message": "Could not mark failed - key may not exist or is not pending"}` | No pending key of yours by that name |
| `503` | `{"error": "storage_unavailable", ...}` | Store failure or timeout |

```bash
curl -s -X POST http://localhost:8080/v1/idempotency/payment:abc-123/fail \
  -H "Authorization: Bearer test-api-key"
```

---

## Token quotas

Quotas cap LLM tokens (input plus output) per user, per agent and per org. A
check charges its estimate to every level it names at once: if any level would
go over its limit, nothing is charged anywhere. Levels are checked in the
order user, agent, org, and the first that would overflow is reported.

Each counter has a fixed window that starts at its first charge and lasts the
level's window length; after it lapses, the next charge starts a new window.
Counters are scoped to your client, so two clients naming the same `userId`
meter separate counters.

| Setting (env) | Default |
|---------------|---------|
| `TOKEN_QUOTA_ENABLED` | `false` (compose: `true`) |
| `TOKEN_QUOTA_USER_LIMIT` / `TOKEN_QUOTA_USER_WINDOW` | 1,000,000 tokens / 3600 s |
| `TOKEN_QUOTA_AGENT_LIMIT` / `TOKEN_QUOTA_AGENT_WINDOW` | 500,000 tokens / 3600 s. With quotas on, startup fails if the limit is above 80% of the user limit. |
| `TOKEN_QUOTA_ORG_LIMIT` / `TOKEN_QUOTA_ORG_WINDOW` | 10,000,000 tokens / 86400 s |
| `TOKEN_QUOTA_RESERVATION_TTL_SECONDS` | 3600 s |
| `TIMEOUT_QUOTA_CHECK` | 5 s per check or reconcile, including its retries (compose: 10 s) |

With quotas disabled, both routes answer `404` with an empty body. The
permission check still runs first, so a key without the permission gets 403.

### `POST /v1/quota/check`

**Permission:** `QuotaCheck`. Reserves the estimate before the LLM call.

```json
{ "userId": "user:alice", "agentId": "agent:planner", "orgId": "org:acme",
  "estimatedInputTokens": 1200, "estimatedOutputTokens": 400 }
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `userId` | string | yes | User charged; always a level |
| `agentId` | string | no | Adds the agent level |
| `orgId` | string | no | Adds the org level |
| `estimatedInputTokens` | integer | yes | Expected prompt tokens, at least 0 |
| `estimatedOutputTokens` | integer | no, default `0` | Expected completion tokens, at least 0 |

**Allowed (200):**

```json
{ "allowed": true, "remainingTokens": { "user": 998400, "agent": 498400, "org": 9998400 },
  "exceededLevel": null, "retryAfter": null, "message": null,
  "reservationId": "5f0c2a6e-8a53-4c43-9f53-8f0d7e1f3b0a" }
```

**Exceeded (429):** nothing was charged. Header: `Retry-After` (same as
`retryAfter`).

```json
{ "allowed": false, "remainingTokens": {}, "exceededLevel": "org", "retryAfter": 1740,
  "message": "org quota exceeded: 9999000/10000000 tokens used", "reservationId": null }
```

| Field | Type | Description |
|-------|------|-------------|
| `allowed` | boolean | Whether the estimate was charged |
| `remainingTokens` | object | Per requested level (`user`, `agent`, `org`): the limit minus usage after this charge. `{}` unless allowed. |
| `exceededLevel` | string or null | `user`, `agent` or `org` on a 429 |
| `retryAfter` | integer or null | 429: seconds until the exceeded level's window ends (at least 1). Contended 503: `1`. |
| `message` | string or null | Why it was refused |
| `reservationId` | string or null | Set when allowed; pass it to `/v1/quota/reconcile` |

An estimate larger than a level's limit is always refused.

**503, contended:** the store lost every conditional write in its 25 attempts,
or the reservation record could not be written (the charge is then released).
Nothing was reserved; retry after `Retry-After: 1`. Body in the same shape:
`allowed: false`, `remainingTokens: {}`, `exceededLevel: null`,
`retryAfter: 1`, `message: "quota state contended after 25 attempts; nothing was reserved, retry shortly"`.

**503, store failure:** a timeout, a full bulkhead or an SDK error, with
`Retry-After: 1`. A check that timed out may still have charged; that charge
cannot be reconciled and stays counted until its window ends.

```json
{ "error": "storage_unavailable", "message": "The quota store did not answer; the check did not complete, retry it" }
```

**400:** `{"error": "validation_error", "message": "token estimates must be non-negative"}`.

```bash
curl -s -X POST http://localhost:8080/v1/quota/check \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer test-api-key" \
  -d '{"userId": "user:alice", "orgId": "org:acme", "estimatedInputTokens": 1200, "estimatedOutputTokens": 400}'
```

### `POST /v1/quota/reconcile`

**Permission:** `QuotaReconcile`. After the LLM call, replaces the reservation's
estimate with actual usage. The estimate comes from the reservation the check
stored, never from the request, so a reconcile can only give back its own
reservation's charge.

```json
{ "reservationId": "5f0c2a6e-8a53-4c43-9f53-8f0d7e1f3b0a", "actualInputTokens": 1000, "actualOutputTokens": 550 }
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `reservationId` | string | yes | From the check's 200 |
| `actualInputTokens` | integer | yes | Prompt tokens used, at least 0 |
| `actualOutputTokens` | integer | yes | Completion tokens used, at least 0 |

Other fields are ignored; a body without `reservationId` answers 422.

For each counter the check charged, `actual - estimate` is applied (it may be
negative) together with marking the reservation reconciled, in one
transaction. A counter whose window has rolled over since the check never held
the estimate, so it gets only usage above the estimate, never a refund.
Actual usage is recorded even past the limit; later checks for that identity
are then refused until the window ends.

| Status | Body | Meaning |
|--------|------|---------|
| `200` | `{"status": "reconciled", "inputDelta": -200, "outputDelta": 150}` | Applied. Deltas are actual minus the stored estimate. Sending the same usage again returns the same 200 and changes nothing, so a retry is safe. |
| `404` | `{"error": "reservation_not_found", "message": "No live reservation with this ID for this key: unknown, expired, or another client's"}` | The three cases are deliberately indistinguishable. Nothing was written; the estimate stays counted. |
| `409` | `{"error": "already_reconciled", "message": "Reservation already reconciled with 1000 input and 550 output tokens"}` | Reconciled before with different usage |
| `503` + `Retry-After: 1` | `{"error": "contended", "message": "reconciliation contended after 25 attempts; nothing was recorded, retry shortly"}` | Not recorded; send the same request again |
| `503` + `Retry-After: 1` | `{"error": "storage_unavailable", "message": "The quota store did not answer; retry with the same usage"}` | Store failure or timeout; whether it was recorded is unknown, and a repeat with the same usage is safe |
| `400` | `{"error": "validation_error", "message": "token counts must be non-negative"}` | A negative count |

A reservation can be reconciled for `TOKEN_QUOTA_RESERVATION_TTL_SECONDS`
after the check; after that the answer is 404 and the estimate stays counted.

```bash
curl -s -X POST http://localhost:8080/v1/quota/reconcile \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer test-api-key" \
  -d '{"reservationId": "5f0c2a6e-8a53-4c43-9f53-8f0d7e1f3b0a", "actualInputTokens": 1000, "actualOutputTokens": 550}'
```

---

## Health and monitoring

### `GET /health`

No authentication. Liveness only: `200` whenever the process serves HTTP, with
no dependency checks. `version` is the build's version (sbt-buildinfo).

```json
{ "status": "healthy", "version": "0.1.0" }
```

### `GET /ready`

No authentication. Checks each component and answers `503` only when a
required one fails.

| Component | Required | Check |
|-----------|----------|-------|
| `dynamodb_ratelimit` | yes | The rate-limit table (`DescribeTable`, bounded only by the SDK's own timeouts) |
| `dynamodb_idempotency` | yes | The idempotency table |
| `dynamodb_quota` | yes | The quota table; listed only when quotas are enabled |
| `kinesis` | no | The event stream. Events are fire-and-forget and no request waits on them. Reads `ok` when Kinesis is disabled. |

| `status` | HTTP | Meaning |
|----------|------|---------|
| `ok` | 200 | Every component is `ok` |
| `degraded` | 200 | Only optional components fail |
| `unavailable` | 503 | A required component fails |

```json
{
  "status": "degraded",
  "components": [
    { "name": "dynamodb_ratelimit", "status": "ok", "required": true, "details": null },
    { "name": "dynamodb_idempotency", "status": "ok", "required": true, "details": null },
    { "name": "dynamodb_quota", "status": "ok", "required": true, "details": null },
    { "name": "kinesis", "status": "error", "required": false, "details": "<error message>" }
  ]
}
```

A component's `status` is `ok` or `error`; `details` is the error message, or
`null`.

### `GET /metrics`

**Permission:** `AdminMetrics` (the built-in `admin-api-key`). Prometheus text
exposition, `text/plain; charset=UTF-8`. Gate's own series are prefixed
`gate_`. With `PROMETHEUS_ENABLED=false` an authorized request gets `404` with
an empty body.

```bash
curl -s -H "Authorization: Bearer admin-api-key" http://localhost:8080/metrics
```

---

## Demo dashboard

Mounted only with `DASHBOARD_ENABLED=true` (default `false`; docker compose
sets it). Every dashboard route is **unauthenticated**: the config POST
rewrites the demo bucket's profile for everyone on that instance, and the
decision stream carries every client's key ID. With the flag off these paths
fall through to authentication (401 without a key, 404 with one).

The demo bucket is the fixed key `dashboard-demo` in the real rate-limit
store, starting from `rate-limit.default-*` (100 tokens, 10/s).

| Method | Path | Answer |
|--------|------|--------|
| `GET` | `/dashboard` | The HTML page |
| `GET` | `/dashboard/api/config` | `{"capacity", "refillRatePerSecond", "ttlSeconds"}` of the demo bucket |
| `POST` | `/dashboard/api/config` | Body with the same three fields, each above 0. Answers them plus `"message": "Configuration updated successfully"`, or `400 {"error": "<reason>"}`. |
| `POST` | `/dashboard/api/check` | Consumes 1 token. Always `200`: `allowed`, `tokensRemaining`, `limit`, `resetAt`, plus `retryAfter` when refused |
| `GET` | `/dashboard/api/status` | `tokensRemaining`, `limit`, and `resetAt` (always `""`) |
| `GET` | `/dashboard/api/stats` | Server-sent events every 500 ms: `{"tokensRemaining", "limit", "timestamp"}` (epoch ms) |
| `GET` | `/v1/ratelimit/dashboard/stats` | Server-sent events: each event the service publishes (rate-limit decisions, idempotency and quota events, audit events), as JSON with an `event_type` field. They pass through a 512-event queue: events are dropped while it is full, and concurrent viewers split the stream between them. |

---

## Errors

### Error shapes

| Shape | Where |
|-------|-------|
| `{"error": "<code>", "message": "<text>"}` | Most refusals and store failures ([codes below](#error-codes)) |
| The route's own response shape | Rate-limit 429; idempotency `conflict` 409 and the complete/fail 409s; quota check 429 and contended 503. The outcome is in `allowed` or `status`, not `error`. |
| `{"error": "Rate limited", "retryAfter": N}` | Auth throttle 429 |
| `{"error": "<reason>"}` | Dashboard config POST 400 |
| Plain text | `400 The request body was malformed.` (not JSON, or empty); `422 The request body was invalid.` (JSON without a required field, or a field of the wrong type); `404 Not found` (unknown path, valid key) |
| Empty body | `401` (no or unknown key); `404` (quota routes when disabled, `/metrics` when Prometheus is disabled); `500` (an unhandled error) |

### Error codes

| Status | `error` | Route | Cause |
|--------|---------|-------|-------|
| 400 | `validation_error` | `POST /v1/ratelimit/check` | `cost` below 1, or an unknown `profile` |
| 400 | `validation_error` | `POST /v1/quota/check`, `/reconcile` | A negative token count |
| 403 | `forbidden` | Any authenticated route | The key lacks the route's permission |
| 403 | `profile_not_permitted` | `POST /v1/ratelimit/check` | `profile` above the key's tier |
| 404 | `reservation_not_found` | `POST /v1/quota/reconcile` | Unknown, expired, or another client's reservation |
| 409 | `already_reconciled` | `POST /v1/quota/reconcile` | Reconciled before with different usage |
| 413 | `response_too_large` | `POST /v1/idempotency/{key}/complete` | Stored response over 358,400 bytes encoded |
| 503 | `storage_unavailable` | Rate-limit status, all idempotency routes, both quota routes (quota adds `Retry-After: 1`) | Store error, timeout, or full bulkhead |
| 503 | `storage_corruption` | `POST /v1/idempotency/check` | The stored record cannot be decoded |
| 503 | `contended` | `POST /v1/quota/reconcile` | Every conditional write lost; nothing recorded |

---

## Client guidance

Each point follows from the behavior above.

- **Rate limit:** read `allowed`, not just the status code, and wait
  `Retry-After` seconds after a 429. A 429 with `retryAfter: 60` may be a
  degraded answer rather than an empty bucket. The auth throttle's 429 has
  `"error": "Rate limited"` and no `allowed`; it also means back off.
- **Idempotency:** run the operation only on `new`, and finish every claimed
  key with `complete` or `fail`, or it answers `in_progress` until its TTL
  passes. A 503 means the state is unknown: do not run the operation.
- **Quota:** keep the check's `reservationId` and reconcile with it. After a
  503 on reconcile, send the same request again after `Retry-After`; a repeat
  with the same usage is safe. A 503 on check means you were not admitted.
