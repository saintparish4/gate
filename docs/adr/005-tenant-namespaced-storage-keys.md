# ADR-005: Namespace Every Storage Key by Authenticated Client

**Context:** Every storage key was the string the caller sent. Two clients that picked the same rate-limit key shared one bucket, two that picked the same idempotency key shared one record, and two that named the same quota user shared one counter. Concretely, any valid key could:

- drain another client's rate-limit bucket;
- read another client's stored idempotent response by replaying its key;
- complete another client's pending idempotency key with a forged response;
- zero another client's quota counters through reconcile.

No test sent two clients at the same key. A multi-tenant service cannot take external traffic in that state. We had to choose between isolating tenants inside one deployment (model A) and running one tenant per deployment (model B).

## Decision

Model A. Every storage key is scoped to the authenticated client that sent it, by one pure function, `core.TenantKey`:

```
t1:<length of clientId>:<clientId>:<caller's key>
```

The tenant is `AuthenticatedClient.clientId`. The shape starts on a clean table: rows written in the old shape are not migrated. They are never read again and expire by TTL.

## How It Works

1. **Rate limits.** `RateLimitApi` passes `TenantKey(clientId, key)` to the store for both check and status. The stores keep their own prefixes, so a token-bucket row is `ratelimit#t1:…`, and a sliding-window row is `sw#t1:…`.
2. **Idempotency.** `IdempotencyApi` scopes the key for check and complete. `storeResponse` also takes the client and conditions its write on the record's `clientId`, so a bug in key construction still cannot let one client complete another's record. Responses and events carry the key the caller sent, never the scoped one.
3. **Quotas.** `TokenQuotaService` builds each counter as `<level>:<TenantKey(clientId, id)>:<window>s`. A quota reservation (the record reconcile reads) is keyed the same way, so a reconcile naming another client's reservation finds nothing.
4. **Version.** `t1` names the shape. A future change to it is a new version and a deliberate migration decision, not a silent reinterpretation of old rows.

## Why This Shape

- **Injective.** The length prefix fixes where the client ID ends, even when client IDs or keys contain `:`. Without it, client `a:b` with key `c` and client `a` with key `b:c` would both become `a:b:c`.
- **One place.** Every store receives an already-scoped key and stays unaware of tenants, so the in-memory interpreters and all three rate-limit algorithms are covered by the same function.
- **Readable.** An operator can still read the client and key out of a DynamoDB item, which a hash would hide.

## What's Sacrificed

- **One fresh window on the deploy that introduces it.** No migration means every client starts this deploy with a full bucket and an empty quota window. That is a one-time over-issue, acceptable only because nothing runs paying traffic yet (owner decision, 27 September 2026). A later shape change on live traffic would need a migration or a dual read.
- **No cross-key sharing.** Two API keys share state only if they share a `clientId`. The built-in keys each have their own. Keys from Secrets Manager use their `apiKeyId` as the `clientId`, so today each Secrets Manager key is its own tenant. Grouping several keys under one customer would need a `clientId` field in the secret.
- **Longer keys.** Every partition key grows by the client ID plus a few characters. DynamoDB caps a partition key at 2,048 bytes, and caller keys are not length-checked yet.

## Alternatives Considered

- **Model B: one tenant per deployment.** No key changes, but every customer needs their own stack, and the service's "per-tenant" claims would have to go. It also leaves the shared demo unusable for more than one caller.
- **Namespacing inside each store.** Every store would take a client ID and build its own prefix. That is five implementations, plus the in-memory ones, that each have to get it right, instead of one function.
- **Hashing the client and key.** Fixed length and injective in practice, but it makes items unreadable when debugging, and SHA-256 per request buys nothing the length prefix does not.
