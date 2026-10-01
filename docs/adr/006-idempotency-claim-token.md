# ADR-006: Fence `complete` and `fail` with a Claim Token

**Context:** An idempotency key is claimed by `check` and ended by `complete` or `fail`. Both of those name only the key. The store accepts them when the record is Pending, belongs to the calling client, and has not expired.

That is not enough once a key can be claimed twice. A record that passes its TTL is claimable at once, whether or not DynamoDB has deleted it, and a failed key is claimable by design. So this can happen:

1. Run A checks `payment:1` and is told `new`. Its operation takes longer than the key's TTL.
2. The key expires. Run B checks it, is told `new`, and starts the operation again.
3. Run A finishes and calls `complete`, or gives up and calls `fail`.

The record A's call reaches is B's: Pending, the same client, and live. Nothing tells the two runs apart, because keys are scoped per client (ADR-005) and both runs are the same client.

- A's late `complete` stores A's response on B's claim. B's own `complete` then answers 409, and every later check replays A's response.
- A's late `fail` releases B's claim while B is still running, so a third caller is told `new`. That is one more concurrent execution than the lapsed TTL already allowed.

The expiry check on `complete` and `fail` does not help: it refuses a call on an expired record, and after step 2 the record is no longer expired.

## Decision

A claim gets a token. `check` returns a `claimId` when it answers `new`, and `complete` and `fail` accept it. When a call carries a `claimId`, the store applies it only to the claim that was issued that ID.

The token is optional on `complete` and `fail`. A call without one behaves as it did before this ADR.

## How It Works

1. **Issue.** The store generates a random UUID when a claim wins, writes it on the record as `claimId`, and returns it in `IdempotencyResult.New`. A reclaim, of an expired or a failed key, is a new claim with a new ID.
2. **Carry.** The API returns `claimId` in the `new` answer only. `in_progress`, `duplicate` and `conflict` answers leave it `null`: only the caller that won the claim holds its token.
3. **Fence.** `complete` takes `claimId` in its body. `fail`, which had no body, takes an optional body with the one field. Given a `claimId`, the DynamoDB write adds `claimId = :claimId` to its condition; the in-memory stores compare it the same way.
4. **Refusal.** A mismatch is the same 409 `not_pending` as a key that is not pending: from the first run's side its claim is gone, and it must not act as if it still held the key.

## Why This Shape

- **It names the claim, not the key.** The gap exists because two claims share one key. A token per claim is the smallest thing that separates them.
- **Random, not derived.** The record's `version` restarts at 1 on a reclaim, and `createdAt` can repeat within a millisecond, so neither is unique per claim. The request hash is the same for a retry by definition.
- **In the condition, not read first.** The comparison is part of the conditional write, so there is no window between checking the token and applying the change.
- **Optional, so nothing breaks.** Existing clients send no `claimId` and keep the behavior they have. A client opts in by sending the value `check` already gave it.

## What's Sacrificed

- **The gap stays open for callers that omit the token.** A call without a `claimId` is still accepted on any live Pending record of the client's. Requiring the token would close it for everyone, and would break every existing client; that is a decision for a later version.
- **It does not stop the second execution.** Once a TTL lapses while the first run is still going, the operation runs twice. The token only keeps the first run from ending the second run's claim. The fix for the double run is a TTL longer than the operation.
- **Not a secret.** The token fences a client against its own earlier runs. It is not an authorization check: tenancy is still enforced by the scoped key and the record's `clientId`.
- **Records written before this change have no `claimId`.** A call naming one against such a record is refused. No client can hold a token for those records, because the `check` that claimed them returned none.
- **A lost token cannot be recovered.** A caller that crashes between `check` and saving the `claimId` can still end the claim without one, or wait for the TTL.

## Alternatives Considered

- **Leave it, and document the TTL rule.** This was the state after the expiry check. It leaves a late `fail` able to reopen a live claim, which is a correctness gap in the feature whose point is running an operation once.
- **Require the token.** Closes the gap for every caller, at the cost of a breaking change to two routes. Deferred, not rejected.
- **Use `version` or `createdAt` as the token.** No new attribute, but neither is unique per claim (see above).
- **Refuse a reclaim while a claim is Pending, even past its TTL.** A crashed owner's key would then stay stuck until DynamoDB deleted the item, often days later. That is the failure the expiry check was added to end.
