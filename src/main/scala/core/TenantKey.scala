package core

/** The storage key for a caller's key, scoped to the client that sent it
  * (ADR-005).
  *
  * Every key used to be the caller's string, so two clients naming the same
  * rate-limit key, idempotency key, or quota user shared one bucket, record, or
  * counter. Every storage key now goes through here.
  *
  * The client ID is length-prefixed so the mapping is injective even when IDs
  * or keys contain ':'. Without the length, client "a:b" with key "c" and
  * client "a" with key "b:c" would share a row. `t1` versions the shape, so a
  * future change is a deliberate migration rather than a reinterpretation of
  * old rows.
  */
object TenantKey:
  val Version = "t1"

  def apply(clientId: String, key: String): String =
    s"$Version:${clientId.length}:$clientId:$key"
