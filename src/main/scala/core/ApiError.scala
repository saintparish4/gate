package core

import io.circe.Json

/** The one body every refusal and failure answers with:
  * `{"error": code, "message": text}`.
  *
  * There were five shapes: an empty 401, the auth throttle's
  * `{"error":"Rate limited","retryAfter":N}`, a dashboard 400 with only
  * `error`, plain text for an undecodable body or an unknown path, and this
  * one. A client had to know which route had refused it before it could read
  * why. `error` is a stable code to branch on; `message` is for a person.
  */
object ApiError:

  /** Codes shared by more than one layer. A route's own codes stay beside it.
    */
  val Unauthorized = "unauthorized"
  val RateLimited = "rate_limited"
  val Forbidden = "forbidden"
  val NotFound = "not_found"
  val ValidationError = "validation_error"
  val InvalidRequest = "invalid_request"
  val StorageUnavailable = "storage_unavailable"
  val Contended = "contended"
  val InternalError = "internal_error"

  /** @param extra
    *   Fields beyond the two every error has, such as `retryAfter`
    */
  def body(code: String, message: String, extra: (String, Json)*): Json = Json
    .obj(
      ("error" -> Json.fromString(code)) +:
        ("message" -> Json.fromString(message)) +: extra*,
    )
