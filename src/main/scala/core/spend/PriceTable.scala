package core.spend

/** What one model costs, quoted the way vendors quote it: per million tokens.
  */
case class ModelPrice(
    inputPerMillion: MicroUsd,
    outputPerMillion: MicroUsd,
):
  /** Cost of a call, rounded up.
    *
    * Rounding up matters: rounding down lets a caller issue many tiny requests
    * that each cost "zero" and drain a budget without ever charging it. Up is
    * the direction that fails closed.
    */
  def costOf(inputTokens: Long, outputTokens: Long): MicroUsd =
    divCeil(inputPerMillion.micros * math.max(0L, inputTokens), 1_000_000L) +
      divCeil(outputPerMillion.micros * math.max(0L, outputTokens), 1_000_000L)

  private def divCeil(n: Long, d: Long): MicroUsd =
    MicroUsd(if n <= 0 then 0L else (n + d - 1) / d)

/** Model prices plus the ordered fallback chains used when a request does not
  * fit its budget.
  *
  * A chain is deliberately explicit rather than "any cheaper model": swapping
  * a customer's model is a product decision, not something to infer from a
  * price sort. An operator states which substitutions are acceptable.
  */
case class PriceTable(
    prices: Map[String, ModelPrice],
    fallbacks: Map[String, List[String]],
):
  def priceOf(model: String): Option[ModelPrice] = prices.get(model)

  def knows(model: String): Boolean = prices.contains(model)

  def costOf(
      model: String,
      inputTokens: Long,
      outputTokens: Long,
  ): Option[MicroUsd] = priceOf(model).map(_.costOf(inputTokens, outputTokens))

  /** The requested model first, then its declared substitutions, skipping any
    * that are unpriced or not actually cheaper. Duplicates are dropped so a
    * misconfigured chain cannot make the caller pay twice for one attempt.
    */
  def candidates(model: String): List[String] =
    val requested = priceOf(model)
    val chain = fallbacks.getOrElse(model, Nil).filter(m =>
      knows(m) && m != model,
    )
    val cheaperOnly = requested match
      case None => chain
      case Some(base) => chain.filter(m =>
          prices(m).inputPerMillion <= base.inputPerMillion &&
            prices(m).outputPerMillion <= base.outputPerMillion,
        )
    (model :: cheaperOnly).distinct

object PriceTable:
  val empty: PriceTable = PriceTable(Map.empty, Map.empty)
