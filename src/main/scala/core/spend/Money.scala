package core.spend

/** Money in integer micro-dollars (1 USD = 1,000,000).
  *
  * Budgets are compared and decremented on every request, so the
  * representation has to be exact. Doubles are not: accumulating a few million
  * fractions of a cent drifts, and a budget that drifts is a budget that either
  * over-admits or strands money. A Long of micro-dollars holds about ±9.2
  * trillion USD, which is enough headroom to never think about it again.
  *
  * Micro-dollars rather than cents because model pricing is quoted per million
  * tokens: at $0.15 / 1M input tokens a single token costs 0.00000015 USD, and
  * rounding that to a cent would price almost every request at zero.
  */
opaque type MicroUsd = Long

object MicroUsd:
  val zero: MicroUsd = 0L

  val perDollar: Long = 1_000_000L

  def apply(micros: Long): MicroUsd = micros

  def fromDollars(dollars: Double): MicroUsd =
    math.round(dollars * perDollar.toDouble)

  /** Parses "12.50", "0.000150", "3" as dollars. Rejects anything else rather
    * than guessing, because a misparsed budget silently becomes the wrong
    * limit.
    */
  def parseDollars(s: String): Either[String, MicroUsd] =
    val trimmed = s.trim.stripPrefix("$")
    trimmed.toDoubleOption match
      case Some(d) if d.isNaN || d.isInfinite => Left(s"not a finite amount: $s")
      case Some(d) if d < 0 => Left(s"negative amount: $s")
      case Some(d) => Right(fromDollars(d))
      case None => Left(s"not a dollar amount: $s")

  extension (m: MicroUsd)
    def micros: Long = m
    def toDollars: Double = m.toDouble / perDollar.toDouble

    def +(other: MicroUsd): MicroUsd = m + other
    def -(other: MicroUsd): MicroUsd = m - other
    def *(n: Long): MicroUsd = m * n

    def isZero: Boolean = m == 0L
    def <=(other: MicroUsd): Boolean = m <= other
    def <(other: MicroUsd): Boolean = m < other

    /** Never negative -- used for "remaining budget", which is a floor of 0
      * rather than a debt.
      */
    def clampedAtZero: MicroUsd = math.max(0L, m)

    /** Renders enough decimals to be honest about sub-cent amounts, which is
      * where per-token pricing lives.
      */
    def show: String =
      val d = m.toDouble / perDollar.toDouble
      if m != 0L && math.abs(d) < 0.01 then f"$$$d%.6f" else f"$$$d%.2f"

  given Ordering[MicroUsd] = Ordering.Long
