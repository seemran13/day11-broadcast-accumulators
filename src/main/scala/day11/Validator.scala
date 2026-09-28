package day11

/**
 * Pure validation logic, kept in a standalone object so Spark closures
 * don't need to serialize any bigger enclosing object.
 *
 * Returns Left((reason, rawLine)) for a bad record, Right(txn) for a good one.
 */
object Validator {

  def validate(line: String, products: Map[String, Product]): Either[(String, String), Txn] = {
    val p = line.split(",", -1).map(_.trim)
    if (p.length != 4) Left(("MALFORMED", line))
    else {
      try {
        val qty   = p(2).toInt
        val price = p(3).toDouble
        products.get(p(1)) match {
          case None => Left(("UNKNOWN_PRODUCT", line))
          case Some(prod) =>
            if (qty <= 0) Left(("BAD_QUANTITY", line))
            else if (math.abs(prod.price - price) > 0.01) Left(("PRICE_MISMATCH", line))
            else Right(Txn(p(0), p(1), qty, price))
        }
      } catch {
        case _: NumberFormatException => Left(("MALFORMED", line))
      }
    }
  }
}
