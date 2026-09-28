package day11

/** One row of the small master table (the reference data we broadcast). */
final case class Product(id: String, name: String, price: Double)

/** A transaction that passed every validation rule. */
final case class Txn(txnId: String, productId: String, quantity: Int, price: Double)
