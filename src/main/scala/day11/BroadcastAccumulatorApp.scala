package day11

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
 * Day 11 — Broadcast and Accumulators
 *
 * Covers:
 *   1. Broadcasting a small product reference map
 *   2. Using accumulators to count bad records
 *   3. Why plain driver variables must not be used for distributed updates
 *   4. Combining broadcast data with RDD processing
 *   5. Scenario: validating transactions against a small master table
 *
 * Run with:
 *   sbt "run local[2]"
 *   sbt "run local[4]"
 */
object BroadcastAccumulatorApp {

  // The small master table. In real life this might come from a database
  // or a reference file; here it is hard-coded to keep the demo simple.
  val products: Map[String, Product] = Map(
    "P1" -> Product("P1", "Wireless Mouse", 799.0),
    "P2" -> Product("P2", "Mechanical Keyboard", 3499.0),
    "P3" -> Product("P3", "USB-C Hub", 1299.0),
    "P4" -> Product("P4", "Laptop Stand", 1999.0),
    "P5" -> Product("P5", "Webcam 1080p", 2499.0),
    "P6" -> Product("P6", "Phone Stand", 499.0),
    "P7" -> Product("P7", "Cable Organizer", 299.0)
  )

  def main(args: Array[String]): Unit = {
    val masterUrl = if (args.nonEmpty) args(0) else "local[*]"

    val spark = SparkSession.builder()
      .appName("Day11-BroadcastAccumulatorApp")
      .master(masterUrl)
      .getOrCreate()

    val sc = spark.sparkContext

    section("1. Broadcasting a small product reference map")
    val bcProducts = sc.broadcast(products)
    println(s"Broadcast created with ${bcProducts.value.size} products.")
    println(
      """Why broadcast? If a Map is simply captured in a closure, Spark
        |serializes and ships a copy with EVERY TASK. A broadcast variable is
        |sent to each executor ONCE, cached there read-only, and shared by
        |all tasks running on it. For a small lookup table used by many
        |tasks, that's far less network traffic (and it's what makes a
        |"map-side join" against a small table possible without a shuffle).
        |""".stripMargin)

    section("3. Why normal driver variables can't be used for distributed updates")
    driverVariablePitfall(sc)

    section("2, 4 & 5. Scenario: validate transactions against the master table")
    validationScenario(sc, bcProducts)

    bcProducts.destroy() // release the broadcast's memory on driver and executors
    spark.stop()
  }

  // ---------------------------------------------------------------------
  // 3. The driver-variable pitfall, shown live
  // ---------------------------------------------------------------------
  def driverVariablePitfall(sc: SparkContext): Unit = {
    val lines = sc.textFile("src/main/resources/transactions.txt")
      .filter(l => l.nonEmpty && !l.startsWith("TxnId"))

    var driverCounter = 0                          // plain driver-side variable
    val accumulator   = sc.longAccumulator("demo") // Spark accumulator

    lines.foreach { line =>
      if (line.split(",", -1).length != 4) {
        driverCounter += 1     // mutates a COPY inside the task
        accumulator.add(1)     // safely aggregated back to the driver
      }
    }

    println(s"Plain driver var after foreach : $driverCounter   <- still 0 (wrong)")
    println(s"Accumulator after foreach      : ${accumulator.value}   <- correct")
    println(
      """
        |Why the plain variable stays 0: Spark serializes the closure and
        |sends it to the tasks. Each task gets its OWN deserialized copy of
        |driverCounter and increments that copy. Nothing is ever sent back,
        |so the driver's original variable is never changed. This happens
        |even in local mode, and on a real cluster it is worse, because the
        |tasks run in different JVMs on different machines.
        |
        |An Accumulator is the purpose-built fix: tasks can only ADD to it,
        |Spark merges each task's contribution back on the driver, and only
        |the driver can read the value.
        |""".stripMargin)
  }

  // ---------------------------------------------------------------------
  // 2, 4 & 5. Validate transactions using broadcast + accumulators
  // ---------------------------------------------------------------------
  def validationScenario(sc: SparkContext, bcProducts: org.apache.spark.broadcast.Broadcast[Map[String, Product]]): Unit = {
    // One accumulator per kind of problem, plus one that collects samples.
    val malformed    = sc.longAccumulator("malformed")
    val unknownProd  = sc.longAccumulator("unknownProduct")
    val badQuantity  = sc.longAccumulator("badQuantity")
    val priceMismatch = sc.longAccumulator("priceMismatch")
    val badSamples   = sc.collectionAccumulator[String]("badSamples")

    val lines = sc.textFile("src/main/resources/transactions.txt")
      .filter(l => l.nonEmpty && !l.startsWith("TxnId"))

    // Combine broadcast data with RDD processing: every task looks up
    // bcProducts.value locally - no shuffle, no join.
    val validated = lines.map { line =>
      val result = Validator.validate(line, bcProducts.value)
      result match {
        case Left((reason, rawLine)) =>
          reason match {
            case "MALFORMED"        => malformed.add(1)
            case "UNKNOWN_PRODUCT"  => unknownProd.add(1)
            case "BAD_QUANTITY"     => badQuantity.add(1)
            case "PRICE_MISMATCH"   => priceMismatch.add(1)
            case _                  => ()
          }
          badSamples.add(s"[$reason] $rawLine")
        case Right(_) => ()
      }
      result
    }

    // IMPORTANT: accumulator updates inside transformations are only
    // applied when an action runs, and would be applied AGAIN if the RDD
    // were recomputed by a later action (or a task retry). So we cache()
    // the RDD and trigger it with exactly one action before reading them.
    validated.cache()
    val total = validated.count()

    val badTotal = malformed.value + unknownProd.value + badQuantity.value + priceMismatch.value
    println(s"Total records checked : $total")
    println(s"Valid records         : ${total - badTotal}")
    println(s"Bad records           : $badTotal")
    println(s"  - malformed         : ${malformed.value}")
    println(s"  - unknown product   : ${unknownProd.value}")
    println(s"  - bad quantity      : ${badQuantity.value}")
    println(s"  - price mismatch    : ${priceMismatch.value}")

    println("\nBad record samples (from the collection accumulator):")
    val samples = badSamples.value
    val it = samples.iterator()
    while (it.hasNext) println(s"  ${it.next()}")

    // Continue processing only the valid records, still using the broadcast
    // map to enrich each one with its product name.
    val validTxns = validated.collect { case Right(t) => t }
    val revenueByProduct = validTxns
      .map { t =>
        val name = bcProducts.value(t.productId).name
        (name, t.quantity * t.price)
      }
      .reduceByKey(_ + _)
      .collect()
      .sortBy(-_._2)

    println("\nRevenue by product name (valid transactions only, enriched via broadcast):")
    revenueByProduct.foreach { case (name, rev) => println(f"  $name%-22s Rs. $rev%.2f") }

    val grandTotal = revenueByProduct.map(_._2).sum
    println(f"\nTotal valid revenue: Rs. $grandTotal%.2f")

    validated.unpersist()
  }

  def section(title: String): Unit = {
    println("\n" + "=" * 70)
    println(title)
    println("=" * 70)
  }
}
