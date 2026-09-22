import org.apache.spark.{SparkConf, SparkContext}

object BroadcastAccumulatorPractice {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day11-Broadcast-Accumulators")
      .setMaster("local[4]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    // 1. Small product master table
    val productMaster = Map(
      "P100" -> "Laptop",
      "P101" -> "Mouse",
      "P102" -> "Keyboard",
      "P103" -> "Monitor"
    )

    // 2. Broadcast the product master
    val broadcastProducts = sc.broadcast(productMaster)

    println("\n=== Broadcast Product Master ===")
    println(broadcastProducts.value)

    // 3. Transaction data
    val transactions = sc.parallelize(Seq(
      ("T001", "P100", 50000.0),
      ("T002", "P101", 1000.0),
      ("T003", "P999", 2000.0),
      ("T004", "P102", 2500.0),
      ("T005", "P888", 1500.0),
      ("T006", "P103", 12000.0),
      ("T007", "P100", -500.0)
    ))

    // 4. Accumulator for bad records
    val badRecords = sc.longAccumulator("Bad Records")

    // 5. Validate transactions
    val validatedTransactions = transactions.map {
      case (transactionId, productId, amount) =>

        val products = broadcastProducts.value

        if (!products.contains(productId) || amount <= 0) {
          badRecords.add(1)

          (transactionId, productId, amount, "INVALID")
        } else {
          val productName = products(productId)

          (transactionId, productId, amount, productName)
        }
    }

    // 6. Trigger Spark execution
    val results = validatedTransactions.collect()

    // 7. Display results
    println("\n=== Transaction Validation Results ===")
    results.foreach(println)

    // 8. Display accumulator value
    println("\n=== Accumulator Result ===")
    println(s"Bad records: ${badRecords.value}")

    // 9. Summary
    val validCount = results.count {
      case (_, _, _, status) => status != "INVALID"
    }

    println("\n=== Summary ===")
    println(s"Valid transactions: $validCount")
    println(s"Bad transactions: ${badRecords.value}")

    sc.stop()
  }
}
