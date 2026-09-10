import org.apache.spark.{SparkConf, SparkContext}

object Day04 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day04-RDD-Creation")
      .setMaster("local[4]")

    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    println("=== Day 04 - RDD Creation ===")


    // 1. Create RDD from Scala collection

    val numbers = Seq(10, 20, 30, 40, 50)

    val numberRDD = sc.parallelize(numbers)

    println("\n--- RDD from Collection ---")
    numberRDD.collect().foreach(println)


    // 2. map, filter and flatMap

    val doubledRDD = numberRDD.map(_ * 2)

    val filteredRDD = numberRDD.filter(_ >= 30)

    val wordsRDD = sc.parallelize(
      Seq("Spark RDD", "Scala Spark", "RDD Practice")
    )

    val wordListRDD = wordsRDD.flatMap(_.split(" "))

    println("\n--- map ---")
    doubledRDD.collect().foreach(println)

    println("\n--- filter ---")
    filteredRDD.collect().foreach(println)

    println("\n--- flatMap ---")
    wordListRDD.collect().foreach(println)


    // 3. Create RDD from text file

    val transactionLines =
      sc.textFile("data/transactions.txt")

    println("\n--- Transactions from Text File ---")
    transactionLines.collect().foreach(println)


    // 4. Calculate total sales

    val transactionAmounts = transactionLines.map { line =>
      val fields = line.split(",")

      val quantity = fields(3).toInt
      val price = fields(4).toDouble

      quantity * price
    }

    val totalSales = transactionAmounts.reduce(_ + _)

    println("\n--- Total Sales ---")
    println(f"Total Sales: $totalSales%.2f")


    // 5. Inspect partitions

    println("\n--- Partition Information ---")

    println(
      s"Transaction RDD partitions: ${transactionLines.getNumPartitions}"
    )

    println(
      s"Default Parallelism: ${sc.defaultParallelism}"
    )


    // 6. Customer file processing

    val customersRDD =
      sc.textFile("data/customers.txt", 4)

    println("\n--- Customer File ---")
    customersRDD.collect().foreach(println)

    println(
      s"Customer RDD partitions: ${customersRDD.getNumPartitions}"
    )


    // 7. Process customer records

    val customerNames = customersRDD.map { line =>
      val fields = line.split(",")

      fields(1)
    }

    println("\n--- Customer Names ---")
    customerNames.collect().foreach(println)


    println("\n=== Day 04 Completed ===")

    sc.stop()
  }
}
