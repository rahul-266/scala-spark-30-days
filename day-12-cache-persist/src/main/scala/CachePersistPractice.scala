import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

object CachePersistPractice {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day12-Cache-Persist")
      .setMaster("local[4]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    // --------------------------------------------------
    // 1. Transaction dataset
    // --------------------------------------------------

    val transactions = sc.parallelize(
      (1 to 100000).map { id =>
        val transactionType =
          if (id % 2 == 0) "UPI" else "ATM"

        val status =
          if (id % 10 == 0) "FAILED" else "SUCCESS"

        val amount = (id % 5000) + 100.0

        val accountId = s"ACC${id % 1000}"

        (id, accountId, transactionType, amount, status)
      },
      4
    )

    println("\n=== Original Dataset ===")
    println(s"Partitions: ${transactions.getNumPartitions}")

    // --------------------------------------------------
    // 2. Clean the transaction dataset
    // --------------------------------------------------

    val cleanedTransactions = transactions
      .filter {
        case (_, _, _, amount, status) =>
          amount > 0 && status == "SUCCESS"
      }

    // --------------------------------------------------
    // 3. Cache the cleaned dataset
    // --------------------------------------------------

    cleanedTransactions.cache()

    println("\n=== Cache Example ===")
    println(
      s"Storage Level before action: ${cleanedTransactions.getStorageLevel}"
    )

    // First action materializes the cache
    val cleanedCount = cleanedTransactions.count()

    println(s"Cleaned transactions: $cleanedCount")
    println(
      s"Storage Level after action: ${cleanedTransactions.getStorageLevel}"
    )

    // --------------------------------------------------
    // 4. Report 1 - Revenue by transaction type
    // --------------------------------------------------

    val revenueByType = cleanedTransactions
      .map {
        case (_, _, transactionType, amount, _) =>
          (transactionType, amount)
      }
      .reduceByKey(_ + _)

    println("\n=== Report 1: Revenue by Transaction Type ===")
    revenueByType.collect().sortBy(_._1).foreach(println)

    // --------------------------------------------------
    // 5. Report 2 - Successful transactions by type
    // --------------------------------------------------

    val countByType = cleanedTransactions
  .map {
    case (_, _, transactionType, _, _) =>
      (transactionType, 1)
  }
      .reduceByKey(_ + _)

    println("\n=== Report 2: Successful Transactions by Type ===")
    countByType.collect().sortBy(_._1).foreach(println)

    // --------------------------------------------------
    // 6. Report 3 - Top accounts by transaction count
    // --------------------------------------------------

    val topAccounts = cleanedTransactions
      .map {
        case (_, accountId, _, _, _) =>
          (accountId, 1)
      }
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
      .take(10)

    println("\n=== Report 3: Top Accounts ===")
    topAccounts.foreach(println)

    // --------------------------------------------------
    // 7. Compare cache and persist
    // --------------------------------------------------

    println("\n=== Cache vs Persist ===")

    val cacheRDD = sc.parallelize(1 to 10000, 4)
    cacheRDD.cache()
    cacheRDD.count()

    println(
      s"cache() storage level: ${cacheRDD.getStorageLevel}"
    )

    val persistRDD = sc.parallelize(1 to 10000, 4)
    persistRDD.persist(StorageLevel.MEMORY_AND_DISK)
    persistRDD.count()

    println(
      s"persist(MEMORY_AND_DISK) storage level: ${persistRDD.getStorageLevel}"
    )

    // --------------------------------------------------
    // 8. Experiment with another storage level
    // --------------------------------------------------

    val diskRDD = sc.parallelize(1 to 10000, 4)

    diskRDD.persist(StorageLevel.DISK_ONLY)
    diskRDD.count()

    println(
      s"persist(DISK_ONLY) storage level: ${diskRDD.getStorageLevel}"
    )

    // --------------------------------------------------
    // 9. When caching can hurt performance
    // --------------------------------------------------

    println("\n=== When Caching Can Hurt ===")
    println("Caching can hurt when:")
    println("- Dataset is used only once")
    println("- Dataset is very large")
    println("- Available executor memory is low")
    println("- Cache causes memory pressure or eviction")

    // --------------------------------------------------
    // 10. Clean up cached RDDs
    // --------------------------------------------------

    cleanedTransactions.unpersist()
    cacheRDD.unpersist()
    persistRDD.unpersist()
    diskRDD.unpersist()

    println("\n=== Day 12 Completed Successfully ===")

    sc.stop()
  }
}
