import org.apache.spark.{SparkConf, SparkContext}

object PairRDDPractice {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day9-PairRDD")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    // --------------------------------------------------
    // 1. Create key-value RDD
    // --------------------------------------------------

    val sales = sc.parallelize(Seq(
      ("Laptop", "Electronics", "IT", 2, 50000.0),
      ("Mouse", "Accessories", "IT", 5, 1000.0),
      ("Keyboard", "Accessories", "IT", 3, 2000.0),
      ("Laptop", "Electronics", "Sales", 1, 50000.0),
      ("Phone", "Electronics", "Sales", 4, 20000.0),
      ("Chair", "Furniture", "HR", 5, 5000.0),
      ("Desk", "Furniture", "HR", 2, 10000.0)
    ))

    // --------------------------------------------------
    // 2. reduceByKey
    // --------------------------------------------------

    val productRevenue = sales
      .map { case (product, _, _, quantity, price) =>
        (product, quantity * price)
      }
      .reduceByKey(_ + _)

    println("\n=== Revenue by Product using reduceByKey ===")
    productRevenue.collect().sortBy(_._1).foreach(println)

    // --------------------------------------------------
    // 3. groupByKey
    // --------------------------------------------------

    val productQuantities = sales
      .map { case (product, _, _, quantity, _) =>
        (product, quantity)
      }
      .groupByKey()
      .mapValues(_.sum)

    println("\n=== Quantity by Product using groupByKey ===")
    productQuantities.collect().sortBy(_._1).foreach(println)

    // --------------------------------------------------
    // 4. mapValues
    // --------------------------------------------------

    val departmentSales = sales
      .map { case (_, _, department, quantity, price) =>
        (department, quantity * price)
      }

    val doubledDepartmentSales =
      departmentSales.mapValues(_ * 2)

    println("\n=== mapValues Example ===")
    doubledDepartmentSales
      .collect()
      .sortBy(_._1)
      .foreach(println)

    // --------------------------------------------------
    // 5. Revenue by Department
    // --------------------------------------------------

    val revenueByDepartment = sales
      .map { case (_, _, department, quantity, price) =>
        (department, quantity * price)
      }
      .reduceByKey(_ + _)

    println("\n=== Revenue by Department ===")
    revenueByDepartment
      .collect()
      .sortBy(_._1)
      .foreach(println)

    // --------------------------------------------------
    // 6. reduceByKey vs groupByKey performance
    // --------------------------------------------------

    val largeData = sc.parallelize(
  1 to 100000,
  4
).map { i =>
      (i % 1000, 1)
    }

    val startReduce = System.nanoTime()

    largeData
      .reduceByKey(_ + _)
      .collect()

    val reduceTime =
      (System.nanoTime() - startReduce) / 1e6

    val startGroup = System.nanoTime()

    largeData
      .groupByKey()
      .mapValues(_.sum)
      .collect()

    val groupTime =
      (System.nanoTime() - startGroup) / 1e6

    println("\n=== Performance Comparison ===")
    println(f"reduceByKey time: $reduceTime%.2f ms")
    println(f"groupByKey time:  $groupTime%.2f ms")

    // --------------------------------------------------
    // 7. Bank transactions by Account ID
    // --------------------------------------------------

    val bankTransactions = sc.parallelize(Seq(
      ("ACC1001", 5000.0),
      ("ACC1002", 3000.0),
      ("ACC1001", 2500.0),
      ("ACC1003", 7000.0),
      ("ACC1002", 1500.0),
      ("ACC1001", 1000.0),
      ("ACC1003", 2000.0)
    ))

    val totalByAccount =
      bankTransactions.reduceByKey(_ + _)

    println("\n=== Bank Transactions by Account ID ===")
    totalByAccount
      .collect()
      .sortBy(_._1)
      .foreach(println)

    sc.stop()
  }
}
