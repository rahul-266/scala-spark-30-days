import org.apache.spark.{SparkConf, SparkContext}

object Day08 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day08-DAG-Spark-Execution")
      .setMaster("local[4]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    println("=== Day 08 - DAG and Spark Execution ===")

    val lines = sc.textFile("data/sales.txt")

    // Several transformations followed by an action.
    val amounts = lines
      .map(_.split(","))
      .filter(fields => fields.length == 3)
      .map(fields => (fields(0), fields(1).toInt * fields(2).toDouble))

    val totalRevenue = amounts
      .mapValues(identity)
      .reduceByKey(_ + _)

    println("\n--- Job 1: reduceByKey pipeline ---")
    totalRevenue.collect().sortBy(_._1).foreach { case (product, revenue) =>
      println(f"$product%-12s $revenue%.2f")
    }

    val productNames = lines
      .map(_.split(",")(0))
      .filter(_.nonEmpty)

    println("\n--- Job 2: count action ---")
    println(s"Product records: ${productNames.count()}")

    println("\n--- Execution Concepts ---")
    println("Job: created when an action such as collect() or count() is called")
    println("Narrow transformations: map, filter, mapValues")
    println("Wide transformation: reduceByKey")
    println("Shuffle boundary: reduceByKey")
    println("Expected stages for the reduceByKey pipeline: 2")
    println("Stage 1: map -> filter -> map -> mapValues")
    println("Stage 2: reduceByKey -> collect")
    println("Tasks: one task per partition within a stage")

    println("\n--- Narrow vs Wide ---")
    println("Narrow: child partition depends on one parent partition")
    println("Wide: child partition can depend on multiple parent partitions")

    println("\n=== Day 08 Completed ===")

    sc.stop()
  }
}
