import org.apache.spark.{SparkConf, SparkContext}

object Day05 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day05-Transformations-Actions")
      .setMaster("local[4]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    println("=== Day 05 - Transformations and Actions ===")

    // Part 1: Transformations
    val numbers = sc.parallelize(Seq(1, 2, 3, 3, 4, 5))
    val mapped = numbers.map(_ * 2)
    val filtered = numbers.filter(_ > 3)
    val words = sc.parallelize(Seq("Spark RDD", "Scala Spark"))
    val flatMapped = words.flatMap(_.split(" "))
    val distinctNumbers = numbers.distinct()

    val firstRDD = sc.parallelize(Seq("Spark", "Scala"))
    val secondRDD = sc.parallelize(Seq("RDD", "Spark"))
    val unionRDD = firstRDD.union(secondRDD)

    println("\n--- Transformations ---")
    println("map: " + mapped.collect().mkString(", "))
    println("filter: " + filtered.collect().mkString(", "))
    println("flatMap: " + flatMapped.collect().mkString(", "))
    println("distinct: " + distinctNumbers.collect().mkString(", "))
    println("union: " + unionRDD.collect().mkString(", "))

    // Part 2: Actions
    println("\n--- Actions ---")
    println("count: " + numbers.count())
    println("collect: " + numbers.collect().mkString(", "))
    println("first: " + numbers.first())
    println("take(3): " + numbers.take(3).mkString(", "))
    println("reduce: " + numbers.reduce(_ + _))

    // Part 3: Lazy evaluation
    println("\n--- Lazy Evaluation ---")
    val lazyRDD = numbers.filter(_ % 2 == 0).map(_ * 10)
    println("Transformation defined; execution happens when an action is called.")
    println("Lazy result: " + lazyRDD.collect().mkString(", "))

    // Part 4: Log analyzer
    println("\n--- Log Analyzer ---")
    val logLines = sc.textFile("data/logs.txt")
    val errorLogs = logLines.filter(_.contains("ERROR"))

    println("ERROR messages:")
    errorLogs.collect().foreach(println)
    println("Total ERROR messages: " + errorLogs.count())

    println("\n=== Day 05 Completed ===")

    sc.stop()
  }
}
