import org.apache.spark.{SparkConf, SparkContext}

object Day07 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day07-Immutability-Lineage-Fault-Tolerance")
      .setMaster("local[4]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    println("=== Day 07 - Immutability, Lineage and Fault Tolerance ===")

    val inputRDD = sc.textFile("data/sample.txt")

    // Multi-step transformation chain.
    val wordsRDD = inputRDD.flatMap(_.split(" "))
    val sparkWordsRDD = wordsRDD.filter(_.equalsIgnoreCase("spark"))
    val resultRDD = sparkWordsRDD.map(_.toUpperCase)

    println("\n--- Transformation Chain ---")
    println("inputRDD -> flatMap -> wordsRDD")
    println("wordsRDD -> filter -> sparkWordsRDD")
    println("sparkWordsRDD -> map -> resultRDD")

    println("\n--- Original RDD ---")
    inputRDD.collect().foreach(println)

    println("\n--- Final Result ---")
    resultRDD.collect().foreach(println)

    println("\n--- Immutability ---")
    println("inputRDD is unchanged after creating wordsRDD, sparkWordsRDD and resultRDD.")
    println(s"Original input partitions: ${inputRDD.getNumPartitions}")
    println(s"Final result partitions: ${resultRDD.getNumPartitions}")

    println("\n--- Lineage ---")
    println(resultRDD.toDebugString)

    println("\n--- Fault Tolerance Scenario ---")
    println("Suppose one executor is lost and a partition of resultRDD is lost.")
    println("Spark uses the RDD lineage to recompute the lost partition.")
    println("It does not need to recompute unaffected partitions.")
    println("For this narrow transformation chain, Spark can read the required parent partition and reapply flatMap, filter and map.")

    println("\n=== Day 07 Completed ===")

    sc.stop()
  }
}
