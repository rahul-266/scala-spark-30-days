import org.apache.spark.sql.SparkSession

object Day03 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day03-Spark-Setup")
      .master("local[4]")
      .getOrCreate()

    val sc = spark.sparkContext

    println("=== Spark Application ===")
    println(s"Application Name: ${sc.appName}")
    println(s"Master: ${sc.master}")
    println(s"Default Parallelism: ${sc.defaultParallelism}")

    val filePath = "data/sample.txt"
    val lines = sc.textFile(filePath)

    println("\n=== File Contents ===")
    lines.collect().foreach(println)

    println("\n=== Number of Lines ===")
    println(lines.count())

    spark.stop()
  }
}
