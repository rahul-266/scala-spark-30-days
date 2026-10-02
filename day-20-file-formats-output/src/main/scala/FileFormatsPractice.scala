import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.hadoop.fs.Path

object FileFormatsPractice {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day20-File-Formats-Output")
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // 1. Define the input schema
    val salesSchema = StructType(Seq(
      StructField("sale_id", StringType, false),
      StructField("product_id", StringType, true),
      StructField("region", StringType, true),
      StructField("quantity", IntegerType, true),
      StructField("unit_price", DoubleType, true),
      StructField("sale_timestamp", StringType, true)
    ))

    // 2. Read CSV
    val rawSales = spark.read
      .option("header", "true")
      .schema(salesSchema)
      .csv("data/daily_sales.csv")

    val sales = rawSales
      .withColumn(
        "sale_time",
        to_timestamp(
          col("sale_timestamp"),
          "yyyy-MM-dd'T'HH:mm:ss"
        )
      )
      .filter(col("sale_time").isNotNull)
      .withColumn(
        "revenue",
        round(col("quantity") * col("unit_price"), 2)
      )
      .withColumn("year", year(col("sale_time")))
      .withColumn("month", month(col("sale_time")))
      .withColumn("day", dayofmonth(col("sale_time")))

    println("\n=== ORIGINAL SALES DATA ===")
    sales.orderBy("sale_id").show(false)

    println(s"Total sales records: ${sales.count()}")

    // 3. CSV write and read
    val csvPath = "output/sales_csv"

    sales.write
      .mode("overwrite")
      .option("header", "true")
      .csv(csvPath)

    val csvRead = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(csvPath)

    println("\n=== CSV READ BACK ===")
    csvRead.show(5, false)

    // 4. JSON write and read
    val jsonPath = "output/sales_json"

    sales.write
      .mode("overwrite")
      .json(jsonPath)

    val jsonRead = spark.read.json(jsonPath)

    println("\n=== JSON READ BACK ===")
    jsonRead.show(5, false)

    // 5. Parquet write and read
    val parquetPath = "output/sales_parquet"

    sales.write
      .mode("overwrite")
      .parquet(parquetPath)

    val parquetRead = spark.read.parquet(parquetPath)

    println("\n=== PARQUET READ BACK ===")
    parquetRead.show(5, false)

    // 6. Daily sales report
    val dailyReport = sales
      .groupBy("year", "month", "day")
      .agg(
        count("*").as("sales_count"),
        sum("quantity").as("total_quantity"),
        round(sum("revenue"), 2).as("total_revenue")
      )
      .orderBy("year", "month", "day")

    println("\n=== DAILY SALES REPORT ===")
    dailyReport.show(false)

    // 7. Repartition before writing
    val partitionedPath = "output/sales_partitioned"

    val partitionedSales = sales
      .repartition(4, col("year"), col("month"), col("day"))

    println(
      s"Partitions before writing: ${partitionedSales.rdd.getNumPartitions}"
    )

    partitionedSales.write
      .mode("overwrite")
      .partitionBy("year", "month", "day")
      .parquet(partitionedPath)

    // 8. Read partitioned output
    val partitionedRead = spark.read.parquet(partitionedPath)

    println("\n=== PARTITIONED PARQUET DATA ===")
    partitionedRead
      .select("sale_id", "year", "month", "day", "revenue")
      .orderBy("sale_id")
      .show(false)

    // 9. Count actual data files
    def countPartFiles(pathString: String): Long = {
      val path = new Path(pathString)
      val fs = path.getFileSystem(
        spark.sparkContext.hadoopConfiguration
      )

      if (!fs.exists(path)) {
        0L
      } else {
        val files = fs.listFiles(path, true)
        var count = 0L

        while (files.hasNext) {
          if (files.next().getPath.getName.startsWith("part-")) {
            count += 1
          }
        }

        count
      }
    }

    println("\n=== DATA FILE COUNTS ===")
    println("CSV files: " + countPartFiles(csvPath))
    println("JSON files: " + countPartFiles(jsonPath))
    println("Parquet files: " + countPartFiles(parquetPath))
    println(
      "Partitioned Parquet files: " +
        countPartFiles(partitionedPath)
    )

    println("\n=== DAY 20 COMPLETED SUCCESSFULLY ===")

    spark.stop()
  }
}
