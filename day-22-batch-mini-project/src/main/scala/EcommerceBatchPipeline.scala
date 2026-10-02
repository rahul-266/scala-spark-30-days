import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.Path

object EcommerceBatchPipeline {

  def main(args: Array[String]): Unit = {

    val outputDir = new java.io.File("output").getAbsolutePath
    val warehouseDir =
      new java.io.File("spark-warehouse").getAbsolutePath

    val spark = SparkSession.builder()
      .appName("Day22-Ecommerce-Batch-Pipeline")
      .master("local[2]")
      .config("spark.sql.ansi.enabled", "false")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.hadoop.fs.defaultFS", "file:///")
      .config("spark.sql.warehouse.dir", "file://" + warehouseDir)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // 1. Read raw transactions
    val raw = spark.read
      .option("header", "true")
      .csv("data/raw_transactions.csv")

    val customers = spark.read
      .option("header", "true")
      .csv("data/customers.csv")
      .withColumn("customer_id", trim(col("customer_id")))

    val products = spark.read
      .option("header", "true")
      .csv("data/products.csv")
      .withColumn("product_id", trim(col("product_id")))

    println("\n=== RAW TRANSACTIONS ===")
    raw.show(false)

    // 2. Clean and parse transactions
    val parsed = raw
      .withColumn("transaction_id", trim(col("transaction_id")))
      .withColumn("customer_id", trim(col("customer_id")))
      .withColumn("product_id", trim(col("product_id")))
      .withColumn("quantity_num", col("quantity").cast("int"))
      .withColumn("unit_price_num", col("unit_price").cast("double"))
      .withColumn(
        "transaction_ts",
        to_timestamp(
          trim(col("transaction_timestamp")),
          "yyyy-MM-dd'T'HH:mm:ss"
        )
      )

    val validated = parsed.withColumn(
      "rejection_reason",
      when(
        col("transaction_id").isNull ||
          (col("transaction_id") === ""),
        lit("MISSING_TRANSACTION_ID")
      ).when(
        col("customer_id").isNull ||
          (col("customer_id") === ""),
        lit("MISSING_CUSTOMER_ID")
      ).when(
        col("product_id").isNull ||
          (col("product_id") === ""),
        lit("MISSING_PRODUCT_ID")
      ).when(
        col("quantity_num").isNull ||
          (col("quantity_num") <= 0),
        lit("INVALID_QUANTITY")
      ).when(
        col("unit_price_num").isNull ||
          (col("unit_price_num") <= 0),
        lit("INVALID_UNIT_PRICE")
      ).when(
        col("transaction_ts").isNull,
        lit("INVALID_TIMESTAMP")
      ).otherwise(lit(null).cast("string"))
    )

    val rejected = validated
      .filter(col("rejection_reason").isNotNull)
      .select(
        col("transaction_id"),
        col("customer_id"),
        col("product_id"),
        col("quantity"),
        col("unit_price"),
        col("transaction_timestamp"),
        col("rejection_reason")
      )

    val clean = validated
      .filter(col("rejection_reason").isNull)
      .select(
        col("transaction_id"),
        col("customer_id"),
        col("product_id"),
        col("quantity_num").as("quantity"),
        col("unit_price_num").as("unit_price"),
        col("transaction_ts")
      )
      .dropDuplicates("transaction_id")

    println("\n=== REJECTED INVALID TRANSACTIONS ===")
    rejected.show(false)

    println("\n=== CLEAN TRANSACTIONS ===")
    clean.show(false)

    // 3. Join transactions with customers and products
    val joined = clean.alias("t")
      .join(
        customers.alias("c"),
        col("t.customer_id") === col("c.customer_id"),
        "left"
      )
      .join(
        products.alias("p"),
        col("t.product_id") === col("p.product_id"),
        "left"
      )
      .select(
        col("t.transaction_id").as("transaction_id"),
        col("t.customer_id").as("customer_id"),
        col("c.customer_name").as("customer_name"),
        col("c.region").as("region"),
        col("t.product_id").as("product_id"),
        col("p.product_name").as("product_name"),
        col("p.category").as("category"),
        col("t.quantity").as("quantity"),
        col("t.unit_price").as("unit_price"),
        col("t.transaction_ts").as("transaction_ts")
      )
      .withColumn(
        "revenue",
        round(col("quantity") * col("unit_price"), 2)
      )
      .withColumn("transaction_date", to_date(col("transaction_ts")))
      .withColumn("year", year(col("transaction_date")))
      .withColumn("month", month(col("transaction_date")))
      .withColumn("day", dayofmonth(col("transaction_date")))

    // 4. Handle unmatched customer/product references
    val unmatched = joined.filter(
      col("customer_name").isNull || col("product_name").isNull
    )

    val enriched = joined.filter(
      col("customer_name").isNotNull &&
        col("product_name").isNotNull
    )

    println("\n=== UNMATCHED REFERENCE RECORDS ===")
    unmatched.show(false)

    println("\n=== ENRICHED TRANSACTIONS ===")
    enriched.orderBy("transaction_id").show(false)

    // 5. Aggregate daily sales by region and category
    val dailySales = enriched
      .groupBy("year", "month", "day", "region", "category")
      .agg(
        count(lit(1)).as("transaction_count"),
        sum("quantity").as("total_quantity"),
        round(sum("revenue"), 2).as("total_revenue")
      )

    println("\n=== DAILY SALES AGGREGATION ===")
    dailySales.orderBy(
      "year", "month", "day", "region", "category"
    ).show(false)

    // 6. Prepare local output paths
    val outputRoot = "file://" + outputDir
    val rejectedPath = outputRoot + "/rejected_transactions"
    val cleanPath = outputRoot + "/clean_transactions"
    val unmatchedPath = outputRoot + "/unmatched_references"
    val enrichedPath = outputRoot + "/enriched_transactions"
    val dailyPath = outputRoot + "/daily_sales_partitioned"

    // 7. Write cleaned and rejected transactions
    rejected.write
      .mode("overwrite")
      .parquet(rejectedPath)

    clean.write
      .mode("overwrite")
      .parquet(cleanPath)

    unmatched.write
      .mode("overwrite")
      .parquet(unmatchedPath)

    enriched.write
      .mode("overwrite")
      .parquet(enrichedPath)

    // 8. Repartition and write partitioned daily sales
    val repartitioned = dailySales
      .repartition(4, col("year"), col("month"), col("day"))

    println(
      s"\nPartitions before output: ${repartitioned.rdd.getNumPartitions}"
    )

    repartitioned.write
      .mode("overwrite")
      .partitionBy("year", "month", "day")
      .parquet(dailyPath)

    // 9. Count actual Parquet part files
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
          val file = files.next()
          if (file.getPath.getName.startsWith("part-")) {
            count += 1
          }
        }

        count
      }
    }

    // 10. Final pipeline summary
    println("\n=== PIPELINE SUMMARY ===")
    println(s"Raw records: ${raw.count()}")
    println(s"Invalid records: ${rejected.count()}")
    println(s"Clean records: ${clean.count()}")
    println(s"Unmatched references: ${unmatched.count()}")
    println(s"Enriched records: ${enriched.count()}")

    println("\n=== OUTPUT FILE COUNTS ===")
    println(s"Rejected Parquet files: ${countPartFiles(rejectedPath)}")
    println(s"Clean Parquet files: ${countPartFiles(cleanPath)}")
    println(s"Enriched Parquet files: ${countPartFiles(enrichedPath)}")
    println(s"Daily partitioned Parquet files: ${countPartFiles(dailyPath)}")

    println("\n=== DAY 22 BATCH PIPELINE COMPLETED ===")

    spark.stop()
  }
}
