import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkCatalogPractice {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day21-Spark-Catalog")
      .master("local[2]")
      .config("spark.sql.warehouse.dir", "spark-warehouse")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // 1. Read hotel and booking data
    val hotels = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/hotels.csv")

    val bookingsRaw = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/hotel_bookings.csv")

    val bookings = bookingsRaw
      .withColumn("nights", col("nights").cast("int"))
      .withColumn(
        "price_per_night",
        col("price_per_night").cast("double")
      )
      .withColumn(
        "booking_date",
        to_date(col("booking_date"), "yyyy-MM-dd")
      )
      .withColumn(
        "revenue",
        when(
          col("status") === "CONFIRMED",
          col("nights") * col("price_per_night")
        ).otherwise(lit(0.0))
      )

    println("\n=== HOTELS ===")
    hotels.show(false)

    println("\n=== HOTEL BOOKINGS ===")
    bookings.show(false)

    // 2. Create a database
    spark.sql("CREATE DATABASE IF NOT EXISTS hotel_analytics")
    spark.sql("USE hotel_analytics")

    println("\n=== DATABASES ===")
    spark.catalog.listDatabases().show(false)

    // 3. Register persistent tables in the catalog
    hotels.write
      .mode("overwrite")
      .format("parquet")
      .saveAsTable("hotel_analytics.hotels")

    bookings.write
      .mode("overwrite")
      .format("parquet")
      .saveAsTable("hotel_analytics.bookings")

    println("\n=== TABLES IN HOTEL_ANALYTICS ===")
    spark.catalog.listTables("hotel_analytics").show(false)

    // 4. Create temporary views
    hotels.createOrReplaceTempView("hotels_temp")
    bookings.createOrReplaceTempView("bookings_temp")

    println("\n=== TEMPORARY VIEWS ===")
    spark.catalog.listTables()
      .filter(col("name").isin("hotels_temp", "bookings_temp"))
      .show(false)

    // 5. Query persistent tables using SQL
    println("\n=== REGISTERED TABLE QUERY ===")
    spark.sql(
      """
        |SELECT booking_id, customer_name, hotel_id,
        |       status, revenue
        |FROM hotel_analytics.bookings
        |ORDER BY booking_id
        |""".stripMargin
    ).show(false)

    // 6. Join temporary views and calculate hotel metrics
    println("\n=== HOTEL BOOKING ANALYTICS ===")
    spark.sql(
      """
        |SELECT
        |  b.hotel_id,
        |  COALESCE(h.hotel_name, 'Unknown Hotel') AS hotel_name,
        |  COUNT(*) AS total_bookings,
        |  SUM(CASE WHEN b.status = 'CONFIRMED'
        |           THEN 1 ELSE 0 END) AS confirmed_bookings,
        |  SUM(CASE WHEN b.status = 'CANCELLED'
        |           THEN 1 ELSE 0 END) AS cancelled_bookings,
        |  ROUND(SUM(b.revenue), 2) AS total_revenue
        |FROM bookings_temp b
        |LEFT JOIN hotels_temp h
        |  ON b.hotel_id = h.hotel_id
        |GROUP BY b.hotel_id, h.hotel_name
        |ORDER BY b.hotel_id
        |""".stripMargin
    ).show(false)

    // 7. Inspect schema
    println("\n=== BOOKINGS SCHEMA ===")
    spark.table("hotel_analytics.bookings").printSchema()

    // 8. Inspect table columns and metadata
    println("\n=== CATALOG COLUMNS ===")
    spark.catalog.listColumns("hotel_analytics.bookings").show(false)

    println("\n=== TABLE DESCRIPTION ===")
    spark.sql(
      "DESCRIBE TABLE hotel_analytics.bookings"
    ).show(false)

    println("\n=== TABLE EXISTS ===")
    println(
      spark.catalog.tableExists("hotel_analytics.bookings")
    )

    println("\n=== DAY 21 SPARK CATALOG COMPLETED ===")

    spark.stop()
  }
}
