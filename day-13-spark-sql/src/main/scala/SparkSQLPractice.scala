import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkSQLPractice {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day13-Spark-SQL")
      .master("local[4]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // --------------------------------------------------
    // 1. Read Customer Data from CSV
    // --------------------------------------------------

    val csvCustomers = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/customers.csv")

    println("\n=== CSV Data ===")
    csvCustomers.show(false)

    println("\n=== CSV Schema ===")
    csvCustomers.printSchema()

    // --------------------------------------------------
    // 2. Read Customer Data from JSON
    // --------------------------------------------------

    val jsonCustomers = spark.read
      .option("inferSchema", "true")
      .json("data/customers.json")

    println("\n=== JSON Data ===")
    jsonCustomers.show(false)

    println("\n=== JSON Schema ===")
    jsonCustomers.printSchema()

    // --------------------------------------------------
    // 3. Make both DataFrames use the same schema
    // --------------------------------------------------

    val csvClean = csvCustomers
      .select(
        col("customer_id").cast("string"),
        col("name").cast("string"),
        col("city").cast("string"),
        col("age").cast("int"),
        col("total_spend").cast("double"),
        col("status").cast("string")
      )

    val jsonClean = jsonCustomers
      .select(
        col("customer_id").cast("string"),
        col("name").cast("string"),
        col("city").cast("string"),
        col("age").cast("int"),
        col("total_spend").cast("double"),
        col("status").cast("string")
      )

    // Combine CSV and JSON data
    val customers = csvClean.unionByName(jsonClean)

    println("\n=== Combined Customer Data ===")
    customers.show(false)

    // --------------------------------------------------
    // 4. Select columns
    // --------------------------------------------------

    println("\n=== Selected Columns ===")

    customers
      .select("customer_id", "name", "city", "total_spend")
      .show(false)

    // --------------------------------------------------
    // 5. Filter columns
    // --------------------------------------------------

    println("\n=== Active Customers ===")

    customers
      .filter(col("status") === "ACTIVE")
      .show(false)

    println("\n=== High Value Customers ===")

    customers
      .filter(col("total_spend") > 50000)
      .select("customer_id", "name", "total_spend")
      .show(false)

    // --------------------------------------------------
    // 6. withColumn and expressions
    // --------------------------------------------------

    val enrichedCustomers = customers
      .withColumn(
        "spend_category",
        when(col("total_spend") >= 75000, "PREMIUM")
          .when(col("total_spend") >= 50000, "HIGH")
          .otherwise("STANDARD")
      )
      .withColumn(
        "estimated_monthly_spend",
        round(col("total_spend") / 12, 2)
      )

    println("\n=== Enriched Customer Data ===")
    enrichedCustomers.show(false)

    // --------------------------------------------------
    // 7. Register Temporary View
    // --------------------------------------------------

    enrichedCustomers.createOrReplaceTempView("customers_view")

    // --------------------------------------------------
    // 8. SQL Query
    // --------------------------------------------------

    println("\n=== SQL: Active Customers ===")

    spark.sql(
      """
        |SELECT customer_id, name, city, total_spend
        |FROM customers_view
        |WHERE status = 'ACTIVE'
        |ORDER BY total_spend DESC
        |""".stripMargin
    ).show(false)

    // --------------------------------------------------
    // 9. Customer Analytics Report
    // --------------------------------------------------

    println("\n=== Customer Analytics Report ===")

    val analyticsReport = spark.sql(
      """
        |SELECT
        |    city,
        |    COUNT(*) AS customer_count,
        |    ROUND(AVG(total_spend), 2) AS average_spend,
        |    ROUND(SUM(total_spend), 2) AS total_city_spend
        |FROM customers_view
        |GROUP BY city
        |ORDER BY total_city_spend DESC
        |""".stripMargin
    )

    analyticsReport.show(false)

    // --------------------------------------------------
    // 10. Overall Summary
    // --------------------------------------------------

    println("\n=== Overall Summary ===")

    spark.sql(
      """
        |SELECT
        |    COUNT(*) AS total_customers,
        |    SUM(CASE WHEN status = 'ACTIVE' THEN 1 ELSE 0 END)
        |        AS active_customers,
        |    ROUND(AVG(total_spend), 2) AS average_spend,
        |    ROUND(SUM(total_spend), 2) AS total_spend
        |FROM customers_view
        |""".stripMargin
    ).show(false)

    println("\n=== Day 13 Completed Successfully ===")

    spark.stop()
  }
}
