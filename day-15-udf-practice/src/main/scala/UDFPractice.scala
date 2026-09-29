import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object UDFPractice {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day15-UDF-Practice")
      .master("local[4]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // 1. Read employee data
    val employees = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("data/employees.csv")
  .withColumn("salary", col("salary").cast("double"))

    println("\n=== Original Employee Data ===")
    employees.show(false)

    // 2. Register salary classification UDF
    spark.udf.register(
      "salary_band_udf",
      (salary: Double) => {
        if (salary >= 100000) "HIGH"
        else if (salary >= 50000) "MEDIUM"
        else "LOW"
      }
    )

    // 3. Apply registered UDF using withColumn
    val udfSalaryReport = employees
      .withColumn(
        "salary_band",
        callUDF("salary_band_udf", col("salary"))
      )

    println("\n=== Salary Classification Using UDF ===")
    udfSalaryReport.show(false)

    // 4. Classify salary using built-in Spark functions
    val builtinSalaryReport = employees
      .withColumn(
        "salary_band",
        when(col("salary") >= 100000, "HIGH")
          .when(col("salary") >= 50000, "MEDIUM")
          .otherwise("LOW")
      )

    println("\n=== Salary Classification Using Built-in Functions ===")
    builtinSalaryReport.show(false)

    // 5. Read customer transaction data
    val transactions = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("data/customer_transactions.csv")
  .withColumn("amount", col("amount").cast("double"))

    println("\n=== Customer Transactions ===")
    transactions.show(false)

    // 6. Register customer risk UDF
    spark.udf.register(
      "customer_risk_udf",
      (amount: Double) => {
        if (amount >= 100000) "HIGH_RISK"
        else if (amount >= 50000) "MEDIUM_RISK"
        else "LOW_RISK"
      }
    )

    // 7. Apply risk UDF
    val riskReport = transactions
      .withColumn(
        "risk_category",
        callUDF("customer_risk_udf", col("amount"))
      )

    println("\n=== Customer Risk Report Using UDF ===")
    riskReport.show(false)

    // 8. Compare with built-in Spark functions
    val builtinRiskReport = transactions
      .withColumn(
        "risk_category",
        when(col("amount") >= 100000, "HIGH_RISK")
          .when(col("amount") >= 50000, "MEDIUM_RISK")
          .otherwise("LOW_RISK")
      )

    println("\n=== Customer Risk Report Using Built-in Functions ===")
    builtinRiskReport.show(false)

    // 9. Register a temporary view for SQL
    riskReport.createOrReplaceTempView("customer_risk_view")

    println("\n=== SQL: Customer Risk Summary ===")

    spark.sql(
      """
        |SELECT
        |  risk_category,
        |  COUNT(*) AS transaction_count,
        |  ROUND(SUM(amount), 2) AS total_amount
        |FROM customer_risk_view
        |GROUP BY risk_category
        |ORDER BY risk_category
        |""".stripMargin
    ).show(false)

    // 10. Final summary
    println("\n=== Summary ===")
    println(s"Total employees: ${employees.count()}")
    println(s"Total transactions: ${transactions.count()}")

    println("\n=== Day 15 Completed Successfully ===")

    spark.stop()
  }
}
