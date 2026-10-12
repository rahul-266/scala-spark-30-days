package com.capstone

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object CapstoneApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("FinalSparkCapstone")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val inputPath =
      args.headOption.getOrElse("data/transactions.csv")

    // 1. Read raw transactions
    val raw = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(inputPath)

    // 2. Clean invalid records
    val cleaned = raw
      .filter(
        col("transaction_id").isNotNull &&
        col("account_id").isNotNull &&
        col("amount").isNotNull &&
        col("amount") > 0
      )
      .dropDuplicates("transaction_id")
      .persist(StorageLevel.MEMORY_AND_DISK)

    println("\n========== DATA QUALITY ==========")
    println(s"Raw records: ${raw.count()}")
    println(s"Clean records: ${cleaned.count()}")
    println(s"Rejected records: ${raw.count() - cleaned.count()}")

    // 3. Pair RDD: count transactions per account
    val accountCounts = cleaned.rdd
      .map(row => (row.getAs[String]("account_id"), 1))
      .reduceByKey(_ + _)

    println("\n========== ACCOUNT COUNTS ==========")
    accountCounts.collect().foreach {
      case (account, count) =>
        println(s"$account -> $count transactions")
    }

    // 4. Successful transaction analytics
    val successful = cleaned
      .filter(upper(col("status")) === "SUCCESS")

    val accountSummary = successful
      .groupBy("account_id")
      .agg(
        count("*").as("transaction_count"),
        sum("amount").as("total_amount"),
        avg("amount").as("average_amount"),
        max("amount").as("maximum_amount")
      )
      .orderBy(desc("total_amount"))

    println("\n========== ACCOUNT SUMMARY ==========")
    accountSummary.show(false)

    // 5. Detect potentially suspicious activity
    val suspicious = successful
      .filter(col("amount") >= 15000)
      .withColumn(
        "alert_reason",
        lit("High-value transaction: amount >= 15000")
      )

    println("\n========== HIGH-VALUE ALERTS ==========")
    suspicious.show(false)

    // 6. City-wise analysis
    val citySummary = successful
      .groupBy("city")
      .agg(
        count("*").as("successful_transactions"),
        sum("amount").as("total_amount")
      )
      .orderBy(desc("total_amount"))

    println("\n========== CITY SUMMARY ==========")
    citySummary.show(false)

    // 7. Spark SQL
    cleaned.createOrReplaceTempView("transactions")

    val sqlReport = spark.sql("""
      SELECT
        account_id,
        COUNT(*) AS total_transactions,
        SUM(CASE WHEN UPPER(status) = 'SUCCESS'
                 THEN amount ELSE 0 END) AS successful_amount,
        SUM(CASE WHEN UPPER(status) = 'FAILED'
                 THEN 1 ELSE 0 END) AS failed_transactions
      FROM transactions
      GROUP BY account_id
      ORDER BY successful_amount DESC
    """)

    println("\n========== SPARK SQL REPORT ==========")
    sqlReport.show(false)

    // 8. Save reports
    accountSummary.write.mode("overwrite")
      .option("header", "true")
      .csv("output/account_summary")

    suspicious.write.mode("overwrite")
      .option("header", "true")
      .csv("output/suspicious_alerts")

    citySummary.write.mode("overwrite")
      .option("header", "true")
      .csv("output/city_summary")

    sqlReport.write.mode("overwrite")
      .option("header", "true")
      .csv("output/sql_report")

    cleaned.unpersist()

    println("\nCapstone batch pipeline completed.")
    spark.stop()
  }
}
