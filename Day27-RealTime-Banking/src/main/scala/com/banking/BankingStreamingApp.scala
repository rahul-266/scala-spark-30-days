package com.banking

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.storage.StorageLevel

object BankingStreamingApp {

  def main(args: Array[String]): Unit = {

    val root = args.headOption.getOrElse(".").stripSuffix("/")
    def path(relative: String): String = s"$root/$relative"

    val master = sys.env.getOrElse("SPARK_MASTER", "local[*]")

    val spark = SparkSession.builder()
      .appName("Day27-RealTime-Banking")
      .master(master)
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val inputDir = path("data/stream_input")
    val branchesPath = path("data/reference/branches.csv")
    val riskPath = path("data/reference/account_risk.csv")
    val checkpointPath = path("checkpoints/banking")
    val metricsPath = path("output/account_metrics")
    val alertsPath = path("output/suspicious_alerts")

    // 1. Transaction event schema
    val transactionSchema = StructType(Seq(
      StructField("transaction_id", StringType, nullable = true),
      StructField("account_id", StringType, nullable = true),
      StructField("amount", DoubleType, nullable = true),
      StructField("transaction_type", StringType, nullable = true),
      StructField("branch_id", StringType, nullable = true),
      StructField("event_time", TimestampType, nullable = true)
    ))

    // 2. Read incoming JSON transaction files as a stream
    val rawTransactions = spark.readStream
      .schema(transactionSchema)
      .option("maxFilesPerTrigger", 1)
      .json(inputDir)

    // Reject records with missing identifiers, timestamp, or invalid amount
    val validTransactions = rawTransactions
      .filter(
        col("transaction_id").isNotNull &&
        col("account_id").isNotNull &&
        col("branch_id").isNotNull &&
        col("event_time").isNotNull &&
        col("amount").isNotNull &&
        col("amount") > 0
      )
      .withWatermark("event_time", "2 minutes")

    // 3. Load small static reference datasets
    val branches = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(branchesPath)

    val accountRisk = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(riskPath)

    // 4. Broadcast joins: avoid shuffling the transaction stream
    // to join these small reference datasets.
    val enrichedTransactions = validTransactions
      .join(broadcast(branches), Seq("branch_id"), "left")
      .join(broadcast(accountRisk), Seq("account_id"), "left")
      .withColumn(
        "branch_name",
        coalesce(col("branch_name"), lit("UNKNOWN"))
      )
      .withColumn(
        "region",
        coalesce(col("region"), lit("UNKNOWN"))
      )
      .withColumn(
        "risk_level",
        coalesce(col("risk_level"), lit("UNKNOWN"))
      )
      .withColumn(
        "high_value_limit",
        coalesce(col("high_value_limit"), lit(50000.0))
      )

    // 5. Account-level aggregation over event-time windows
    val accountMetrics = enrichedTransactions
      .groupBy(
        window(col("event_time"), "1 minute").as("window"),
        col("account_id"),
        col("branch_id"),
        col("branch_name"),
        col("region"),
        col("risk_level"),
        col("high_value_limit")
      )
      .agg(
        count("transaction_id").as("transaction_count"),
        round(sum("amount"), 2).as("total_amount"),
        round(avg("amount"), 2).as("average_amount"),
        max("amount").as("max_transaction")
      )
      // A burst is 3+ transactions in one window.
      // High-risk accounts also alert when total exceeds their limit.
      .withColumn(
        "suspicious_burst",
        when(
          col("transaction_count") >= 3 ||
          (
            col("risk_level") === "HIGH" &&
            col("total_amount") >= col("high_value_limit")
          ),
          lit(true)
        ).otherwise(lit(false))
      )
      .withColumn("processing_time", current_timestamp())
      // Explicit partitioning for downstream parallel processing.
      .repartition(4, col("account_id"))

    // 6. foreachBatch is a micro-batch sink.
    // Persist the aggregated batch while it is used for reports and alerts.
    val query = accountMetrics.writeStream
      .outputMode("update")
      .option("checkpointLocation", checkpointPath)
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>

        val cachedBatch = batchDF.persist(StorageLevel.MEMORY_AND_DISK)

        try {
          val report = cachedBatch
            .withColumn("window_start", col("window.start"))
            .withColumn("window_end", col("window.end"))
            .drop("window")

          println(s"\n========== MICRO-BATCH $batchId ==========")
          println("Account transaction metrics:")

          report
            .orderBy(col("window_start"), col("account_id"))
            .show(100, truncate = false)

          // CSV path is a Spark output directory containing part-files.
          report.write
            .mode("append")
            .option("header", "true")
            .csv(metricsPath)

          val alerts = report.filter(col("suspicious_burst") === true)

          println("Suspicious burst alerts:")
          alerts.show(100, truncate = false)

          if (alerts.take(1).nonEmpty) {
            alerts.write
              .mode("append")
              .option("header", "true")
              .csv(alertsPath)
          }

        } finally {
          cachedBatch.unpersist()
        }
      }
      .start()

    println(s"Input directory: $inputDir")
    println("Banking stream started. Press Ctrl+C to stop.")
    query.awaitTermination()
  }
}
