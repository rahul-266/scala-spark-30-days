package com.booking

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object BookingApp {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("RealTimeBookingSystem")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val bookingsPath = args.headOption.getOrElse("data/bookings.csv")
    val availabilityPath =
      args.lift(1).getOrElse("data/availability.csv")

    // 1. Read booking data
    val bookings = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(bookingsPath)

    // 2. Basic data cleaning
    val cleanBookings = bookings
      .filter(
        col("booking_id").isNotNull &&
        col("customer_id").isNotNull &&
        col("amount").isNotNull &&
        col("amount") > 0
      )
      .withColumn("event_time", to_timestamp(col("event_time")))

    // 3. Remove duplicate booking IDs
    val uniqueBookings = cleanBookings
      .dropDuplicates("booking_id")

    // 4. Separate confirmed and cancelled bookings
    val confirmed = uniqueBookings
      .filter(upper(col("status")) === "CONFIRMED")

    val cancelled = uniqueBookings
      .filter(upper(col("status")) === "CANCELLED")

    println("\\n========== ALL BOOKINGS ==========")
    uniqueBookings.show(false)

    println("\\n========== CONFIRMED BOOKINGS ==========")
    confirmed.show(false)

    println("\\n========== CANCELLED BOOKINGS ==========")
    cancelled.show(false)

    // 5. Revenue and booking count by type
    val bookingSummary = uniqueBookings
      .groupBy(upper(col("booking_type")).as("booking_type"))
      .agg(
        count("*").as("total_bookings"),
        sum(when(
          upper(col("status")) === "CONFIRMED", col("amount")
        ).otherwise(0)).as("confirmed_amount"),
        sum(when(
          upper(col("status")) === "CANCELLED", col("amount")
        ).otherwise(0)).as("cancelled_amount")
      )

    println("\\n========== BOOKING SUMMARY ==========")
    bookingSummary.show(false)

    // 6. Customer-wise confirmed spending
    val customerSummary = confirmed
      .groupBy("customer_id")
      .agg(
        count("*").as("confirmed_bookings"),
        sum("amount").as("total_spending")
      )
      .orderBy(desc("total_spending"))

    println("\\n========== CUSTOMER SUMMARY ==========")
    customerSummary.show(false)

    // 7. Broadcast the small availability reference table
    val availability = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(availabilityPath)

    val destinationSummary = confirmed
  .withColumn("booking_type", upper(col("booking_type")))
  .groupBy("destination", "booking_type")
  .agg(count("*").as("confirmed_bookings"))

    val capacityReport = destinationSummary
      .join(
        broadcast(availability),
        Seq("destination", "booking_type"),
        "left"
      )
      .withColumn(
        "remaining_capacity",
        col("total_capacity") - col("confirmed_bookings")
      )

    println("\\n========== CAPACITY REPORT ==========")
    capacityReport.show(false)

    // 8. Register a temporary view and run Spark SQL
    uniqueBookings.createOrReplaceTempView("bookings")

    val sqlReport = spark.sql("""
      SELECT
        UPPER(booking_type) AS booking_type,
        COUNT(*) AS total_bookings,
        SUM(CASE WHEN UPPER(status) = 'CONFIRMED'
                 THEN amount ELSE 0 END) AS confirmed_amount
      FROM bookings
      GROUP BY UPPER(booking_type)
      ORDER BY total_bookings DESC
    """)

    println("\\n========== SPARK SQL REPORT ==========")
    sqlReport.show(false)

    // 9. Save reports
    bookingSummary.write.mode("overwrite")
      .option("header", "true")
      .csv("output/booking_summary")

    customerSummary.write.mode("overwrite")
      .option("header", "true")
      .csv("output/customer_summary")

    capacityReport.write.mode("overwrite")
      .option("header", "true")
      .csv("output/capacity_report")

    sqlReport.write.mode("overwrite")
      .option("header", "true")
      .csv("output/sql_report")

    println("\\nReports saved under output/")
    spark.stop()
  }
}
