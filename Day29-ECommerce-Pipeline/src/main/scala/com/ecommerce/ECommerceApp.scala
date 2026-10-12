package com.ecommerce

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object ECommerceApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("ECommerceEndToEndPipeline")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val customersPath = args.lift(0).getOrElse("data/customers.csv")
    val productsPath = args.lift(1).getOrElse("data/products.csv")
    val ordersPath = args.lift(2).getOrElse("data/orders.csv")

    // 1. Read raw data
    val customersRaw = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(customersPath)

    val productsRaw = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(productsPath)

    val ordersRaw = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(ordersPath)

    println("\n========== RAW DATA COUNTS ==========")
    println(s"Customers: ${customersRaw.count()}")
    println(s"Products: ${productsRaw.count()}")
    println(s"Orders: ${ordersRaw.count()}")

    // 2. Clean customers and products
    val customers = customersRaw
      .filter(
        col("customer_id").isNotNull &&
        col("customer_name").isNotNull
      )
      .dropDuplicates("customer_id")

    val products = productsRaw
      .filter(
        col("product_id").isNotNull &&
        col("price").isNotNull &&
        col("price") > 0
      )
      .dropDuplicates("product_id")

    // 3. Clean orders
    val validOrders = ordersRaw
      .filter(
        col("order_id").isNotNull &&
        col("customer_id").isNotNull &&
        col("product_id").isNotNull &&
        col("quantity").isNotNull &&
        col("quantity") > 0 &&
        col("order_amount").isNotNull &&
        col("order_amount") > 0
      )
      .dropDuplicates("order_id")
      .withColumn("order_date", to_date(col("order_date")))

    println("\n========== CLEANING SUMMARY ==========")
    println(s"Raw orders: ${ordersRaw.count()}")
    println(s"Valid unique orders: ${validOrders.count()}")
    println(s"Rejected orders: ${ordersRaw.count() - validOrders.count()}")

    // 4. RDD practice: count valid orders by customer
    val customerOrderCounts = validOrders.rdd
      .map(row => (row.getAs[String]("customer_id"), 1))
      .reduceByKey(_ + _)

    println("\n========== RDD CUSTOMER ORDER COUNTS ==========")
    customerOrderCounts.collect().foreach {
      case (customerId, count) =>
        println(s"$customerId -> $count orders")
    }

    // 5. Enrich orders with product and customer details
    val enrichedOrders = validOrders
      .join(broadcast(products), Seq("product_id"), "inner")
      .join(customers, Seq("customer_id"), "left")
      .withColumn(
        "calculated_amount",
        col("quantity") * col("price")
      )
      .withColumn(
        "revenue_difference",
        col("order_amount") - col("calculated_amount")
      )
      .persist(StorageLevel.MEMORY_AND_DISK)

    // Materialize the persisted DataFrame
    val enrichedCount = enrichedOrders.count()

    println("\n========== ENRICHED ORDERS ==========")
    enrichedOrders.show(false)
    println(s"Enriched order count: $enrichedCount")

    // 6. Revenue by product category
    val categorySummary = enrichedOrders
      .groupBy("category")
      .agg(
        count("*").as("order_count"),
        sum("quantity").as("units_sold"),
        sum("order_amount").as("total_revenue")
      )
      .orderBy(desc("total_revenue"))

    println("\n========== CATEGORY REVENUE ==========")
    categorySummary.show(false)

    // 7. Customer analytics
    val customerSummary = enrichedOrders
      .groupBy("customer_id", "customer_name", "city")
      .agg(
        count("*").as("total_orders"),
        sum("order_amount").as("total_spending")
      )
      .orderBy(desc("total_spending"))

    println("\n========== CUSTOMER ANALYTICS ==========")
    customerSummary.show(false)

    // 8. Spark SQL analytics
    enrichedOrders.createOrReplaceTempView("enriched_orders")

    val sqlReport = spark.sql("""
      SELECT
        category,
        COUNT(*) AS order_count,
        SUM(quantity) AS units_sold,
        ROUND(SUM(order_amount), 2) AS total_revenue,
        ROUND(AVG(order_amount), 2) AS average_order_amount
      FROM enriched_orders
      GROUP BY category
      ORDER BY total_revenue DESC
    """)

    println("\n========== SPARK SQL REPORT ==========")
    sqlReport.show(false)

    // 9. Check order amount against product price * quantity
    val amountCheck = enrichedOrders
      .filter(abs(col("revenue_difference")) > 0.01)
      .select(
        "order_id",
        "order_amount",
        "calculated_amount",
        "revenue_difference"
      )

    println("\n========== AMOUNT VALIDATION ==========")
    amountCheck.show(false)

    // 10. Write reports
    categorySummary.write.mode("overwrite")
      .option("header", "true")
      .csv("output/category_summary")

    customerSummary.write.mode("overwrite")
      .option("header", "true")
      .csv("output/customer_summary")

    enrichedOrders.write.mode("overwrite")
      .option("header", "true")
      .csv("output/enriched_orders")

    sqlReport.write.mode("overwrite")
      .option("header", "true")
      .csv("output/sql_report")

    amountCheck.write.mode("overwrite")
      .option("header", "true")
      .csv("output/amount_validation")

    enrichedOrders.unpersist()

    println("\nPipeline completed. Reports are in output/")
    spark.stop()
  }
}
