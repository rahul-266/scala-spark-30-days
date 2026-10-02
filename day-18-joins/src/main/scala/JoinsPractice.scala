import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object JoinsPractice {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day18-Joins-Practice")
      .master("local[2]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // Read CSV files
    val customers = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/customers.csv")

    val orders = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/orders.csv")

    val payments = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/payments.csv")

    println("\n=== CUSTOMERS ===")
    customers.show(false)

    println("\n=== ORDERS ===")
    orders.show(false)

    println("\n=== PAYMENTS ===")
    payments.show(false)

    // 1. INNER JOIN
    val innerJoin = orders.alias("o")
      .join(
        customers.alias("c"),
        col("o.customer_id") === col("c.customer_id"),
        "inner"
      )
      .select(
        col("o.order_id").as("order_id"),
        col("o.customer_id").as("customer_id"),
        col("c.customer_name").as("customer_name"),
        col("c.city").as("city"),
        col("o.amount").as("order_amount")
      )

    println("\n=== INNER JOIN: Orders and Customers ===")
    innerJoin.orderBy(col("order_id")).show(false)

    // 2. LEFT JOIN
    val leftJoin = orders.alias("o")
      .join(
        customers.alias("c"),
        col("o.customer_id") === col("c.customer_id"),
        "left"
      )
      .select(
        col("o.order_id").as("order_id"),
        col("o.customer_id").as("customer_id"),
        col("c.customer_name").as("customer_name"),
        col("c.city").as("city"),
        col("o.amount").as("order_amount")
      )
      .withColumn(
        "customer_name",
        coalesce(col("customer_name"), lit("Unknown Customer"))
      )
      .withColumn(
        "city",
        coalesce(col("city"), lit("Unknown City"))
      )

    println("\n=== LEFT JOIN WITH NULL HANDLING ===")
    leftJoin.orderBy(col("order_id")).show(false)

    // 3. RIGHT JOIN
    val rightJoin = orders.alias("o")
      .join(
        customers.alias("c"),
        col("o.customer_id") === col("c.customer_id"),
        "right"
      )
      .select(
        col("c.customer_id").as("customer_id"),
        col("c.customer_name").as("customer_name"),
        col("o.order_id").as("order_id"),
        col("o.amount").as("order_amount")
      )

    println("\n=== RIGHT JOIN: All Customers ===")
    rightJoin
      .orderBy(col("customer_id"), col("order_id"))
      .show(false)

    // 4. FULL OUTER JOIN
    val fullJoin = orders.alias("o")
      .join(
        customers.alias("c"),
        col("o.customer_id") === col("c.customer_id"),
        "full_outer"
      )
      .select(
        coalesce(
          col("o.customer_id"),
          col("c.customer_id")
        ).as("customer_id"),
        col("o.order_id").as("order_id"),
        col("c.customer_name").as("customer_name"),
        col("c.city").as("city"),
        col("o.amount").as("order_amount")
      )

    println("\n=== FULL OUTER JOIN ===")
    fullJoin
      .orderBy(col("customer_id"), col("order_id"))
      .show(false)

    // 5. JOIN ORDERS, CUSTOMERS AND PAYMENTS
    val orderCustomers = orders.alias("o")
      .join(
        customers.alias("c"),
        col("o.customer_id") === col("c.customer_id"),
        "left"
      )
      .select(
        col("o.order_id").as("order_id"),
        col("o.customer_id").as("customer_id"),
        col("c.customer_name").as("customer_name"),
        col("c.city").as("city"),
        col("o.amount").as("order_amount")
      )

    val finalReport = orderCustomers.alias("oc")
      .join(
        payments.alias("p"),
        col("oc.order_id") === col("p.order_id"),
        "left"
      )
      .select(
        col("oc.order_id").as("order_id"),
        col("oc.customer_id").as("customer_id"),
        col("oc.customer_name").as("customer_name"),
        col("oc.city").as("city"),
        col("oc.order_amount").as("order_amount"),
        col("p.payment_id").as("payment_id"),
        coalesce(
          col("p.payment_amount"),
          lit(0)
        ).as("paid_amount"),
        coalesce(
          col("p.payment_status"),
          lit("NOT_PAID")
        ).as("payment_status")
      )
      .withColumn(
        "balance_due",
        greatest(
          col("order_amount") - col("paid_amount"),
          lit(0)
        )
      )

    println("\n=== FINAL ORDERS, CUSTOMERS AND PAYMENTS REPORT ===")
    finalReport.orderBy(col("order_id")).show(false)

    // 6. SHUFFLE SORT MERGE JOIN DEMONSTRATION
    // Disable broadcast so the equi-join can demonstrate
    // the SortMergeJoin physical operator.
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.join.preferSortMergeJoin", "true")

    val sortMergeDemo = orders.alias("o")
      .join(
        payments.alias("p"),
        col("o.order_id") === col("p.order_id"),
        "inner"
      )
      .select(
        col("o.order_id").as("order_id"),
        col("o.amount").as("order_amount"),
        col("p.payment_amount").as("payment_amount")
      )

    println("\n=== SHUFFLE SORT MERGE JOIN PHYSICAL PLAN ===")
    sortMergeDemo.explain("formatted")

    println("\n=== DAY 18 JOINS COMPLETED ===")

    spark.stop()
  }
}
