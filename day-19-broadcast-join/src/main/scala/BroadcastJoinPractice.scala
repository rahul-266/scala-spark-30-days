import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object BroadcastJoinPractice {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day19-Broadcast-Join")
      .master("local[1]")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.sql.join.preferSortMergeJoin", "true")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // 1. Create a large fact DataFrame (1 million rows)
    val transactions = spark.range(1L, 100001L)
      .withColumnRenamed("id", "transaction_id")
      .withColumn(
        "branch_id",
        format_string(
          "B%03d",
          (
            pmod(
              col("transaction_id") - lit(1L),
              lit(10L)
            ) + lit(1L)
          ).cast("int")
        )
      )
      .withColumn(
        "amount",
        (
          pmod(col("transaction_id"), lit(1000L)) + lit(100L)
        ).cast("double")
      )

    // 2. Read the small branch master
    val branches = spark.read
      .option("header", "true")
      .csv("data/branches.csv")

    println("\n=== DATASET SIZES ===")
    println(s"Total transactions: ${transactions.count()}")
    println(s"Total branches: ${branches.count()}")

    // 3. Explicit broadcast join
    val broadcastJoined = transactions.alias("t")
      .join(
        broadcast(branches).alias("b"),
        col("t.branch_id") === col("b.branch_id"),
        "inner"
      )
      .select(
        col("t.transaction_id"),
        col("t.branch_id"),
        col("b.branch_name"),
        col("b.city"),
        col("t.amount")
      )

    println("\n=== BROADCAST JOIN PHYSICAL PLAN ===")
    broadcastJoined.explain("formatted")

    // 4. Calculate branch-wise metrics
    val broadcastMetrics = broadcastJoined
      .groupBy("branch_id", "branch_name", "city")
      .agg(
        count(lit(1)).as("transaction_count"),
        round(sum(col("amount")), 2).as("total_amount"),
        round(avg(col("amount")), 2).as("average_amount"),
        min(col("amount")).as("minimum_amount"),
        max(col("amount")).as("maximum_amount")
      )
      .orderBy(col("branch_id"))

    println("\n=== BROADCAST JOIN REPORT ===")
    broadcastMetrics.show(20, false)

    // 5. Disable automatic broadcasting for comparison
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    // 6. Regular join for Shuffle Sort Merge comparison
    val sortMergeJoined = transactions.alias("t")
      .join(
        branches.alias("b"),
        col("t.branch_id") === col("b.branch_id"),
        "inner"
      )
      .select(
        col("t.transaction_id"),
        col("t.branch_id"),
        col("b.branch_name"),
        col("b.city"),
        col("t.amount")
      )

    println("\n=== SHUFFLE SORT MERGE JOIN PHYSICAL PLAN ===")
    sortMergeJoined.explain("formatted")

    val sortMergeMetrics = sortMergeJoined
      .groupBy("branch_id", "branch_name", "city")
      .agg(
        count(lit(1)).as("transaction_count"),
        round(sum(col("amount")), 2).as("total_amount"),
        round(avg(col("amount")), 2).as("average_amount"),
        min(col("amount")).as("minimum_amount"),
        max(col("amount")).as("maximum_amount")
      )
      .orderBy(col("branch_id"))

    println("\n=== SHUFFLE SORT MERGE JOIN REPORT ===")
    sortMergeMetrics.show(20, false)

    println("\n=== DAY 19 BROADCAST JOIN COMPLETED ===")

    spark.stop()
  }
}
