import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object TicketStreaming {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day16-Ticket-Streaming")
      .master("local[2]")
       .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._
	spark.sql("CREATE DATABASE IF NOT EXISTS day16")

    // Read route capacities
    val capacities = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/route_capacity.csv")
      .withColumn("total_seats", col("total_seats").cast("long"))

    // Define the ticket event schema
    val schema = new org.apache.spark.sql.types.StructType()
      .add("event_id", "string")
      .add("booking_id", "string")
      .add("route_id", "string")
      .add("passenger_id", "string")
      .add("event_type", "string")
      .add("fare", "double")
      .add("seats", "integer")
      .add("event_time", "string")

    // Read events from Kafka
    val events = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "ticket-bookings")
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING) AS json")
      .select(from_json(col("json"), schema).as("data"))
      .select("data.*")
      .withColumn(
        "event_ts",
        to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ss")
      )      .filter(
        col("route_id").isNotNull &&
        col("event_ts").isNotNull &&
        col("seats").isNotNull &&
        col("fare").isNotNull &&
        col("event_type").isin("BOOKING", "CANCELLATION")
      )

    // Process each micro-batch
    val query = events.writeStream
      .outputMode("append")
      .option(
        "checkpointLocation",
        "/tmp/day16-ticket-hive-checkpoint"
      )
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>

        println(s"\n=== Micro-batch $batchId ===")

        if (!batchDF.isEmpty) {
	
	val historyTable = "day16.ticket_booking_history"

val uniqueBatch = batchDF.dropDuplicates("event_id")

val newEvents =
  if (spark.catalog.tableExists(historyTable)) {
    uniqueBatch.join(
      spark.table(historyTable)
        .select("event_id")
        .distinct(),
      Seq("event_id"),
      "left_anti"
    )
  } else {
    uniqueBatch
  }

if (!newEvents.isEmpty) {
  newEvents
    .withColumn("processed_batch_id", lit(batchId))
    .write
    .mode("append")
    .format("parquet")
    .saveAsTable(historyTable)

  println("New booking events saved to Hive.")
}

          // Window-based route metrics
          val windowMetrics = batchDF
            .groupBy(
              window(col("event_ts"), "10 minutes"),
              col("route_id")
            )
            .agg(
              sum(
                when(col("event_type") === "BOOKING", 1)
                  .otherwise(0)
              ).as("booking_count"),
              sum(
                when(col("event_type") === "CANCELLATION", 1)
                  .otherwise(0)
              ).as("cancellation_count"),
              sum(
                when(
                  col("event_type") === "BOOKING",
                  col("fare") * col("seats")
                ).when(
                  col("event_type") === "CANCELLATION",
                  -col("fare") * col("seats")
                ).otherwise(0.0)
              ).as("revenue")
            )
            .select(
              col("route_id"),
              col("window.start").as("window_start"),
              col("window.end").as("window_end"),
              col("booking_count"),
              col("cancellation_count"),
              round(col("revenue"), 2).as("revenue")
            )

          println("\n=== Bookings and Revenue by Route and Window ===")
          windowMetrics.orderBy("window_start", "route_id").show(false)

          // Peak booking period per route
          val rankWindow = Window
            .partitionBy("route_id")
            .orderBy(
              col("booking_count").desc,
              col("window_start").asc
            )

          val peakPeriods = windowMetrics
            .withColumn("rank", row_number().over(rankWindow))
            .filter(col("rank") === 1)
            .drop("rank")

          println("\n=== Peak Booking Period ===")
          peakPeriods.show(false)

          // Route-wise seat availability and cancellation rate
          val routeSummary = batchDF
            .groupBy("route_id")
            .agg(
              sum(
                when(col("event_type") === "BOOKING", 1)
                  .otherwise(0)
              ).as("bookings"),
              sum(
                when(col("event_type") === "CANCELLATION", 1)
                  .otherwise(0)
              ).as("cancellations"),
              sum(
                when(
                  col("event_type") === "BOOKING",
                  col("seats")
                ).when(
                  col("event_type") === "CANCELLATION",
                  -col("seats")
                ).otherwise(0)
              ).as("net_seats"),
              sum(
                when(
                  col("event_type") === "BOOKING",
                  col("fare") * col("seats")
                ).when(
                  col("event_type") === "CANCELLATION",
                  -col("fare") * col("seats")
                ).otherwise(0.0)
              ).as("total_revenue")
            )
            .join(capacities, Seq("route_id"), "left")
            .withColumn(
              "available_seats",
              greatest(
                lit(0L),
                col("total_seats") - col("net_seats")
              )
            )
            .withColumn(
              "cancellation_rate",
              round(
                when(
                  col("bookings") + col("cancellations") > 0,
                  col("cancellations") * 100.0 /
                    (col("bookings") + col("cancellations"))
                ).otherwise(0.0),
                2
              )
            )
            .select(
              col("route_id"),
              col("bookings"),
              col("cancellations"),
              round(col("total_revenue"), 2).as("total_revenue"),
              col("available_seats"),
              col("cancellation_rate")
            )

          println("\n=== Route Summary ===")
          routeSummary.orderBy("route_id").show(false)
        }
      }
      .start()

    query.awaitTermination()
  }
}
