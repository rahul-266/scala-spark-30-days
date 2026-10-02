import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

object WindowOperations {

  def main(args: Array[String]): Unit = {

    // 1. Configure Spark Streaming
    val conf = new SparkConf()
      .setAppName("Day25-Window-Operations")
      .setMaster("local[2]")

    val batchInterval = Seconds(10)
    val windowDuration = Seconds(600) // 10 minutes
    val slideDuration = Seconds(30)   // 30 seconds

    val ssc = new StreamingContext(conf, batchInterval)
ssc.sparkContext.setLogLevel("ERROR")

// Required for window operations
ssc.checkpoint("/home/donesh/spark-checkpoints/day25-window-operations")

    // 2. Read transactions from a socket
    // Input format: route_id,amount
    val lines = ssc.socketTextStream("localhost", 9999)

    // 3. Parse and validate the incoming records
    val transactions = lines.flatMap { line =>
      val parts = line.trim.split(",", -1)

      if (parts.length == 2 && parts(0).trim.nonEmpty) {
        try {
          val amount = parts(1).trim.toDouble

          if (amount > 0) {
            Some((parts(0).trim, amount))
          } else {
            None
          }
        } catch {
          case _: NumberFormatException => None
        }
      } else {
        None
      }
    }

    // 4. Overall transaction count in the last 10 minutes
    val rollingCount = transactions
      .countByWindow(windowDuration, slideDuration)

    rollingCount.foreachRDD {
      (rdd: RDD[Long], time: Time) =>
        val count = rdd.take(1).headOption.getOrElse(0L)

        println(
          s"\n[$time] Transactions in last 10 minutes: $count"
        )
    }

    // 5. Route-wise rolling sales totals
    val rollingSales = transactions
      .reduceByKeyAndWindow(
        (a: Double, b: Double) => a + b,
        (a: Double, b: Double) => a - b,
        windowDuration,
        slideDuration,
        2
      )

    rollingSales.foreachRDD {
      (rdd: RDD[(String, Double)], time: Time) =>
        println(s"\n=== Rolling Sales at $time ===")

        if (rdd.isEmpty()) {
          println("No sales in the current window.")
        } else {
          rdd.collect().sortBy(_._1).foreach {
            case (route, total) =>
              println(f"$route%s -> ₹$total%.2f")
          }
        }
    }

    // 6. Route-wise transaction counts in the rolling window
    val routeCounts = transactions
      .map { case (route, _) => (route, 1L) }
      .reduceByKeyAndWindow(
        (a: Long, b: Long) => a + b,
        (a: Long, b: Long) => a - b,
        windowDuration,
        slideDuration,
        2
      )

    // Alert if a route has 5 or more transactions
    // during the last 10 minutes.
    val surgeThreshold = 5L

    val surgeAlerts = routeCounts.filter {
      case (_, count) => count >= surgeThreshold
    }

    surgeAlerts.foreachRDD {
      (rdd: RDD[(String, Long)], time: Time) =>
        println(s"\n=== Transaction Surge Alerts at $time ===")

        if (rdd.isEmpty()) {
          println("No transaction surge detected.")
        } else {
          rdd.collect().sortBy(_._1).foreach {
            case (route, count) =>
              println(
                s"ALERT: Route $route has $count transactions " +
                "in the last 10 minutes."
              )
          }
        }
    }

    println("Starting Day 25 Window Operations...")
    println("Socket: localhost:9999")
    println("Batch interval: 10 seconds")
    println("Window size: 10 minutes")
    println("Sliding interval: 30 seconds")
    println(s"Surge threshold: $surgeThreshold transactions")

    ssc.start()
    ssc.awaitTermination()
  }
}
