import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

object AccountStatefulStreaming {

  def createContext(checkpointDir: String): StreamingContext = {

    val conf = new SparkConf()
      .setAppName("Day24-Stateful-Streaming")
      .setMaster("local[2]")

    val ssc = new StreamingContext(conf, Seconds(10))

    // Required for updateStateByKey
    ssc.checkpoint(checkpointDir)

    // Read account transactions from socket
    val lines = ssc.socketTextStream("localhost", 9999)

    // Parse transaction records
    val parsedTransactions = lines.flatMap { line =>
      val parts = line.trim.split(",", -1)

      if (
        parts.length == 3 &&
        parts(0).trim.nonEmpty &&
        parts(1).trim.nonEmpty
      ) {
        try {
          Some((
            parts(0).trim,
            parts(1).trim,
            parts(2).trim.toDouble
          ))
        } catch {
          case _: NumberFormatException => None
        }
      } else {
        None
      }
    }

    // Stateless transformations
    val validTransactions = parsedTransactions
      .filter { case (_, _, amount) => amount > 0 }

    // Count transactions in the current batch only
    val batchCounts = validTransactions
      .map { case (accountId, _, _) => (accountId, 1) }
      .reduceByKey(_ + _)

    // Stateful transformation: maintain running counts
    val runningCounts = batchCounts.updateStateByKey[Int](
      (newCounts: Seq[Int], previousCount: Option[Int]) => {
        Some(previousCount.getOrElse(0) + newCounts.sum)
      }
    )

    // Compare current-batch counts and accumulated counts
    val comparison = batchCounts.fullOuterJoin(runningCounts)

    comparison.foreachRDD {
      (rdd: RDD[(String, (Option[Int], Option[Int]))], time) =>

        println(s"\n=== Batch comparison at $time ===")
        println("Account ID | Current Batch | Running Total")
        println("--------------------------------------------")

        if (rdd.isEmpty()) {
          println("No transactions received yet.")
        } else {
          rdd.collect().sortBy(_._1).foreach {
            case (accountId, (batchCount, runningCount)) =>
              println(
                s"$accountId | ${batchCount.getOrElse(0)} | " +
                s"${runningCount.getOrElse(0)}"
              )
          }
        }
    }

    ssc
  }

  def main(args: Array[String]): Unit = {

    val checkpointDir =
      new java.io.File("checkpoint").getAbsolutePath

    val ssc = StreamingContext.getOrCreate(
      checkpointDir,
      () => createContext(checkpointDir)
    )

    ssc.sparkContext.setLogLevel("ERROR")

    println("Starting Day 24 Stateful Streaming...")
    println("Socket: localhost:9999")
    println("Batch interval: 10 seconds")

    ssc.start()
    ssc.awaitTermination()
  }
}
