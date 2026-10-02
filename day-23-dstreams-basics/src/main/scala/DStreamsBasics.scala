import java.util.Locale

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

object DStreamsBasics {

  def main(args: Array[String]): Unit = {

    // 1. Configure Spark
    val conf = new SparkConf()
      .setAppName("Day23-DStreams-Basics")
      .setMaster("local[2]")

    // 2. Create StreamingContext with 10-second batches
    val batchInterval = Seconds(10)
    val ssc = new StreamingContext(conf, batchInterval)

    ssc.sparkContext.setLogLevel("ERROR")

    // 3. Read the text stream from a socket
    val lines = ssc.socketTextStream("localhost", 9999)

    // 4. Use flatMap, filter and map
    // Each matching log line produces one ERROR record.
    val errorMessages = lines
      .flatMap { line =>
        if (line.toUpperCase(Locale.ROOT).contains("ERROR")) {
          Seq(line)
        } else {
          Seq.empty[String]
        }
      }
      .filter(_.trim.nonEmpty)
      .map(_ => 1)

    // 5. Count errors in each micro-batch
    errorMessages.foreachRDD {
      (rdd: RDD[Int], time: Time) =>
        val errorCount = rdd.count()

        println(
          s"[$time] ERROR messages in this 10-second batch: $errorCount"
        )
    }

    // 6. Start the streaming application
    println("Starting DStreams application...")
    println("Listening on localhost:9999")
    println("Batch interval: 10 seconds")

    ssc.start()
    ssc.awaitTermination()
  }
}
