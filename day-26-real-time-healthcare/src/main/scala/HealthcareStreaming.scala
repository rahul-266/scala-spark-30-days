import com.fasterxml.jackson.databind.ObjectMapper

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext, Time}
import org.apache.spark.streaming.kafka010.{
  ConsumerStrategies,
  KafkaUtils,
  LocationStrategies
}

case class PatientVital(
  eventId: String,
  patientId: String,
  heartRate: Int,
  temperature: Double,
  oxygen: Double,
  timestamp: String
) extends Serializable

case class VitalThresholds(
  maxHeartRate: Int,
  maxTemperature: Double,
  minOxygen: Double
) extends Serializable

case class VitalAssessment(
  reading: PatientVital,
  reasons: Seq[String]
) extends Serializable

object HealthcareStreaming {

  private def parseVital(json: String): Option[PatientVital] = {
    try {
      val node = new ObjectMapper().readTree(json)

      val requiredFields = Seq(
        "event_id",
        "patient_id",
        "heart_rate",
        "temperature",
        "oxygen",
        "timestamp"
      )

      val validFields = requiredFields.forall(
        field => node.has(field) && !node.get(field).isNull
      )

      if (!validFields) {
        None
      } else {
        val reading = PatientVital(
          eventId = node.get("event_id").asText().trim,
          patientId = node.get("patient_id").asText().trim,
          heartRate = node.get("heart_rate").asInt(),
          temperature = node.get("temperature").asDouble(),
          oxygen = node.get("oxygen").asDouble(),
          timestamp = node.get("timestamp").asText().trim
        )

        val valid =
          reading.eventId.nonEmpty &&
          reading.patientId.nonEmpty &&
          reading.timestamp.nonEmpty &&
          reading.heartRate > 0 &&
          reading.temperature > 0.0 &&
          reading.oxygen > 0.0 &&
          reading.oxygen <= 100.0

        if (valid) Some(reading) else None
      }
    } catch {
      case _: Exception => None
    }
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day26-Real-Time-Healthcare")
      .setMaster("local[2]")

    val batchInterval = Seconds(10)
    val ssc = new StreamingContext(conf, batchInterval)

    ssc.sparkContext.setLogLevel("ERROR")

    // Checkpoint for window processing
    ssc.checkpoint(
      "/home/donesh/spark-checkpoints/day26-healthcare"
    )

    // Broadcast configurable demo thresholds
    val thresholdBroadcast =
      ssc.sparkContext.broadcast(
        VitalThresholds(
          maxHeartRate = 120,
          maxTemperature = 39.0,
          minOxygen = 90.0
        )
      )

    // Accumulators for processed reading counts
    val totalReadings =
      ssc.sparkContext.longAccumulator("total-vital-readings")

    val abnormalReadings =
      ssc.sparkContext.longAccumulator("abnormal-vital-readings")

    // Kafka configuration
    val kafkaParams: Map[String, Object] = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "day26-healthcare-group",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->
        classOf[StringDeserializer].getName,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->
        classOf[StringDeserializer].getName,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"
    )

    val topics = Array("healthcare-vitals")

    // Read patient events from Kafka
    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](
        topics,
        kafkaParams
      )
    )

    // Parse the incoming JSON records
    val readings = kafkaStream
      .flatMap(record => parseVital(record.value()))

    // Classify each reading using broadcast thresholds
    val assessments = readings.map { reading =>
      val thresholds = thresholdBroadcast.value

      val reasons = Seq(
        if (reading.heartRate > thresholds.maxHeartRate)
          Seq("HIGH_HEART_RATE")
        else Seq.empty[String],

        if (reading.temperature > thresholds.maxTemperature)
          Seq("HIGH_TEMPERATURE")
        else Seq.empty[String],

        if (reading.oxygen < thresholds.minOxygen)
          Seq("LOW_OXYGEN")
        else Seq.empty[String]
      ).flatten

      VitalAssessment(reading, reasons)
    }.persist(StorageLevel.MEMORY_ONLY)

    // Current batch alerts and accumulator reporting
    assessments.foreachRDD {
      (rdd: RDD[VitalAssessment], time: Time) =>

        // This small demo collects each batch for display.
        val records = rdd.collect()

        totalReadings.add(records.length.toLong)

        val abnormal = records.filter(_.reasons.nonEmpty)
        abnormalReadings.add(abnormal.length.toLong)

        println(s"\n=== Healthcare Batch at $time ===")
        println(s"Readings in batch: ${records.length}")

        if (abnormal.isEmpty) {
          println("No abnormal readings in this batch.")
        } else {
          println("=== ABNORMAL VITAL ALERTS ===")
          abnormal.foreach { assessment =>
            println(
              s"ALERT patient=${assessment.reading.patientId}, " +
              s"event=${assessment.reading.eventId}, " +
              s"heart_rate=${assessment.reading.heartRate}, " +
              s"temperature=${assessment.reading.temperature}, " +
              s"oxygen=${assessment.reading.oxygen}, " +
              s"reasons=${assessment.reasons.mkString(",")}"
            )
          }
        }

        println(
          s"Accumulator totals: readings=${totalReadings.value}, " +
          s"abnormal=${abnormalReadings.value}"
        )
    }

    // Count repeated abnormal readings by patient
    // within a five-minute rolling window.
    val repeatedAbnormal = assessments
      .filter(_.reasons.nonEmpty)
      .map(assessment => (assessment.reading.patientId, 1))
      .reduceByKeyAndWindow(
        (a: Int, b: Int) => a + b,
        Minutes(5),
        Seconds(30)
      )
      .filter { case (_, count) => count >= 3 }

    repeatedAbnormal.foreachRDD {
      (rdd: RDD[(String, Int)], time: Time) =>
        val alerts = rdd.collect().sortBy(_._1)

        println(s"\n=== Repeated Abnormal Readings at $time ===")

        if (alerts.isEmpty) {
          println("No repeated abnormal readings detected.")
        } else {
          alerts.foreach {
            case (patientId, count) =>
              println(
                s"REPEATED ALERT: Patient $patientId has " +
                s"$count abnormal readings in the last 5 minutes."
              )
          }
        }
    }

    println("Starting Healthcare Streaming...")
    println("Kafka topic: healthcare-vitals")
    println("Batch interval: 10 seconds")
    println("Repeated alert window: 5 minutes")
    println("Repeated alert threshold: 3 abnormal readings")

    ssc.start()
    ssc.awaitTermination()
  }
}
