import java.util.Properties
import scala.io.Source

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

object HealthcareProducer {

  def main(args: Array[String]): Unit = {

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)
    props.put("acks", "all")
    props.put("enable.idempotence", "true")

    val producer = new KafkaProducer[String, String](props)
    val mapper = new ObjectMapper()

    try {
      val source = Source.fromFile("data/patient_vitals.json")

      try {
        for (line <- source.getLines().filter(_.trim.nonEmpty)) {
          val node = mapper.readTree(line)
          val patientId = node.get("patient_id").asText()

          val record = new ProducerRecord[String, String](
            "healthcare-vitals",
            patientId,
            line
          )

          producer.send(record).get()
          println(s"Sent vital event for patient $patientId")
        }
      } finally {
        source.close()
      }

      println("All patient vital events sent successfully.")
    } finally {
      producer.close()
    }
  }
}
