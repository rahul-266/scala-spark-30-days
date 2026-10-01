import java.util.Properties
import scala.io.Source

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

object TicketProducer {

  def main(args: Array[String]): Unit = {

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)
    props.put("acks", "all")
    props.put("enable.idempotence", "true")

    val producer = new KafkaProducer[String, String](props)
    val filePath = "data/ticket_events.json"

    val routePattern = """"route_id"\s*:\s*"([^"]+)"""".r

    try {
      val source = Source.fromFile(filePath)

      try {
        for (line <- source.getLines().filter(_.trim.nonEmpty)) {
          val routeId = routePattern
            .findFirstMatchIn(line)
            .map(_.group(1))
            .getOrElse("UNKNOWN")

          val record = new ProducerRecord[String, String](
            "ticket-bookings",
            routeId,
            line
          )

          producer.send(record).get()
          println(s"Sent event for route $routeId: $line")
        }
      } finally {
        source.close()
      }

      println("All ticket events sent successfully.")
    } finally {
      producer.close()
    }
  }
}
