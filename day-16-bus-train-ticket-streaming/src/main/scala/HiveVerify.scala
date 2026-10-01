import org.apache.spark.sql.SparkSession

object HiveVerify {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Day16-Hive-Verify")
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    println("=== Hive Tables ===")
    spark.sql("SHOW TABLES IN day16").show(false)

    println("=== Booking History ===")
    spark.sql(
      "SELECT event_type, COUNT(*) AS total " +
      "FROM day16.ticket_booking_history " +
      "GROUP BY event_type"
    ).show(false)

    spark.stop()
  }
}
