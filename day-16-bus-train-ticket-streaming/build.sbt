ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "day-16-bus-train-ticket-streaming",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.5.6",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.6",
      "org.apache.spark" %% "spark-hive" % "3.5.6"
    )
  )
