ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "day-26-real-time-healthcare",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-streaming" % "3.5.6",
      "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.5.6"
    )
  )
