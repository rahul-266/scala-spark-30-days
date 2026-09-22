ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "day-11-broadcast-accumulators",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.6"
    )
  )
