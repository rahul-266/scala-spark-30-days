ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "day-18-joins",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.5.6"
    )
  )
