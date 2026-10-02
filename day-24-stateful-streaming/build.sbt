ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "day-24-stateful-streaming",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-streaming" % "3.5.6"
    )
  )
