ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "day-25-window-operations",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-streaming" % "3.5.6"
    )
  )
