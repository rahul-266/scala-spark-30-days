ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "day-12-cache-persist",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.6"
    )
  )
