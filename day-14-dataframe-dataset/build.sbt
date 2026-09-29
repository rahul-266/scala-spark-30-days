ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "day-14-dataframe-dataset",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.5.6"
    )
  )
