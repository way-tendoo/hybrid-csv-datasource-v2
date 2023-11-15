ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.10"

lazy val root = (project in file("."))
  .settings(
    name := "hybrid-csv-datasource-v2",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.3.2",
      "org.apache.spark" %% "spark-sql"  % "3.3.2" % "provided",
      "org.scalatest"    %% "scalatest"  % "3.2.17" % Test
    )
  )
