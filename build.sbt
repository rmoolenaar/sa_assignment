name := "SchipholAirport"

version := "0.1"

scalaVersion := "2.12.14"

idePackagePrefix := Some("nl.javadb")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.1.2",
  "org.apache.spark" %% "spark-sql" % "3.1.2",
  "org.scalatest" %% "scalatest" % "3.2.9" % Test
)