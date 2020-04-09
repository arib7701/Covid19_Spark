name := "Covid19_Spark"

version := "0.1"

scalaVersion := "2.11.4"

scalacOptions ++= List("-feature","-deprecation", "-unchecked", "-Xlint")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.1",
  "org.apache.spark" %% "spark-sql" % "2.3.1"
)