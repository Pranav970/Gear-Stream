name := "fraud-detection-pipeline"
version := "1.0.0"
scalaVersion := "2.12.18"

val sparkVersion = "3.4.1"
val circeVersion = "0.14.3"

libraryDependencies ++= Seq(
  // Spark Core & Streaming
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,

  // Snowflake Connector
  "net.snowflake" %% "spark-snowflake" % "2.12.0-spark_3.4",
  "net.snowflake" % "snowflake-jdbc" % "3.13.33",

  // Kafka Client (Producer)
  "org.apache.kafka" % "kafka-clients" % "3.4.0",

  // JSON Serialization (Circe)
  "io.circe" %% "circe-core" % circeVersion,
  "io.circe" %% "circe-generic" % circeVersion,
  "io.circe" %% "circe-parser" % circeVersion
)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}
