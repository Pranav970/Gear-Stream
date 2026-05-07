package com.fraudpipeline.streaming

import com.fraudpipeline.sink.SnowflakeSinkProvider
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object FraudDetectionEngine {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("RealTimeFraudAnomalyDetection")
      // .master("local[*]") // Uncomment for local run without Databricks/cluster
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val kafkaBrokers = sys.env.getOrElse("KAFKA_BROKERS", "localhost:9092")
    val kafkaTopic = sys.env.getOrElse("KAFKA_TOPIC", "transactions")

    // 1. Define strict schema to handle corrupted/null JSON safely
    val transactionSchema = StructType(Seq(
      StructField("transactionId", StringType, nullable = false),
      StructField("accountId", StringType, nullable = true),
      StructField("amount", DoubleType, nullable = true),
      StructField("timestamp", TimestampType, nullable = true),
      StructField("merchantId", StringType, nullable = true),
      StructField("location", StringType, nullable = true)
    ))

    // 2. Ingest stream from Kafka
    val rawStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("subscribe", kafkaTopic)
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .load()

    // 3. Parse JSON, handle Nulls appropriately
    val parsedStream = rawStream
      .selectExpr("CAST(value AS STRING) as json_payload")
      .filter(col("json_payload").isNotNull)
      .select(from_json(col("json_payload"), transactionSchema).alias("data"))
      .select("data.*")
      .na.fill(0.0, Seq("amount")) // Prevent NPE on math operations
      .na.fill("UNKNOWN", Seq("accountId", "merchantId", "location"))

    // 4. Deduplication & Watermarking for late-arriving events
    val cleanStream = parsedStream
      .withWatermark("timestamp", "10 minutes") // Tolerate 10-min lag
      .dropDuplicates("transactionId", "timestamp") // Idempotency guarantee

    // 5. Logic A: Single-Event High Amount Threshold (>$10,000)
    val highAmountAlerts = cleanStream
      .withColumn("is_fraud", when(col("amount") > 10000, true).otherwise(false))
      .filter(col("is_fraud") === true)

    // 6. Logic B: High-Frequency Anomalies via Sliding Window (e.g., >3 txns in 1 minute)
    val frequencyAlerts = cleanStream
      .groupBy(
        window(col("timestamp"), "1 minute", "30 seconds"), // Sliding window
        col("accountId")
      )
      .agg(
        count("transactionId").alias("tx_count"),
        sum("amount").alias("total_amount_in_window")
      )
      .filter(col("tx_count") > 3)
      .withColumn("alert_reason", lit("HIGH_FREQUENCY_BURST"))

    // 7. Write Streams to Snowflake via ForeachBatch
    val checkpointBase = sys.env.getOrElse("CHECKPOINT_DIR", "/tmp/checkpoints")

    // Sink Query 1: Amount Alerts
    val amountQuery = highAmountAlerts.writeStream
      .foreachBatch { (batchDF, batchId) =>
        SnowflakeSinkProvider.writeToSnowflake(batchDF, batchId, "FRAUD_ALERTS_AMOUNT")
      }
      .option("checkpointLocation", s"$checkpointBase/amount_alerts")
      .outputMode("append")
      .start()

    // Sink Query 2: Frequency Alerts
    val frequencyQuery = frequencyAlerts.writeStream
      .foreachBatch { (batchDF, batchId) =>
        SnowflakeSinkProvider.writeToSnowflake(batchDF, batchId, "FRAUD_ALERTS_FREQUENCY")
      }
      .option("checkpointLocation", s"$checkpointBase/frequency_alerts")
      .outputMode("append") // Append is required when streaming windowed aggregations with watermarks
      .start()

    spark.streams.awaitAnyTermination()
  }
}
