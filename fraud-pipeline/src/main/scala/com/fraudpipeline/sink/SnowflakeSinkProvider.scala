package com.fraudpipeline.sink

import org.apache.spark.sql.{DataFrame, SaveMode}

object SnowflakeSinkProvider {

  // Load from Env variables for plug-and-play security
  private val sfOptions = Map(
    "sfURL" -> sys.env.getOrElse("SF_URL", ""),
    "sfUser" -> sys.env.getOrElse("SF_USER", ""),
    "sfPassword" -> sys.env.getOrElse("SF_PASSWORD", ""), // Use KeyPair in Prod!
    "sfDatabase" -> sys.env.getOrElse("SF_DATABASE", "FRAUD_DB"),
    "sfSchema" -> sys.env.getOrElse("SF_SCHEMA", "PUBLIC"),
    "sfWarehouse" -> sys.env.getOrElse("SF_WAREHOUSE", "COMPUTE_WH"),
    "sfRole" -> sys.env.getOrElse("SF_ROLE", "SYSADMIN")
  )

  def writeToSnowflake(batchDF: DataFrame, batchId: Long, tableName: String): Unit = {
    if (!batchDF.isEmpty) {
      println(s"Writing Batch ID $batchId to Snowflake table $tableName...")
      
      batchDF.write
        .format("net.snowflake.spark.snowflake")
        .options(sfOptions)
        .option("dbtable", tableName)
        // Snowflake handles micro-batch idempotency nicely when using SaveMode.Append with proper staging
        .mode(SaveMode.Append) 
        .save()
    }
  }
}
