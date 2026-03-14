package com.sparkrca.config

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

/**
 * Centralized Spark Configuration for the RCA Project.
 * Provides unified session management for YARN and local modes.
 */
object SparkConfig {

  // ============================================================================
  // HDFS Path Constants
  // ============================================================================
  object Paths {
    val HDFS_BASE = "hdfs://namenode:8020/project"
    
    // TPC-H Data Paths
    val TPCH_RAW = "/raw"
    val TPCH_PARQUET = s"$HDFS_BASE/parquet"
    
    // Spark Event Logs
    val EVENT_LOGS = s"$HDFS_BASE/spark-logs"
    
    // ML Model Paths
    val ML_MODELS = s"$HDFS_BASE/models"
    val ML_TRAINING_DATA = s"$HDFS_BASE/training"
    val ML_PREDICTIONS = s"$HDFS_BASE/predictions"
    
    // Intermediate Processing Paths
    val PARSED_LOGS = s"$HDFS_BASE/parsed-logs"
    val EXTRACTED_FEATURES = s"$HDFS_BASE/features"
    val PREPROCESS = s"$HDFS_BASE/preprocess"
  }

  // ============================================================================
  // Failure Labels (Ground Truth)
  // ============================================================================
  object Labels {
    val BASELINE = 0           // Normal execution
    val OUT_OF_MEMORY = 1      // OOM failure
    val DATA_SKEW = 2          // Straggler tasks due to skew
    val SERIALIZATION = 3      // Task not serializable
    val NETWORK_TIMEOUT = 4    // ExecutorLost due to missed heartbeats
    val DISK_SPACE = 5         // No space left on device
    val METADATA_FAILURE = 6   // FileNotFoundException
    
    val labelNames: Map[Int, String] = Map(
      BASELINE -> "BASELINE",
      OUT_OF_MEMORY -> "OUT_OF_MEMORY",
      DATA_SKEW -> "DATA_SKEW",
      SERIALIZATION -> "SERIALIZATION_FAILURE",
      NETWORK_TIMEOUT -> "NETWORK_TIMEOUT",
      DISK_SPACE -> "DISK_SPACE_FAILURE",
      METADATA_FAILURE -> "METADATA_FAILURE"
    )
    
    def labelName(label: Int): String = labelNames.getOrElse(label, s"UNKNOWN($label)")
  }


  // ============================================================================
  // Spark Session Builder
  // ============================================================================
  
  /**
   * Creates a SparkSession configured for the RCA project.
   * 
   * @param appName Application name for Spark UI
   * @param master Spark master URL ("yarn", "local[*]", etc.)
   * @param enableEventLog Whether to enable event logging for analysis
   * @param eventLogSubDir Optional subdirectory for event logs (e.g., "baseline", "oom")
   * @return Configured SparkSession
   */
  def createSparkSession(
    appName: String,
    master: String = "yarn",
    enableEventLog: Boolean = true,
    eventLogSubDir: Option[String] = None
  ): SparkSession = {
    
    val conf = new SparkConf()
      .setAppName(appName)
      .setIfMissing("spark.master", master)
      
      // Memory Configuration (optimized for cluster)
      .setIfMissing("spark.executor.memory", "3g")
      .setIfMissing("spark.driver.memory", "1g")
      .setIfMissing("spark.executor.memoryOverhead", "512m")
      .setIfMissing("spark.executor.cores", "2")
      
      // Shuffle Configuration
      .setIfMissing("spark.shuffle.file.buffer", "64k")
      .setIfMissing("spark.shuffle.spill.compress", "true")
      
      // Serialization
      .setIfMissing("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .setIfMissing("spark.kryoserializer.buffer.max", "256m")
      
      // SQL Configuration
      .setIfMissing("spark.sql.adaptive.enabled", "true")
      .setIfMissing("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .setIfMissing("spark.sql.parquet.compression.codec", "snappy")
    
    // Event Logging (Required for RCA analysis)
    if (enableEventLog) {
      val logDir = eventLogSubDir match {
        case Some(subDir) => s"${Paths.EVENT_LOGS}/$subDir"
        case None => Paths.EVENT_LOGS
      }
      conf
        .setIfMissing("spark.eventLog.enabled", "true")
        .setIfMissing("spark.eventLog.dir", logDir)
        .setIfMissing("spark.eventLog.compress", "true")
    }
    
    SparkSession.builder()
      .config(conf)
      .enableHiveSupport() // For TPC-H table queries
      .getOrCreate()
  }

  
  /**
   * Creates a local SparkSession for development/testing.
   */
  def createLocalSession(appName: String): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .master("local[*]")
      .config("spark.driver.memory", "4g")
      .config("spark.sql.adaptive.enabled", "true")
      .getOrCreate()
  }
  
  /**
   * Stops the current SparkSession gracefully.
   */
  def stopSession(spark: SparkSession): Unit = {
    if (spark != null && !spark.sparkContext.isStopped) {
      spark.stop()
    }
  }
}
