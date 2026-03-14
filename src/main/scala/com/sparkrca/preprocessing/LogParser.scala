package com.sparkrca.preprocessing

import org.apache.spark.sql.{SparkSession, DataFrame, Row}
import org.apache.spark.sql.types._
import org.apache.hadoop.fs.{FileSystem, Path}
import com.github.luben.zstd.ZstdInputStream
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import java.io.{BufferedReader, InputStreamReader}
import scala.collection.mutable.ArrayBuffer

/**
 * Log Parser: Extracts structured metrics from Spark event logs.
 * Supports ZSTD-compressed (.zstd) event logs using zstd-jni library.
 * Handles the directory structure with separate folders per failure type.
 * Uses Jackson ObjectMapper for JSON parsing (compatible with Spark's bundled version).
 */
object LogParser {
  
  // Jackson ObjectMapper (thread-safe for reading)
  private val mapper = new ObjectMapper()

  // Label mapping based on folder names
  val LABEL_MAP: Map[String, Int] = Map(
    "baseline" -> 0,
    "oom" -> 1,
    "skew" -> 2,
    "serialization" -> 3,
    "network" -> 4,
    "disk" -> 5,
    "metadata" -> 6
  )

  // ============================================================================
  // Schema Definitions
  // ============================================================================
  
  val taskMetricsSchema: StructType = StructType(Seq(
    StructField("app_id", StringType, nullable = false),
    StructField("label", IntegerType),
    StructField("stage_id", IntegerType),
    StructField("stage_attempt_id", IntegerType),
    StructField("task_id", LongType),
    StructField("task_attempt", IntegerType),
    StructField("executor_id", StringType),
    StructField("host", StringType),
    StructField("success", BooleanType),
    StructField("duration_ms", LongType),
    StructField("executor_run_time", LongType),
    StructField("jvm_gc_time", LongType),
    StructField("memory_bytes_spilled", LongType),
    StructField("disk_bytes_spilled", LongType),
    StructField("peak_execution_memory", LongType),
    StructField("input_bytes_read", LongType),
    StructField("shuffle_bytes_read", LongType),
    StructField("shuffle_bytes_written", LongType),
    StructField("shuffle_fetch_wait_time", LongType)
  ))

  val stageMetricsSchema: StructType = StructType(Seq(
    StructField("app_id", StringType, nullable = false),
    StructField("label", IntegerType),
    StructField("stage_id", IntegerType),
    StructField("stage_attempt_id", IntegerType),
    StructField("stage_name", StringType),
    StructField("num_tasks", IntegerType),
    StructField("submission_time", LongType),
    StructField("completion_time", LongType),
    StructField("status", StringType),
    StructField("failure_reason", StringType)
  ))

  // ============================================================================
  // JSON Helper Methods (using Jackson)
  // ============================================================================
  
  private def getInt(node: JsonNode, field: String, default: Int = -1): Int = {
    val child = node.get(field)
    if (child != null && child.isNumber) child.asInt() else default
  }
  
  private def getLong(node: JsonNode, field: String, default: Long = 0L): Long = {
    val child = node.get(field)
    if (child != null && child.isNumber) child.asLong() else default
  }
  
  private def getString(node: JsonNode, field: String, default: String = ""): String = {
    val child = node.get(field)
    if (child != null && child.isTextual) child.asText() else default
  }
  
  private def getNode(node: JsonNode, field: String): JsonNode = {
    val child = node.get(field)
    if (child != null) child else mapper.createObjectNode()
  }

  // ============================================================================
  // ZSTD Reading using zstd-jni library
  // ============================================================================

  /**
   * Reads a ZSTD-compressed or plain text event log file.
   * Uses zstd-jni library for .zstd files.
   */
  def readEventLogLines(spark: SparkSession, logPath: String): Seq[String] = {
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)
    val path = new Path(logPath)
    
    val lines = ArrayBuffer[String]()
    
    val inputStream = fs.open(path)
    val reader = if (logPath.endsWith(".zstd")) {
      new BufferedReader(new InputStreamReader(new ZstdInputStream(inputStream)))
    } else {
      new BufferedReader(new InputStreamReader(inputStream))
    }
    
    try {
      var line = reader.readLine()
      while (line != null) {
        lines += line
        line = reader.readLine()
      }
    } finally {
      reader.close()
    }
    
    lines.toSeq
  }

  // ============================================================================
  // Parsing Functions (using Jackson ObjectMapper)
  // ============================================================================

  /**
   * Parses a Spark event log file.
   */
  def parseEventLog(spark: SparkSession, logPath: String, label: Int): (DataFrame, DataFrame) = {
    println(s"  Parsing: ${logPath.split("/").last} (label=$label)")
    
    val lines = readEventLogLines(spark, logPath)
    val appId = extractAppId(logPath)
    
    val taskEvents = lines.filter(_.contains("SparkListenerTaskEnd"))
      .flatMap(line => parseTaskEvent(appId, label, line))
    
    val stageEvents = lines.filter(_.contains("SparkListenerStageCompleted"))
      .flatMap(line => parseStageEvent(appId, label, line))
    
    val taskDf = if (taskEvents.nonEmpty) {
      spark.createDataFrame(spark.sparkContext.parallelize(taskEvents), taskMetricsSchema)
    } else {
      spark.createDataFrame(spark.sparkContext.emptyRDD[Row], taskMetricsSchema)
    }
    
    val stageDf = if (stageEvents.nonEmpty) {
      spark.createDataFrame(spark.sparkContext.parallelize(stageEvents), stageMetricsSchema)
    } else {
      spark.createDataFrame(spark.sparkContext.emptyRDD[Row], stageMetricsSchema)
    }
    
    (taskDf, stageDf)
  }

  /**
   * Parses SparkListenerTaskEnd event into a Row using Jackson.
   */
  def parseTaskEvent(appId: String, label: Int, jsonLine: String): Option[Row] = {
    try {
      val json = mapper.readTree(jsonLine)
      
      val stageId = getInt(json, "Stage ID")
      val stageAttemptId = getInt(json, "Stage Attempt ID", 0)
      val taskInfo = getNode(json, "Task Info")
      val taskMetrics = getNode(json, "Task Metrics")
      val taskEndReason = getNode(json, "Task End Reason")
      
      val taskId = getLong(taskInfo, "Task ID", -1L)
      val taskAttempt = getInt(taskInfo, "Attempt", 0)
      val executorId = getString(taskInfo, "Executor ID")
      val host = getString(taskInfo, "Host")
      val duration = getLong(taskInfo, "Duration")
      
      val reason = getString(taskEndReason, "Reason", "Success")
      val success = reason == "Success"
      
      val executorRunTime = getLong(taskMetrics, "Executor Run Time")
      val jvmGcTime = getLong(taskMetrics, "JVM GC Time")
      val memorySpill = getLong(taskMetrics, "Memory Bytes Spilled")
      val diskSpill = getLong(taskMetrics, "Disk Bytes Spilled")
      val peakMemory = getLong(taskMetrics, "Peak Execution Memory")
      
      val inputMetrics = getNode(taskMetrics, "Input Metrics")
      val inputBytes = getLong(inputMetrics, "Bytes Read")
      
      val shuffleReadMetrics = getNode(taskMetrics, "Shuffle Read Metrics")
      val shuffleRead = getLong(shuffleReadMetrics, "Remote Bytes Read") +
                        getLong(shuffleReadMetrics, "Local Bytes Read")
      val shuffleFetchWait = getLong(shuffleReadMetrics, "Fetch Wait Time")
      
      val shuffleWriteMetrics = getNode(taskMetrics, "Shuffle Write Metrics")
      val shuffleWrite = getLong(shuffleWriteMetrics, "Shuffle Bytes Written")
      
      Some(Row(
        appId, label, stageId, stageAttemptId, taskId, taskAttempt,
        executorId, host, success, duration, executorRunTime, jvmGcTime,
        memorySpill, diskSpill, peakMemory, inputBytes,
        shuffleRead, shuffleWrite, shuffleFetchWait
      ))
    } catch {
      case _: Exception => None
    }
  }

  /**
   * Parses SparkListenerStageCompleted event into a Row using Jackson.
   */
  def parseStageEvent(appId: String, label: Int, jsonLine: String): Option[Row] = {
    try {
      val json = mapper.readTree(jsonLine)
      val stageInfo = getNode(json, "Stage Info")
      
      val stageId = getInt(stageInfo, "Stage ID")
      val stageAttemptId = getInt(stageInfo, "Stage Attempt ID", 0)
      val stageName = getString(stageInfo, "Stage Name")
      val numTasks = getInt(stageInfo, "Number of Tasks", 0)
      val submissionTime = getLong(stageInfo, "Submission Time")
      val completionTime = getLong(stageInfo, "Completion Time")
      
      val failureReasonNode = stageInfo.get("Failure Reason")
      val failureReason = if (failureReasonNode != null && failureReasonNode.isTextual) 
        Some(failureReasonNode.asText()) else None
      val status = if (failureReason.isDefined) "FAILED" else "COMPLETED"
      
      Some(Row(
        appId, label, stageId, stageAttemptId, stageName, numTasks,
        submissionTime, completionTime, status, failureReason.orNull
      ))
    } catch {
      case _: Exception => None
    }
  }

  /**
   * Extracts application ID from log path.
   */
  def extractAppId(logPath: String): String = {
    val filename = logPath.split("/").last
    filename.replace(".zstd", "").replace(".lz4", "").replace(".gz", "")
  }

  // ============================================================================
  // Main Parsing Function for All Logs
  // ============================================================================

  /**
   * Parses all event logs from the structured directory layout.
   * Expects: baseDir/<failure_type>/application_*.zstd
   */
  def parseAllLogsWithLabels(spark: SparkSession, baseDir: String): (DataFrame, DataFrame) = {
    println(s"\n${"=" * 70}")
    println("PARSING EVENT LOGS WITH LABELS")
    println(s"${"=" * 70}")
    println(s"Base directory: $baseDir")
    
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)
    
    var allTasks = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], taskMetricsSchema)
    var allStages = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], stageMetricsSchema)
    
    var totalFiles = 0
    var successfulFiles = 0
    
    // Iterate through each failure type folder
    LABEL_MAP.foreach { case (folderName, label) =>
      val folderPath = new Path(s"$baseDir/$folderName")
      
      if (fs.exists(folderPath)) {
        println(s"\n>>> $folderName (label=$label)")
        val logFiles = fs.listStatus(folderPath)
          .filter(_.getPath.getName.startsWith("application_"))
          .map(_.getPath.toString)
        
        println(s"  Found ${logFiles.length} log files")
        totalFiles += logFiles.length
        
        for (logFile <- logFiles) {
          try {
            val (taskDf, stageDf) = parseEventLog(spark, logFile, label)
            allTasks = allTasks.union(taskDf)
            allStages = allStages.union(stageDf)
            successfulFiles += 1
          } catch {
            case e: Exception =>
              println(s"    FAILED: ${logFile.split("/").last} - ${e.getMessage.take(50)}")
          }
        }
      } else {
        println(s"\n>>> $folderName (label=$label): folder not found")
      }
    }
    
    println(s"\n${"=" * 70}")
    println(s"COMPLETE: $successfulFiles / $totalFiles files")
    println(s"Tasks: ${allTasks.count()}, Stages: ${allStages.count()}")
    println(s"${"=" * 70}\n")
    
    (allTasks, allStages)
  }
}
