package com.sparkrca.injection

import org.apache.spark.launcher.{SparkLauncher, SparkAppHandle}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

import java.time.{LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.concurrent.{CountDownLatch, TimeUnit}
import scala.collection.mutable.ListBuffer

/**
 * Campaign Runner: Automated Failure Injection Orchestrator
 * 
 * Generates 75 labeled log files for ML training:
 * - Group A (Baseline): 15 runs with varying executor cores
 * - Group B (Resource Failures): OOM(10) + DISK(10) + NET(10) = 30 runs
 * - Group C (Logic Failures): SKEW(10) + SER(10) + META(10) = 30 runs
 * 
 * Usage: spark-submit --class com.sparkrca.injection.CampaignRunner \
 *          --master yarn --deploy-mode client \
 *          spark-rca-assembly.jar
 * 
 * @author Spark RCA Project
 */
object CampaignRunner {

  // ============================================================================
  // Configuration
  // ============================================================================
  
  val JAR_PATH = "hdfs:///project/lib/spark-rca-assembly.jar"
  val MANIFEST_PATH = "/project/spark-logs/run_manifest.csv"
  val MAIN_CLASS = "com.sparkrca.injection.TPCHFailureSuite"
  
  // Scenario definitions with variance strategies
  case class ScenarioConfig(
    name: String,
    failureType: String,
    label: Int,
    logDir: String,  // Scenario-specific event log directory
    totalRuns: Int,
    getConfigs: Int => Map[String, String]  // Function: runNumber => configs
  )
  
  val SCENARIOS: List[ScenarioConfig] = List(
    // =========================================================================
    // GROUP A: Baseline (20 runs) — 4 query variations
    // =========================================================================
    ScenarioConfig(
      name = "BASELINE",
      failureType = "BASELINE",
      label = 0,
      logDir = "hdfs:///project/spark-logs/baseline",
      totalRuns = 12,
      getConfigs = (run: Int) => {
        val cores = if (run % 2 == 0) "1" else "2"
        val mem = if (run <= 10) "4g" else "2g"
        Map(
          "spark.executor.memory" -> mem,
          "spark.executor.memoryOverhead" -> "512m",
          "spark.executor.cores" -> cores
        )
      }
    ),
    
    // =========================================================================
    // GROUP B: Resource Failures (45 runs total)
    // =========================================================================
    
    // OOM: 15 runs, vary memory (400m–700m) and overhead
    ScenarioConfig(
      name = "OOM",
      failureType = "OOM",
      label = 1,
      logDir = "hdfs:///project/spark-logs/oom",
      totalRuns = 12,
      getConfigs = (run: Int) => {
        val memMb = 400 + (run % 5) * 75  // 400, 475, 550, 625, 700
        val overheadMb = 200 + (run % 3) * 100  // 200, 300, 400
        Map(
          "spark.executor.memory" -> s"${memMb}m",
          "spark.executor.memoryOverhead" -> s"${overheadMb}m",
          "spark.executor.cores" -> (if (run % 2 == 0) "1" else "2"),
          "spark.sql.autoBroadcastJoinThreshold" -> "-1"
        )
      }
    ),
    
    // DISK: 15 runs, vary memory.fraction (0.4–0.9)
    ScenarioConfig(
      name = "DISK",
      failureType = "DISK",
      label = 5,
      logDir = "hdfs:///project/spark-logs/disk",
      totalRuns = 11,
      getConfigs = (run: Int) => {
        val fraction = (0.4 + (run % 5) * 0.1).toString  // 0.4, 0.5, 0.6, 0.7, 0.8
        val mem = if (run <= 5) "2g" else if (run <= 10) "1g" else "3g"
        Map(
          "spark.executor.memory" -> mem,
          "spark.executor.memoryOverhead" -> "512m",
          "spark.memory.fraction" -> fraction,
          "spark.sql.crossJoin.enabled" -> "true",
          "spark.executor.cores" -> (if (run % 2 == 0) "1" else "2")
        )
      }
    ),
    
    // NET: 11 runs, vary timeout/heartbeat/memory/cores for distinct logs
    ScenarioConfig(
      name = "NET",
      failureType = "NET",
      label = 4,
      logDir = "hdfs:///project/spark-logs/network",
      totalRuns = 11,
      getConfigs = (run: Int) => {
        val timeout = (20 + (run % 6) * 20).toString + "s"  // 20s,40s,60s,80s,100s,120s
        val heartbeat = (5 + (run % 4) * 8).toString + "s"  // 5s,13s,21s,29s
        val mem = if (run <= 4) "2g" else if (run <= 8) "3g" else "4g"
        val cores = if (run % 3 == 0) "2" else "1"
        val partitions = (100 + (run % 5) * 50).toString  // 100-350
        Map(
          "spark.executor.memory" -> mem,
          "spark.executor.memoryOverhead" -> "512m",
          "spark.network.timeout" -> timeout,
          "spark.executor.heartbeatInterval" -> heartbeat,
          "spark.executor.cores" -> cores,
          "spark.sql.shuffle.partitions" -> partitions
        )
      }
    ),
    
    // =========================================================================
    // GROUP C: Logic Failures (45 runs total)
    // =========================================================================
    
    // SKEW: 15 runs, vary shuffle.partitions (50–300) and memory
    ScenarioConfig(
      name = "SKEW",
      failureType = "SKEW",
      label = 2,
      logDir = "hdfs:///project/spark-logs/skew",
      totalRuns = 12,
      getConfigs = (run: Int) => {
        val partitions = (50 + (run % 5) * 50).toString  // 50, 100, 150, 200, 250
        val mem = if (run <= 8) "4g" else "2g"
        Map(
          "spark.executor.memory" -> mem,
          "spark.executor.memoryOverhead" -> "512m",
          "spark.sql.shuffle.partitions" -> partitions,
          "spark.sql.adaptive.enabled" -> "false"
        )
      }
    ),
    
    // SER: 15 runs, vary serializer and cores
    ScenarioConfig(
      name = "SER",
      failureType = "SER",
      label = 3,
      logDir = "hdfs:///project/spark-logs/serialization",
      totalRuns = 11,
      getConfigs = (run: Int) => {
        val serializer = if (run % 2 == 0)
          "org.apache.spark.serializer.KryoSerializer"
        else
          "org.apache.spark.serializer.JavaSerializer"
        val cores = if (run % 3 == 0) "2" else "1"
        Map(
          "spark.executor.memory" -> "2g",
          "spark.executor.memoryOverhead" -> "512m",
          "spark.serializer" -> serializer,
          "spark.executor.cores" -> cores
        )
      }
    ),
    
    // META: 11 runs, vary memory/partitions/cores for distinct logs
    ScenarioConfig(
      name = "META",
      failureType = "META",
      label = 6,
      logDir = "hdfs:///project/spark-logs/metadata",
      totalRuns = 11,
      getConfigs = (run: Int) => {
        val mem = if (run <= 4) "2g" else if (run <= 8) "3g" else "4g"
        val partitions = (50 + (run % 6) * 50).toString  // 50,100,150,200,250,300
        val cores = if (run % 2 == 0) "2" else "1"
        val fraction = (0.5 + (run % 4) * 0.1).toString  // 0.5,0.6,0.7,0.8
        Map(
          "spark.executor.memory" -> mem,
          "spark.executor.memoryOverhead" -> "512m",
          "spark.sql.shuffle.partitions" -> partitions,
          "spark.executor.cores" -> cores,
          "spark.memory.fraction" -> fraction
        )
      }
    )

  )

  // ============================================================================
  // Main Entry Point
  // ============================================================================
  
  def main(args: Array[String]): Unit = {
    // Parse --skip argument: e.g. --skip BASELINE,OOM
    val skipSet: Set[String] = args.sliding(2).collectFirst {
      case Array("--skip", names) => names.split(",").map(_.trim.toUpperCase).toSet
    }.getOrElse(Set.empty)

    // Parse --autoResume flag: reads manifest and skips already-completed runs
    val autoResume = args.contains("--autoResume")

    val activeScenarios = if (skipSet.nonEmpty) {
      SCENARIOS.filterNot(s => skipSet.contains(s.name.toUpperCase))
    } else {
      SCENARIOS
    }

    val totalJobs = activeScenarios.map(_.totalRuns).sum
    
    println(s"\n${"=" * 70}")
    println("CAMPAIGN RUNNER: Failure Injection Orchestrator")
    println(s"${"=" * 70}")
    println(s"Scenarios: ${activeScenarios.size}")
    if (skipSet.nonEmpty) println(s"Skipped: ${skipSet.mkString(", ")}")
    println(s"Total jobs to submit: $totalJobs")
    println(s"Output directory: hdfs:///project/spark-logs/<scenario>/")
    println(s"${"=" * 70}")
    println()
    println("Jobs breakdown:")
    activeScenarios.foreach { s =>
      println(f"  ${s.name}%-12s: ${s.totalRuns}%3d runs (Label: ${s.label})")
    }
    println(s"${"=" * 70}\n")
    
    // Initialize HDFS for manifest logging
    val conf = new Configuration()
    conf.set("fs.defaultFS", "hdfs://namenode:8020")
    val fs = FileSystem.get(conf)

    // Build set of already-completed (failureType, runId) pairs when --autoResume is set.
    // Runs with status FINISHED or FAILED have event logs — skip them.
    // Runs with TIMEOUT or ERROR are retried.
    val completedRuns: Set[(String, String)] = if (autoResume) {
      val manifestPath = new Path(MANIFEST_PATH)
      if (fs.exists(manifestPath)) {
        val in = fs.open(manifestPath)
        val content = try {
          val bytes = new Array[Byte](in.available())
          in.readFully(bytes)
          new String(bytes, "UTF-8")
        } finally { in.close() }
        // CSV format: AppName,FailureType,Label,RunId,Timestamp,AppId,Status
        val parsed = content.split("\n").drop(1)
          .filter(_.nonEmpty)
          .flatMap { line =>
            val parts = line.split(",")
            if (parts.length >= 7) {
              val failureType = parts(1).trim
              val runId       = parts(3).trim
              val status      = parts(6).trim
              if (status == "FINISHED" || status == "FAILED") Some((failureType, runId))
              else None
            } else None
          }.toSet
        println(s"Auto-resume: ${parsed.size} runs already completed — those will be skipped")
        parsed
      } else {
        println("Auto-resume: no manifest found, starting from scratch")
        Set.empty[(String, String)]
      }
    } else Set.empty[(String, String)]

    // Append to existing manifest if resuming, otherwise create new
    val resume = skipSet.nonEmpty || autoResume
    if (resume) {
      ensureManifestExists(fs)
    } else {
      initializeManifest(fs)
    }
    
    // Track results
    val results = ListBuffer[(String, Int, String, String)]()
    var jobCount = 0
    var skippedCount = 0
    
    // Run each scenario
    for (scenario <- activeScenarios) {
      println(s"\n>>> Scenario: ${scenario.name} (Label: ${scenario.label}, Runs: ${scenario.totalRuns})")
      println("-" * 60)
      
      for (run <- 1 to scenario.totalRuns) {
        jobCount += 1
        val runId = f"$run%03d"
        val appName = s"RCA_${scenario.failureType}_Run_$runId"
        val configs = scenario.getConfigs(run)

        // Auto-resume: skip runs already present in the manifest
        if (autoResume && completedRuns.contains((scenario.failureType, runId))) {
          skippedCount += 1
          println(s"  [$jobCount/$totalJobs] SKIP (done): $appName")
        } else {
          println(s"  [$jobCount/$totalJobs] Submitting: $appName")
          println(s"      Configs: ${configs.filterKeys(k => k.contains("memory") || k.contains("cores") || k.contains("timeout") || k.contains("partitions") || k.contains("fraction")).mkString(", ")}")
          
          val timestamp = LocalDateTime.now(ZoneId.of("UTC"))
            .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
          
          try {
            val (appId, finalState) = submitJob(scenario.failureType, runId, configs, scenario.logDir)
            
            logToManifest(fs, appName, scenario.failureType, scenario.label, runId, timestamp, appId, finalState)
            results += ((appName, scenario.label, appId, finalState))
            
            println(s"      AppId: $appId, Final State: $finalState")
            
          } catch {
            case e: Exception =>
              println(s"      ERROR: ${e.getMessage}")
              logToManifest(fs, appName, scenario.failureType, scenario.label, runId, timestamp, "N/A", s"ERROR: ${e.getMessage.take(100)}")
              results += ((appName, scenario.label, "N/A", s"ERROR: ${e.getMessage.take(50)}"))
          }
        }
      }
    }
    
    if (autoResume && skippedCount > 0)
      println(s"\nAuto-resume: skipped $skippedCount already-completed runs")

    // Print summary
    printSummary(results.toList, totalJobs, activeScenarios)
    
    fs.close()
    println(s"\n✓ Campaign completed! Manifest: hdfs://$MANIFEST_PATH")
  }

  /**
   * Submits a single failure injection job to YARN and WAITS for completion.
   * This is a blocking call that returns only when the job finishes.
   * 
   * @return Tuple of (AppId, FinalState)
   */
  def submitJob(failureType: String, runId: String, configs: Map[String, String], logDir: String): (String, String) = {
    val launcher = new SparkLauncher()
      .setAppResource(JAR_PATH)
      .setMainClass(MAIN_CLASS)
      .setMaster("yarn")
      .setDeployMode("cluster")  // Cluster mode for proper isolation
      .addAppArgs(failureType, runId)
    
    // Apply scenario-specific configurations
    configs.foreach { case (key, value) =>
      launcher.setConf(key, value)
    }
    
    // CRITICAL: Prevent port exhaustion and enable logging
    launcher.setConf("spark.ui.enabled", "false")  // Prevent BindException
    launcher.setConf("spark.yarn.maxAppAttempts", "1")  // Fail fast
    launcher.setConf("spark.eventLog.enabled", "true")
    launcher.setConf("spark.eventLog.dir", logDir)  // Scenario-specific log directory
    launcher.setConf("spark.eventLog.compress", "true")
    launcher.setConf("spark.yarn.submit.waitAppCompletion", "true")
    
    // Use latch to wait for COMPLETION (not just submission)
    val completionLatch = new CountDownLatch(1)
    var appId = "PENDING"
    var finalState = "UNKNOWN"
    
    val handle = launcher.startApplication(new SparkAppHandle.Listener {
      override def stateChanged(handle: SparkAppHandle): Unit = {
        val state = handle.getState
        println(s"      [STATE] ${handle.getAppId}: $state")
        
        if (handle.getAppId != null) {
          appId = handle.getAppId
        }
        
        // Only countdown when job is FINISHED (success, failed, or killed)
        if (state.isFinal) {
          finalState = state.toString
          completionLatch.countDown()
        }
      }
      
      override def infoChanged(handle: SparkAppHandle): Unit = {
        if (handle.getAppId != null) {
          appId = handle.getAppId
        }
      }
    })
    
    // Wait for job to COMPLETE (timeout: 10 minutes per job)
    val completed = completionLatch.await(10, TimeUnit.MINUTES)
    
    if (!completed) {
      finalState = "TIMEOUT"
      println(s"      [TIMEOUT] Job $appId timed out after 10 minutes")
      try { handle.kill() } catch { case _: Exception => }
    }
    
    (if (handle.getAppId != null) handle.getAppId else appId, finalState)
  }

  // ============================================================================
  // HDFS Operations
  // ============================================================================

  
  /**
   * Initializes the manifest CSV file with header.
   */
  def initializeManifest(fs: FileSystem): Unit = {
    val path = new Path(MANIFEST_PATH)
    
    val parent = path.getParent
    if (!fs.exists(parent)) {
      fs.mkdirs(parent)
    }
    
    val out = fs.create(path, true)
    try {
      val header = "AppName,FailureType,Label,RunId,Timestamp,AppId,Status\n"
      out.writeBytes(header)
    } finally {
      out.close()
    }
    println(s"Initialized manifest: hdfs://$MANIFEST_PATH")
  }

  /**
   * Ensures the manifest file exists (for resume mode). Creates with header if missing.
   */
  def ensureManifestExists(fs: FileSystem): Unit = {
    val path = new Path(MANIFEST_PATH)
    val parent = path.getParent
    if (!fs.exists(parent)) {
      fs.mkdirs(parent)
    }
    if (!fs.exists(path)) {
      initializeManifest(fs)
    } else {
      println(s"Resuming — appending to existing manifest: hdfs://$MANIFEST_PATH")
    }
  }
  
  /**
   * Appends a run record to the manifest file.
   */
  def logToManifest(
    fs: FileSystem,
    appName: String,
    failureType: String,
    label: Int,
    runId: String,
    timestamp: String,
    appId: String,
    status: String
  ): Unit = {
    val path = new Path(MANIFEST_PATH)
    
    // Read existing content
    val existingContent = if (fs.exists(path)) {
      val in = fs.open(path)
      try {
        val bytes = new Array[Byte](in.available())
        in.readFully(bytes)
        new String(bytes, "UTF-8")
      } finally {
        in.close()
      }
    } else {
      "AppName,FailureType,Label,RunId,Timestamp,AppId,Status\n"
    }
    
    val newLine = s"$appName,$failureType,$label,$runId,$timestamp,$appId,$status\n"
    
    val out = fs.create(path, true)
    try {
      out.writeBytes(existingContent + newLine)
    } finally {
      out.close()
    }
  }

  // ============================================================================
  // Summary
  // ============================================================================
  
  def printSummary(results: List[(String, Int, String, String)], total: Int, scenarios: List[ScenarioConfig] = SCENARIOS): Unit = {
    println(s"\n${"=" * 70}")
    println("CAMPAIGN SUMMARY")
    println(s"${"=" * 70}")
    
    val submitted = results.count(_._4 == "SUBMITTED")
    val errors = results.count(_._4.startsWith("ERROR"))
    
    println(s"Total Jobs: $total")
    println(s"Submitted Successfully: $submitted")
    println(s"Errors: $errors")
    
    println(s"\nBreakdown by Scenario:")
    scenarios.foreach { scenario =>
      val scenarioResults = results.filter(_._2 == scenario.label)
      val success = scenarioResults.count(_._4 == "SUBMITTED")
      val failed = scenarioResults.count(_._4.startsWith("ERROR"))
      println(f"  ${scenario.name}%-12s: $success%3d submitted, $failed%3d errors")
    }
    
    println(s"${"=" * 70}")
  }
}
