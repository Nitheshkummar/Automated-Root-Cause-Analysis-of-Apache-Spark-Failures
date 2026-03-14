package com.sparkrca

import com.sparkrca.config.SparkConfig
import com.sparkrca.datalake.TPCHParquetConverter
import com.sparkrca.injection.FailureScenarios
import com.sparkrca.preprocessing.{LogParser, DAGBuilder, PropagationAnalyzer, FeatureExtractor}

import org.apache.spark.sql.functions._
import scopt.OParser

/**
 * Main Entry Point for the Spark RCA Project.
 * Orchestrates the complete pipeline: Convert → Inject → Preprocess → Train.
 */
object Main {

  // ============================================================================
  // CLI Configuration
  // ============================================================================

  case class Config(
    mode: String = "help",
    master: String = "yarn",
    tpchRawPath: String = SparkConfig.Paths.TPCH_RAW,
    tpchParquetPath: String = SparkConfig.Paths.TPCH_PARQUET,
    eventLogPath: String = SparkConfig.Paths.EVENT_LOGS,
    modelPath: String = SparkConfig.Paths.ML_MODELS,
    scenario: String = "all",
    iterations: Int = 1
  )

  val builder = OParser.builder[Config]
  val parser: OParser[Unit, Config] = {
    import builder._
    OParser.sequence(
      programName("spark-rca"),
      head("Spark Root Cause Analysis", "1.0.0"),
      
      opt[String]("master")
        .action((x, c) => c.copy(master = x))
        .text("Spark master URL (yarn, local[*], etc.)"),
      
      cmd("convert")
        .action((_, c) => c.copy(mode = "convert"))
        .text("Convert TPC-H raw data to Parquet format")
        .children(
          opt[String]("raw")
            .action((x, c) => c.copy(tpchRawPath = x))
            .text("Path to raw TPC-H data"),
          opt[String]("parquet")
            .action((x, c) => c.copy(tpchParquetPath = x))
            .text("Output path for Parquet data")
        ),
        
      cmd("inject")
        .action((_, c) => c.copy(mode = "inject"))
        .text("Run failure injection scenarios (each scenario runs as separate app)")
        .children(
          opt[String]("scenario")
            .action((x, c) => c.copy(scenario = x))
            .text("Scenario to run: all, baseline, oom, skew, serialization, metadata, disk, network"),
          opt[Int]("iterations")
            .action((x, c) => c.copy(iterations = x))
            .text("Number of iterations per scenario (default: 1)")
        ),
        
      cmd("preprocess")
        .action((_, c) => c.copy(mode = "preprocess"))
        .text("Parse logs, build DAGs, and extract features")
        .children(
          opt[String]("logs")
            .action((x, c) => c.copy(eventLogPath = x))
            .text("Path to Spark event logs")
        ),
        
      cmd("train")
        .action((_, c) => c.copy(mode = "train"))
        .text("Train the Random Forest classifier")
        .children(
          opt[String]("model")
            .action((x, c) => c.copy(modelPath = x))
            .text("Output path for trained model")
        ),
        
      cmd("predict")
        .action((_, c) => c.copy(mode = "predict"))
        .text("Make predictions on new event logs"),
        
      cmd("pipeline")
        .action((_, c) => c.copy(mode = "pipeline"))
        .text("Run the complete pipeline end-to-end"),
        
      cmd("info")
        .action((_, c) => c.copy(mode = "info"))
        .text("Print project information and configuration"),
        
      help("help").text("Print this usage text")
    )
  }

  // ============================================================================
  // Main Entry Point
  // ============================================================================

  def main(args: Array[String]): Unit = {
    
    printBanner()
    
    OParser.parse(parser, args, Config()) match {
      case Some(config) =>
        config.mode match {
          case "convert" => runConvert(config)
          case "inject" => runInject(config)
          case "preprocess" => runPreprocess(config)
          case "pipeline" => runPipeline(config)
          case "info" => printInfo(config)
          case "help" | _ => OParser.usage(parser)
        }
      case None =>
        System.exit(1)
    }
  }

  // ============================================================================
  // Command Implementations
  // ============================================================================

  /**
   * Converts TPC-H raw data to Parquet format.
   */
  def runConvert(config: Config): Unit = {
    println("\n>>> PHASE 1: Data Lake Foundation")
    
    val spark = SparkConfig.createSparkSession("RCA-Convert", config.master, enableEventLog = false)
    
    try {
      TPCHParquetConverter.convertAllTables(
        spark,
        config.tpchRawPath,
        config.tpchParquetPath
      )
      println("\n✓ Conversion completed successfully!")
    } finally {
      SparkConfig.stopSession(spark)
    }
  }

  /**
   * Runs failure injection scenarios.
   * Each scenario runs as a SEPARATE Spark application to generate distinct event logs.
   */
  def runInject(config: Config): Unit = {
    println("\n>>> PHASE 2: Failure Factory")
    println(s">>> Running with ${config.iterations} iteration(s) per scenario")
    
    FailureScenarios.printScenarioSummary()
    
    // Determine which scenarios to run
    val scenariosToRun: Seq[injection.FailureScenario] = config.scenario.toLowerCase match {
      case "all" => FailureScenarios.allScenarios
      case "baseline" => Seq(FailureScenarios.Baseline)
      case "oom" => Seq(FailureScenarios.OutOfMemory)
      case "skew" => Seq(FailureScenarios.DataSkew)
      case "serialization" => Seq(FailureScenarios.SerializationFailure)
      case "metadata" => Seq(FailureScenarios.MetadataFailure)
      case "disk" => Seq(FailureScenarios.DiskSpaceFailure)
      case "network" => Seq(FailureScenarios.NetworkTimeout)
      case _ => 
        println(s"Unknown scenario: ${config.scenario}")
        Seq.empty
    }
    
    if (scenariosToRun.isEmpty) {
      println("No scenarios to run!")
      return
    }
    
    val totalApps = scenariosToRun.size * config.iterations
    println(s"\n>>> Will run ${scenariosToRun.size} scenarios × ${config.iterations} iterations = $totalApps applications")
    println("=" * 70)
    
    var appCount = 0
    val results = scala.collection.mutable.ListBuffer[(String, Int, Boolean, String)]()
    
    for (iteration <- 1 to config.iterations) {
      for (scenario <- scenariosToRun) {
        appCount += 1
        val appName = s"RCA-${scenario.name}-iter$iteration"
        
        println(s"\n[$appCount/$totalApps] Running: ${scenario.name} (Iteration $iteration)")
        println("-" * 50)
        
        // Create a NEW SparkSession for each scenario (generates separate event log)
        // Route event logs to scenario-specific subdirectory
        val scenarioSubDir = scenario.name.toLowerCase
        val spark = SparkConfig.createSparkSession(appName, config.master, enableEventLog = true, eventLogSubDir = Some(scenarioSubDir))
        
        try {
          val (label, name, result) = FailureScenarios.runScenario(spark, scenario)
          
          result match {
            case scala.util.Success(_) =>
              println(s"  ✓ ${scenario.name}: SUCCESS")
              results += ((name.toString, iteration, true, "Completed successfully"))
            case scala.util.Failure(e) =>
              // Failures are expected for some scenarios (e.g., serialization, OOM)
              println(s"  ✗ ${scenario.name}: FAILED (expected for failure injection) - ${e.getMessage.take(80)}")
              results += ((name.toString, iteration, false, e.getMessage.take(100)))
          }
        } finally {
          // Stop session to finalize event log and allow new session
          SparkConfig.stopSession(spark)
          Thread.sleep(2000) // Give YARN time to clean up between apps
        }
      }
    }
    
    // Print summary
    println("\n" + "=" * 70)
    println("INJECTION SUMMARY")
    println("=" * 70)
    println(f"${"Scenario"}%-25s | ${"Iter"}%-4s | ${"Status"}%-10s | ${"Message"}%s")
    println("-" * 70)
    results.foreach { case (name, iter, success, msg) =>
      val status = if (success) "SUCCESS" else "FAILED"
      println(f"$name%-25s | $iter%-4d | $status%-10s | ${msg.take(30)}%s")
    }
    println("=" * 70)
    println(s"\nTotal applications run: $appCount")
    println(s"Successful: ${results.count(_._3)}")
    println(s"Failed (expected): ${results.count(!_._3)}")
    
    println("\n✓ Injection completed! Event logs written to HDFS.")
  }

  /**
   * Runs the preprocessing pipeline (log parsing, DAG building, feature extraction).
   */
  def runPreprocess(config: Config): Unit = {
    println("\n>>> PHASE 3: Intelligence Core")
    
    val spark = SparkConfig.createSparkSession("RCA-Preprocess", config.master, enableEventLog = false)
    
    try {
      // Step 1: Parse logs
      println("\n--- Step 1: Log Parsing ---")
      val (taskDf, stageDf) = LogParser.parseAllLogsWithLabels(spark, config.eventLogPath)
      taskDf.cache()
      stageDf.cache()
      
      println(s"Parsed ${taskDf.count()} task events")
      println(s"Parsed ${stageDf.count()} stage events")
      
      // Step 2: Build DAGs
      println("\n--- Step 2: DAG Building ---")
      val dags = DAGBuilder.buildAllDAGs(spark, stageDf)
      println(s"Built ${dags.size} DAGs")
      
      // Step 3: Analyze propagation
      println("\n--- Step 3: Propagation Analysis ---")
      val rootCauses = PropagationAnalyzer.analyzeAllApplications(dags)
      println(s"Identified ${rootCauses.size} root causes")
      
      // Print summary
      rootCauses.foreach(_.printSummary())
      
      // Step 4: Extract features using V2 (app-level aggregation)
      println("\n--- Step 4: Feature Extraction ---")
      val groundTruthDf = taskDf.groupBy("app_id").agg(first("label").as("label"))
      val featureDf = FeatureExtractor.extractAllFeatures(spark, taskDf, stageDf, groundTruthDf)
      
      FeatureExtractor.printFeatureStats(featureDf)
      
      // Save features
      val outputPath = s"${SparkConfig.Paths.EXTRACTED_FEATURES}"
      featureDf.write.mode("overwrite").parquet(outputPath)
      println(s"\n✓ Features saved to: $outputPath")
      
    } finally {
      SparkConfig.stopSession(spark)
    }
  }

  /**
   * Runs the complete pipeline end-to-end.
   */
  def runPipeline(config: Config): Unit = {
    println("\n>>> RUNNING COMPLETE PIPELINE")
    println("=" * 70)
    
    runConvert(config)
    runInject(config)
    runPreprocess(config)
    
    println("\n" + "=" * 70)
    println("PIPELINE COMPLETED SUCCESSFULLY!")
    println("=" * 70)
  }

  /**
   * Prints project information.
   */
  def printInfo(config: Config): Unit = {
    printBanner()
    
    println("\nConfiguration:")
    println(s"  Master: ${config.master}")
    println(s"  TPC-H Raw: ${config.tpchRawPath}")
    println(s"  TPC-H Parquet: ${config.tpchParquetPath}")
    println(s"  Event Logs: ${config.eventLogPath}")
    println(s"  Model Path: ${config.modelPath}")
    
    println("\nAvailable Commands:")
    println("  convert    - Convert TPC-H raw data to Parquet")
    println("  inject     - Run failure injection scenarios")
    println("  preprocess - Parse logs and extract features")
    println("  train      - Train the ML classifier")
    println("  predict    - Make predictions on new logs")
    println("  pipeline   - Run complete end-to-end pipeline")
    
    FailureScenarios.printScenarioSummary()
  }

  /**
   * Prints the project banner.
   */
  def printBanner(): Unit = {
    println(
      """
        |╔══════════════════════════════════════════════════════════════════╗
        |║     SPARK ROOT CAUSE ANALYSIS (RCA) PROJECT                      ║
        |║     Automated Failure Diagnosis using DAG Propagation & ML       ║
        |╠══════════════════════════════════════════════════════════════════╣
        |║  Version: 1.0.0                                                  ║
        |║  Spark: 3.5.1  |  Scala: 2.12  |  ML: Random Forest              ║
        |╚══════════════════════════════════════════════════════════════════╝
      """.stripMargin)
  }
}
