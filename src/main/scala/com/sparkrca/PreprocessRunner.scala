package com.sparkrca

import com.sparkrca.config.SparkConfig
import com.sparkrca.preprocessing.{LogParser, DAGBuilder, PropagationAnalyzer, FeatureExtractor}

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

/**
 * Preprocessing Runner: Executes the complete preprocessing pipeline
 * and saves outputs from all 4 preprocessing modules to the preprocess/ folder.
 * 
 * Outputs:
 *   1. LogParser     → task_metrics.parquet, stage_metrics.parquet
 *   2. DAGBuilder    → dag_edges.parquet
 *   3. PropagationAnalyzer → root_causes.parquet
 *   4. FeatureExtractor    → features.parquet
 * 
 * Usage: spark-submit --class com.sparkrca.PreprocessRunner spark-rca-assembly.jar [eventLogPath] [outputDir]
 */
object PreprocessRunner {

  def main(args: Array[String]): Unit = {
    println("\n" + "=" * 70)
    println("SPARK RCA - PREPROCESSING PIPELINE")
    println("=" * 70)
    
    // Parse arguments
    val eventLogPath = if (args.length > 0) args(0) else "/project/spark-logs"
    val outputDir = if (args.length > 1) args(1) else "/project/preprocess"
    
    println(s"Event Log Path: $eventLogPath")
    println(s"Output Directory: $outputDir")
    
    val spark = SparkConfig.createSparkSession("RCA-Preprocessing", "yarn", enableEventLog = false)
    
    try {
      runPreprocessing(spark, eventLogPath, outputDir)
    } finally {
      spark.stop()
    }
    
    println("\n" + "=" * 70)
    println("PREPROCESSING COMPLETE")
    println("=" * 70)
  }
  
  /**
   * Runs the complete preprocessing pipeline with outputs saved at each step.
   */
  def runPreprocessing(spark: SparkSession, eventLogPath: String, outputDir: String): Unit = {
    
    // ========================================================================
    // STEP 1: Log Parsing (LogParser.scala)
    // ========================================================================
    println("\n" + "-" * 60)
    println("STEP 1: LOG PARSING")
    println("-" * 60)
    
    val (taskDf, stageDf) = LogParser.parseAllLogsWithLabels(spark, eventLogPath)
    taskDf.cache()
    stageDf.cache()
    
    val taskCount = taskDf.count()
    val stageCount = stageDf.count()
    
    println(s"\n  ✓ Parsed $taskCount task events")
    println(s"  ✓ Parsed $stageCount stage events")
    
    // Save LogParser outputs
    val taskOutputPath = s"$outputDir/task_metrics.parquet"
    val stageOutputPath = s"$outputDir/stage_metrics.parquet"
    
    taskDf.write.mode("overwrite").parquet(taskOutputPath)
    println(s"  ✓ Saved: $taskOutputPath")
    
    stageDf.write.mode("overwrite").parquet(stageOutputPath)
    println(s"  ✓ Saved: $stageOutputPath")
    
    // ========================================================================
    // STEP 2: DAG Building (DAGBuilder.scala)
    // ========================================================================
    println("\n" + "-" * 60)
    println("STEP 2: DAG BUILDING")
    println("-" * 60)
    
    val dags = DAGBuilder.buildAllDAGs(spark, stageDf)
    println(s"\n  ✓ Built ${dags.size} execution DAGs")
    
    // Convert DAGs to DataFrame for saving
    val dagEdgesDf = dagsToEdgesDataFrame(spark, dags)
    val dagOutputPath = s"$outputDir/dag_edges.parquet"
    
    dagEdgesDf.write.mode("overwrite").parquet(dagOutputPath)
    println(s"  ✓ Saved: $dagOutputPath (${dagEdgesDf.count()} edges)")
    
    // ========================================================================
    // STEP 3: Propagation Analysis (PropagationAnalyzer.scala)
    // ========================================================================
    println("\n" + "-" * 60)
    println("STEP 3: PROPAGATION ANALYSIS (Root Cause Detection)")
    println("-" * 60)
    
    val rootCauses = PropagationAnalyzer.analyzeAllApplications(dags)
    println(s"\n  ✓ Identified ${rootCauses.size} root causes")
    
    // Convert to DataFrame and save
    val rootCausesDf = PropagationAnalyzer.resultsToDataFrame(spark, rootCauses)
    val rcaOutputPath = s"$outputDir/root_causes.parquet"
    
    rootCausesDf.write.mode("overwrite").parquet(rcaOutputPath)
    println(s"  ✓ Saved: $rcaOutputPath")
    
    // Print summary of root causes by failure type
    printRootCauseSummary(rootCauses)
    
    // ========================================================================
    // STEP 4: Feature Extraction (FeatureExtractor.scala — V2 App-Level)
    // ========================================================================
    println("\n" + "-" * 60)
    println("STEP 4: FEATURE EXTRACTION (V2 — App-Level Aggregation)")
    println("-" * 60)
    
    // Build ground truth table from parsed task data (one label per app)
    val groundTruthDf = taskDf
      .groupBy("app_id")
      .agg(first("label").as("label"))
    
    println(s"  Ground truth: ${groundTruthDf.count()} applications")
    
    // Extract features using V2 (all stages, 25 features)
    val featureDf = FeatureExtractor.extractAllFeatures(spark, taskDf, stageDf, groundTruthDf)
    
    val featureCount = featureDf.count().toInt
    println(s"\n  ✓ Extracted features for $featureCount applications")
    
    // Save features
    val featuresOutputPath = s"$outputDir/features.parquet"
    featureDf.write.mode("overwrite").parquet(featuresOutputPath)
    println(s"  ✓ Saved: $featuresOutputPath")
    
    // ========================================================================
    // STEP 5: Ground Truth Table + Train/Test Split
    // ========================================================================
    println("\n" + "-" * 60)
    println("STEP 5: GROUND TRUTH TABLE & TRAIN/TEST SPLIT")
    println("-" * 60)
    
    // Label name mapping (matches LogParser.LABEL_MAP)
    val labelNames = Map(
      0 -> "Baseline",
      1 -> "OOM",
      2 -> "Data Skew",
      3 -> "Serialization Failure",
      4 -> "Network Failure",
      5 -> "Disk Spill Overload",
      6 -> "Metadata Failure"
    )
    
    val labelNameUdf = udf((label: Int) => labelNames.getOrElse(label, "Unknown"))
    
    // Ground truth table: app_id, label, label_name
    val gtDf = featureDf
      .select("app_id", "label")
      .withColumn("label_name", labelNameUdf(col("label").cast("int")))
      .orderBy("label", "app_id")
    
    val gtOutputPath = s"$outputDir/ground_truth.parquet"
    gtDf.write.mode("overwrite").parquet(gtOutputPath)
    println(s"  ✓ Saved ground truth: $gtOutputPath (${gtDf.count()} rows)")
    
    println("\n  Ground Truth Table:")
    gtDf.show(100, truncate = false)
    
    println("  Label Distribution:")
    gtDf.groupBy("label", "label_name").count()
      .orderBy("label").show(10, truncate = false)
    
    // 80/20 stratified train/test split (fixed seed for reproducibility)
    println("\n  Performing 80/20 train/test split...")
    val Array(trainDf, testDf) = featureDf.randomSplit(Array(0.8, 0.2), seed = 42L)
    
    val trainOutputPath = s"$outputDir/train.parquet"
    val testOutputPath = s"$outputDir/test.parquet"
    
    trainDf.write.mode("overwrite").parquet(trainOutputPath)
    testDf.write.mode("overwrite").parquet(testOutputPath)
    
    val trainCount = trainDf.count()
    val testCount = testDf.count()
    
    println(s"  ✓ Train set: $trainOutputPath ($trainCount rows)")
    println(s"  ✓ Test set:  $testOutputPath ($testCount rows)")
    
    println("\n  Train label distribution:")
    trainDf.groupBy("label").count().orderBy("label").show()
    
    println("  Test label distribution:")
    testDf.groupBy("label").count().orderBy("label").show()
    
    // ========================================================================
    // Summary
    // ========================================================================
    printPipelineSummary(outputDir, taskCount, stageCount, dags.size, rootCauses.size, featureCount, trainCount, testCount)
    
    // Unpersist cached data
    taskDf.unpersist()
    stageDf.unpersist()
  }
  
  /**
   * Converts all DAGs to a single DataFrame of edges.
   */
  private def dagsToEdgesDataFrame(
    spark: SparkSession, 
    dags: Map[String, DAGBuilder.ExecutionDAG]
  ): DataFrame = {
    import spark.implicits._
    
    val allEdges = dags.values.flatMap { dag =>
      dag.stages.values.flatMap { stage =>
        if (stage.parentIds.isEmpty) {
          // Root stage - create a self-reference for completeness
          Seq((dag.appId, stage.stageId, -1, stage.stageName, stage.status))
        } else {
          stage.parentIds.map { parentId =>
            (dag.appId, stage.stageId, parentId, stage.stageName, stage.status)
          }
        }
      }
    }.toSeq
    
    allEdges.toDF("app_id", "stage_id", "parent_stage_id", "stage_name", "status")
  }
  
  /**
   * Prints summary of root causes by failure type.
   */
  private def printRootCauseSummary(rootCauses: Seq[PropagationAnalyzer.RootCauseResult]): Unit = {
    println("\n  Root Cause Summary:")
    
    val byReason = rootCauses.groupBy { rc =>
      rc.failureReason.take(30)
    }
    
    byReason.foreach { case (reason, results) =>
      println(f"    ${reason}%-35s: ${results.size} occurrences")
    }
  }
  
  /**
   * Prints final pipeline summary.
   */
  private def printPipelineSummary(
    outputDir: String,
    taskCount: Long,
    stageCount: Long,
    dagCount: Int,
    rootCauseCount: Int,
    featureCount: Int,
    trainCount: Long,
    testCount: Long
  ): Unit = {
    println("\n" + "=" * 60)
    println("PIPELINE SUMMARY")
    println("=" * 60)
    println(s"  Output Directory: $outputDir")
    println(s"  Tasks Parsed:     $taskCount")
    println(s"  Stages Parsed:    $stageCount")
    println(s"  DAGs Built:       $dagCount")
    println(s"  Root Causes:      $rootCauseCount")
    println(s"  Features:         $featureCount")
    println(s"  Train Set:        $trainCount")
    println(s"  Test Set:         $testCount")
    println()
    println("  Output Files:")
    println(s"    1. task_metrics.parquet   - Task-level metrics")
    println(s"    2. stage_metrics.parquet  - Stage-level metrics")
    println(s"    3. dag_edges.parquet      - DAG structure (edges)")
    println(s"    4. root_causes.parquet    - Root cause analysis results")
    println(s"    5. features.parquet       - ML feature vectors")
    println(s"    6. ground_truth.parquet   - Ground truth labels")
    println(s"    7. train.parquet          - Training set (80%)")
    println(s"    8. test.parquet           - Test set (20%)")
    println("=" * 60)
  }
}
