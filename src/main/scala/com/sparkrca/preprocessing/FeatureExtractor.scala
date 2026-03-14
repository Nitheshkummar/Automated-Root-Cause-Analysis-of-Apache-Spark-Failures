package com.sparkrca.preprocessing

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.VectorAssembler

/**
 * Feature Extractor V2: Extracts ML features from the ENTIRE APPLICATION lifecycle.
 * Aggregates task metrics across ALL stages (not just root cause) using groupBy(app_id).
 * Outputs 25 features for robust failure classification.
 */
object FeatureExtractor {

  // ============================================================================
  // Feature Columns — The 25 ML features
  // ============================================================================
  
  val FEATURE_COLUMNS: Array[String] = Array(
    // Stage-Level Aggregates (8)
    "mean_task_duration",
    "std_task_duration",
    "max_task_duration",
    "total_memory_spilled",
    "total_disk_spilled",
    "total_shuffle_read",
    "total_shuffle_write",
    "total_gc_time",
    // Structural Features (4)
    "total_stages",
    "failed_stages",
    "max_stage_parallelism",
    "stage_depth_of_failure",
    // Ratio Features (4)
    "completed_stage_ratio",
    "failed_stage_ratio",
    "spill_per_stage",
    "gc_per_stage",
    // Derived Features (9)
    "skew_index",
    "duration_variance",
    "max_min_duration_ratio",
    "spill_ratio",
    "disk_spill_ratio",
    "peak_memory_ratio",
    "gc_time_ratio",
    "task_failure_rate",
    "retry_count"
  )

  // ============================================================================
  // Main Extraction: App-Level Feature Aggregation
  // ============================================================================

  /**
   * Extracts features for ALL applications by grouping task metrics by app_id.
   * Does NOT filter by stage — captures the entire lifecycle.
   * 
   * @param spark SparkSession
   * @param taskDf DataFrame with task metrics (all stages)
   * @param stageDf DataFrame with stage metrics
   * @param groundTruthDf DataFrame with (app_id, label)
   * @return DataFrame with one row per app, containing all 25 features + label
   */
  def extractAllFeatures(
    spark: SparkSession,
    taskDf: DataFrame,
    stageDf: DataFrame,
    groundTruthDf: DataFrame
  ): DataFrame = {
    import spark.implicits._
    
    println("\n" + "=" * 70)
    println("FEATURE EXTRACTION V2: App-Level Aggregation (25 features)")
    println("=" * 70)
    
    // =========================================================================
    // Step 1: Compute task-level duration safely
    // =========================================================================
    val tasksWithDuration = taskDf.withColumn(
      "computed_duration",
      when(col("duration_ms") > 0, col("duration_ms"))
        .otherwise(col("executor_run_time"))
    )
    
    // =========================================================================
    // Step 2: Aggregate task metrics grouped by app_id (across ALL stages)
    // =========================================================================
    println("  Aggregating task metrics by app_id...")
    
    val taskAgg = tasksWithDuration.groupBy("app_id").agg(
      // Stage-Level Aggregates
      avg("computed_duration").as("mean_task_duration"),
      stddev("computed_duration").as("std_task_duration"),
      max("computed_duration").as("max_task_duration"),
      sum("memory_bytes_spilled").as("total_memory_spilled"),
      sum("disk_bytes_spilled").as("total_disk_spilled"),
      sum("shuffle_bytes_read").as("total_shuffle_read"),
      sum("shuffle_bytes_written").as("total_shuffle_write"),
      sum("jvm_gc_time").as("total_gc_time"),
      
      // For derived features
      min("computed_duration").as("min_task_duration"),
      variance("computed_duration").as("duration_variance"),
      avg("peak_execution_memory").as("avg_peak_memory"),
      max("peak_execution_memory").as("max_peak_memory"),
      sum("executor_run_time").as("total_run_time"),
      sum("input_bytes_read").as("total_input"),
      sum(when(col("success") === false, 1).otherwise(0)).as("failed_tasks"),
      count("*").as("total_tasks"),
      // Retry count: tasks with attempt > 0
      sum(when(col("task_attempt") > 0, 1).otherwise(0)).as("retry_count")
    )
    
    // =========================================================================
    // Step 3: Aggregate stage metrics grouped by app_id
    // =========================================================================
    println("  Aggregating stage metrics by app_id...")
    
    val stageAgg = stageDf.groupBy("app_id").agg(
      count("*").as("total_stages"),
      sum(when(col("status") === "FAILED", 1).otherwise(0)).as("failed_stages"),
      sum(when(col("status") === "COMPLETED", 1).otherwise(0)).as("completed_stages"),
      max("num_tasks").as("max_stage_parallelism"),
      // Stage depth of failure: # of completed stages before first failure
      // (approximated by counting completed stages)
      sum(when(col("status") === "COMPLETED", 1).otherwise(0)).as("stage_depth_of_failure")
    )
    
    // =========================================================================
    // Step 4: Join task + stage aggregates
    // =========================================================================
    println("  Joining task and stage aggregates...")
    
    val combinedAgg = taskAgg.join(stageAgg, Seq("app_id"), "left")
    
    // =========================================================================
    // Step 5: Compute derived features
    // =========================================================================
    println("  Computing derived features...")
    
    val withDerived = combinedAgg
      // Fill nulls
      .na.fill(0.0)
      // Ratio features
      .withColumn("completed_stage_ratio",
        when(col("total_stages") > 0,
          col("completed_stages").cast("double") / col("total_stages"))
          .otherwise(0.0))
      .withColumn("failed_stage_ratio",
        when(col("total_stages") > 0,
          col("failed_stages").cast("double") / col("total_stages"))
          .otherwise(0.0))
      .withColumn("spill_per_stage",
        when(col("total_stages") > 0,
          (col("total_memory_spilled") + col("total_disk_spilled")) / col("total_stages"))
          .otherwise(0.0))
      .withColumn("gc_per_stage",
        when(col("total_stages") > 0,
          col("total_gc_time").cast("double") / col("total_stages"))
          .otherwise(0.0))
      // Skew features
      .withColumn("skew_index",
        when(col("mean_task_duration") > 0,
          col("max_task_duration").cast("double") / col("mean_task_duration"))
          .otherwise(1.0))
      .withColumn("max_min_duration_ratio",
        when(col("min_task_duration") > 0,
          col("max_task_duration").cast("double") / col("min_task_duration"))
          .otherwise(1.0))
      // Spill ratios
      .withColumn("spill_ratio",
        when(col("total_input") > 0,
          col("total_memory_spilled").cast("double") / col("total_input"))
          .otherwise(0.0))
      .withColumn("disk_spill_ratio",
        when(col("total_input") > 0,
          col("total_disk_spilled").cast("double") / col("total_input"))
          .otherwise(0.0))
      // Memory ratio
      .withColumn("peak_memory_ratio",
        when(col("avg_peak_memory") > 0,
          col("max_peak_memory").cast("double") / col("avg_peak_memory"))
          .otherwise(1.0))
      // GC ratio
      .withColumn("gc_time_ratio",
        when(col("total_run_time") > 0,
          col("total_gc_time").cast("double") / col("total_run_time"))
          .otherwise(0.0))
      // Task failure rate
      .withColumn("task_failure_rate",
        when(col("total_tasks") > 0,
          col("failed_tasks").cast("double") / col("total_tasks"))
          .otherwise(0.0))
    
    // =========================================================================
    // Step 6: Join with ground truth labels
    // =========================================================================
    println("  Joining with ground truth labels...")
    
    val withLabels = withDerived
      .join(groundTruthDf.select("app_id", "label"), Seq("app_id"), "inner")
    
    // =========================================================================
    // Step 7: Select final feature columns + label
    // =========================================================================
    val finalFeatures = withLabels.select(
      (Seq(col("app_id"), col("label")) ++ FEATURE_COLUMNS.map(c => col(c).cast("double"))):_*
    ).na.fill(0.0)
    
    println(s"\n  Final feature count: ${FEATURE_COLUMNS.length}")
    println(s"  Total apps: ${finalFeatures.count()}")
    
    // Print statistics
    printFeatureStats(finalFeatures)
    
    finalFeatures
  }

  // ============================================================================
  // Feature Vector Assembly for MLlib
  // ============================================================================

  /**
   * Assembles feature columns into a single vector column for MLlib.
   */
  def assembleFeatureVector(featureDf: DataFrame): DataFrame = {
    val assembler = new VectorAssembler()
      .setInputCols(FEATURE_COLUMNS)
      .setOutputCol("features")
      .setHandleInvalid("keep")
    
    assembler.transform(featureDf)
  }

  // ============================================================================
  // Statistics Printing
  // ============================================================================

  /**
   * Prints feature statistics and label distribution for verification.
   */
  def printFeatureStats(featureDf: DataFrame): Unit = {
    println("\n" + "-" * 60)
    println("FEATURE STATISTICS")
    println("-" * 60)
    
    featureDf.describe(
      "mean_task_duration", "std_task_duration", "max_task_duration",
      "total_memory_spilled", "total_disk_spilled",
      "total_shuffle_read", "total_shuffle_write", "total_gc_time",
      "skew_index", "task_failure_rate"
    ).show()
    
    println("\nStructural Feature Stats:")
    featureDf.describe(
      "total_stages", "failed_stages", "max_stage_parallelism",
      "stage_depth_of_failure", "completed_stage_ratio", "failed_stage_ratio"
    ).show()
    
    println("\nLabel Distribution:")
    featureDf.groupBy("label").count().orderBy("label").show()
  }

}
