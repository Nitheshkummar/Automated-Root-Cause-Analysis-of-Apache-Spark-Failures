package com.sparkrca.preprocessing

import org.apache.spark.sql.{SparkSession, DataFrame}
import scala.collection.mutable

/**
 * Propagation Analyzer: The "Brain" of the RCA system.
 * Uses Reverse Breadth-First Search (BFS) to identify the Root Cause Stage.
 */
object PropagationAnalyzer {

  /**
   * Result of root cause analysis.
   */
  case class RootCauseResult(
    appId: String,
    rootCauseStageId: Int,
    rootCauseStageName: String,
    failureReason: String,
    propagationPath: Seq[Int],    // Path from root cause to terminal failure
    victimStages: Set[Int],       // Stages that failed due to propagation
    analysisConfidence: Double    // Confidence in the analysis (0-1)
  ) {
    
    def printSummary(): Unit = {
      println("\n" + "=" * 60)
      println("ROOT CAUSE ANALYSIS RESULT")
      println("=" * 60)
      println(s"Application: $appId")
      println(s"Root Cause Stage: $rootCauseStageId ($rootCauseStageName)")
      println(s"Failure Reason: $failureReason")
      println(s"Propagation Path: ${propagationPath.mkString(" → ")}")
      println(s"Victim Stages: ${victimStages.mkString(", ")}")
      println(f"Analysis Confidence: ${analysisConfidence * 100}%.1f%%")
      println("=" * 60)
    }
  }

  // ============================================================================
  // Core Analysis Algorithm
  // ============================================================================

  /**
   * Identifies the Root Cause Stage using Reverse BFS.
   * 
   * Algorithm:
   * 1. Start at Terminal Failed Stage
   * 2. Traverse to Parent Stages
   * 3. If a Parent failed, current stage is a "Victim" - move upstream
   * 4. If no parents failed (or parents are successful), Current Stage = Root Cause
   * 
   * Refinement: Logical aborts (cancelled/skipped stages) are excluded.
   * 
   * @param dag The execution DAG
   * @return RootCauseResult — always returns a result (NO_FAILURE for healthy apps)
   */
  def analyzeRootCause(dag: DAGBuilder.ExecutionDAG): RootCauseResult = {
    
    println(s"\n>>> Analyzing root cause for: ${dag.appId}")
    
    // Find failed stages
    val failedStages = dag.failedStages
    
    if (failedStages.isEmpty) {
      println("No failed stages found - job completed successfully")
      // Return a default result using the leaf stage (healthy app)
      val leafStageId = dag.leafStageIds.headOption.getOrElse(
        dag.stages.keys.toSeq.sorted.lastOption.getOrElse(0)
      )
      val leafStage = dag.stages.getOrElse(leafStageId,
        DAGBuilder.StageNode(leafStageId, "Unknown", Set.empty))
      return RootCauseResult(
        appId = dag.appId,
        rootCauseStageId = leafStage.stageId,
        rootCauseStageName = leafStage.stageName,
        failureReason = "NO_FAILURE",
        propagationPath = Seq(leafStage.stageId),
        victimStages = Set.empty,
        analysisConfidence = 1.0
      )
    }
    
    println(s"Found ${failedStages.size} failed stage(s)")
    
    // Find terminal failed stage (leaf stage that failed, or most downstream failed stage)
    val terminalFailedStage = findTerminalFailedStage(dag, failedStages)
    
    if (terminalFailedStage.isEmpty) {
      println("Could not identify terminal failed stage")
      // Fallback: use first failed stage
      val fallback = failedStages.head
      return RootCauseResult(
        appId = dag.appId,
        rootCauseStageId = fallback.stageId,
        rootCauseStageName = fallback.stageName,
        failureReason = fallback.failureReason.getOrElse("Unknown"),
        propagationPath = Seq(fallback.stageId),
        victimStages = Set.empty,
        analysisConfidence = 0.3
      )
    }
    
    val terminal = terminalFailedStage.get
    println(s"Terminal failed stage: ${terminal.stageId}")
    
    // Reverse BFS to find root cause
    val (rootCauseStage, path, victims) = reverseBFS(dag, terminal.stageId)
    
    RootCauseResult(
      appId = dag.appId,
      rootCauseStageId = rootCauseStage.stageId,
      rootCauseStageName = rootCauseStage.stageName,
      failureReason = rootCauseStage.failureReason.getOrElse("Unknown"),
      propagationPath = path,
      victimStages = victims,
      analysisConfidence = calculateConfidence(dag, rootCauseStage, path)
    )
  }

  /**
   * Finds the terminal (most downstream) failed stage.
   */
  private def findTerminalFailedStage(
    dag: DAGBuilder.ExecutionDAG,
    failedStages: Seq[DAGBuilder.StageNode]
  ): Option[DAGBuilder.StageNode] = {
    
    // Check leaf stages first
    val failedLeaves = failedStages.filter(s => dag.leafStageIds.contains(s.stageId))
    if (failedLeaves.nonEmpty) {
      return Some(failedLeaves.maxBy(_.stageId))
    }
    
    // Find the stage with no failed children (most downstream failure)
    val terminalFailed = failedStages.find { stage =>
      val children = dag.getChildren(stage.stageId)
      children.forall(!_.isFailed)
    }
    
    terminalFailed.orElse(failedStages.headOption)
  }

  /**
   * Reverse BFS to trace failure back to root cause.
   * 
   * @param dag The execution DAG
   * @param startStageId The terminal failed stage ID
   * @return Tuple of (root cause stage, propagation path, victim stages)
   */
  private def reverseBFS(
    dag: DAGBuilder.ExecutionDAG,
    startStageId: Int
  ): (DAGBuilder.StageNode, Seq[Int], Set[Int]) = {
    
    val visited = mutable.Set[Int]()
    val queue = mutable.Queue[Int](startStageId)
    val path = mutable.ListBuffer[Int](startStageId)
    val victims = mutable.Set[Int]()
    
    var rootCauseStageId = startStageId
    
    while (queue.nonEmpty) {
      val currentId = queue.dequeue()
      
      if (!visited.contains(currentId)) {
        visited.add(currentId)
        
        val currentStage = dag.stages.get(currentId)
        
        if (currentStage.isDefined && currentStage.get.isFailed) {
          // Get parent stages
          val parents = dag.getParents(currentId)
          
          // Filter to failed parents (excluding logical aborts)
          val failedParents = parents.filter { p =>
            p.isFailed && !isLogicalAbort(p)
          }
          
          if (failedParents.isEmpty) {
            // No failed parents → this is the ROOT CAUSE
            rootCauseStageId = currentId
            println(s"  Root cause identified: Stage $currentId (no failed parents)")
          } else {
            // Has failed parents → this stage is a VICTIM
            victims.add(currentId)
            println(s"  Stage $currentId is a victim (has ${failedParents.size} failed parent(s))")
            
            // Continue BFS to parents
            failedParents.foreach { parent =>
              if (!visited.contains(parent.stageId)) {
                queue.enqueue(parent.stageId)
                path.prepend(parent.stageId)
              }
            }
          }
        }
      }
    }
    
    val rootCauseStage = dag.stages.getOrElse(rootCauseStageId, 
      DAGBuilder.StageNode(rootCauseStageId, "Unknown", Set.empty))
    
    (rootCauseStage, path.toSeq, victims.toSet)
  }

  /**
   * Determines if a stage failure is a logical abort (cancelled, not a true failure).
   * Logical aborts should not be considered as root causes.
   */
  private def isLogicalAbort(stage: DAGBuilder.StageNode): Boolean = {
    stage.status == "SKIPPED" || 
    stage.failureReason.exists(reason =>
      reason.toLowerCase.contains("cancelled") ||
      reason.toLowerCase.contains("aborted") ||
      reason.toLowerCase.contains("stage was cancelled") ||
      reason.toLowerCase.contains("job was cancelled")
    )
  }

  /**
   * Calculates confidence score for the root cause analysis.
   * Higher confidence when:
   * - Clear propagation path
   * - Root cause has distinct failure reason
   * - No ambiguous multi-parent scenarios
   */
  private def calculateConfidence(
    dag: DAGBuilder.ExecutionDAG,
    rootCauseStage: DAGBuilder.StageNode,
    path: Seq[Int]
  ): Double = {
    
    var confidence = 0.5  // Base confidence
    
    // Boost if root cause has a clear failure reason
    if (rootCauseStage.failureReason.isDefined && 
        rootCauseStage.failureReason.get.length > 10) {
      confidence += 0.2
    }
    
    // Boost if propagation path is clear (single parent chain)
    val pathStages = path.flatMap(id => dag.stages.get(id))
    if (pathStages.forall(_.parentIds.size <= 1)) {
      confidence += 0.15
    }
    
    // Boost if all parent stages of root cause are successful
    val rootParents = dag.getParents(rootCauseStage.stageId)
    if (rootParents.forall(_.isCompleted)) {
      confidence += 0.15
    }
    
    // Penalize if multiple root cause candidates (ambiguous)
    val failedStages = dag.failedStages
    val potentialRoots = failedStages.count { stage =>
      dag.getParents(stage.stageId).forall(!_.isFailed)
    }
    if (potentialRoots > 1) {
      confidence -= 0.1 * (potentialRoots - 1)
    }
    
    math.max(0.0, math.min(1.0, confidence))
  }

  // ============================================================================
  // Batch Analysis
  // ============================================================================

  /**
   * Analyzes root causes for all applications in the DAG collection.
   */
  def analyzeAllApplications(dags: Map[String, DAGBuilder.ExecutionDAG]): Seq[RootCauseResult] = {
    println(s"\n>>> Analyzing ${dags.size} applications")
    
    dags.values.map { dag =>
      analyzeRootCause(dag)
    }.toSeq
  }

  /**
   * Converts analysis results to DataFrame.
   */
  def resultsToDataFrame(spark: SparkSession, results: Seq[RootCauseResult]): DataFrame = {
    import spark.implicits._
    
    results.map { r =>
      (
        r.appId,
        r.rootCauseStageId,
        r.rootCauseStageName,
        r.failureReason,
        r.propagationPath.mkString(","),
        r.victimStages.mkString(","),
        r.analysisConfidence
      )
    }.toDF(
      "app_id",
      "root_cause_stage_id",
      "root_cause_stage_name",
      "failure_reason",
      "propagation_path",
      "victim_stages",
      "analysis_confidence"
    )
  }
}
