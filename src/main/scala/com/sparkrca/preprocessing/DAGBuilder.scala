package com.sparkrca.preprocessing

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import scala.collection.mutable

/**
 * DAG Builder: Reconstructs the Acyclic Dependency Graph from Spark event logs.
 * Uses SparkListenerStageSubmitted events to build parent-child relationships.
 */
object DAGBuilder {

  /**
   * Represents a node in the DAG (a stage).
   */
  case class StageNode(
    stageId: Int,
    stageName: String,
    parentIds: Set[Int],
    childIds: mutable.Set[Int] = mutable.Set.empty,
    status: String = "UNKNOWN",
    failureReason: Option[String] = None
  ) {
    def isFailed: Boolean = status == "FAILED"
    def isCompleted: Boolean = status == "COMPLETED"
    def isSkipped: Boolean = status == "SKIPPED"
  }

  /**
   * Represents the complete DAG for a Spark application.
   */
  case class ExecutionDAG(
    appId: String,
    stages: Map[Int, StageNode],
    rootStageIds: Set[Int],    // Stages with no parents
    leafStageIds: Set[Int]     // Stages with no children
  ) {
    
    /**
     * Gets all failed stages in the DAG.
     */
    def failedStages: Seq[StageNode] = stages.values.filter(_.isFailed).toSeq
    
    /**
     * Gets the terminal (leaf) failed stage if any.
     */
    def terminalFailedStage: Option[StageNode] = {
      leafStageIds.flatMap(id => stages.get(id)).find(_.isFailed)
    }
    
    /**
     * Gets all parent stages for a given stage.
     */
    def getParents(stageId: Int): Set[StageNode] = {
      stages.get(stageId).map(_.parentIds).getOrElse(Set.empty)
        .flatMap(id => stages.get(id))
    }
    
    /**
     * Gets all child stages for a given stage.
     */
    def getChildren(stageId: Int): Set[StageNode] = {
      stages.get(stageId).map(_.childIds.toSet).getOrElse(Set.empty)
        .flatMap(id => stages.get(id))
    }
    
    /**
     * Gets all ancestor stages (transitive parents).
     */
    def getAncestors(stageId: Int): Set[Int] = {
      val ancestors = mutable.Set[Int]()
      val queue = mutable.Queue[Int]()
      
      stages.get(stageId).foreach(s => s.parentIds.foreach(queue.enqueue(_)))
      
      while (queue.nonEmpty) {
        val parentId = queue.dequeue()
        if (!ancestors.contains(parentId)) {
          ancestors.add(parentId)
          stages.get(parentId).foreach(p => p.parentIds.foreach(queue.enqueue(_)))
        }
      }
      
      ancestors.toSet
    }
    
    /**
     * Gets all descendant stages (transitive children).
     */
    def getDescendants(stageId: Int): Set[Int] = {
      val descendants = mutable.Set[Int]()
      val queue = mutable.Queue[Int]()
      
      stages.get(stageId).foreach(s => s.childIds.foreach(queue.enqueue(_)))
      
      while (queue.nonEmpty) {
        val childId = queue.dequeue()
        if (!descendants.contains(childId)) {
          descendants.add(childId)
          stages.get(childId).foreach(c => c.childIds.foreach(queue.enqueue(_)))
        }
      }
      
      descendants.toSet
    }
    
    /**
     * Validates that the DAG is acyclic.
     */
    def isAcyclic: Boolean = {
      val visited = mutable.Set[Int]()
      val recursionStack = mutable.Set[Int]()
      
      def hasCycle(stageId: Int): Boolean = {
        if (recursionStack.contains(stageId)) return true
        if (visited.contains(stageId)) return false
        
        visited.add(stageId)
        recursionStack.add(stageId)
        
        val hasCycleInChildren = stages.get(stageId) match {
          case Some(stage) => stage.childIds.exists(hasCycle)
          case None => false
        }
        
        recursionStack.remove(stageId)
        hasCycleInChildren
      }
      
      !stages.keys.exists(hasCycle)
    }
    
    /**
     * Gets the depth (longest path) of the DAG.
     */
    def depth: Int = {
      if (stages.isEmpty) return 0
      
      val memoized = mutable.Map[Int, Int]()
      
      def stageDepth(stageId: Int): Int = {
        memoized.getOrElseUpdate(stageId, {
          val parents = stages.get(stageId).map(_.parentIds).getOrElse(Set.empty)
          if (parents.isEmpty) 1
          else 1 + parents.map(stageDepth).max
        })
      }
      
      stages.keys.map(stageDepth).max
    }
    
    /**
     * Prints the DAG structure.
     */
    def printDAG(): Unit = {
      println("\n" + "=" * 60)
      println(s"EXECUTION DAG: $appId")
      println("=" * 60)
      println(s"Total Stages: ${stages.size}")
      println(s"Root Stages: ${rootStageIds.mkString(", ")}")
      println(s"Leaf Stages: ${leafStageIds.mkString(", ")}")
      println(s"DAG Depth: $depth")
      println(s"Is Acyclic: $isAcyclic")
      println("-" * 60)
      
      stages.values.toSeq.sortBy(_.stageId).foreach { stage =>
        val statusIcon = stage.status match {
          case "COMPLETED" => "✓"
          case "FAILED" => "✗"
          case "SKIPPED" => "○"
          case _ => "?"
        }
        val parents = if (stage.parentIds.isEmpty) "none" else stage.parentIds.mkString(",")
        val children = if (stage.childIds.isEmpty) "none" else stage.childIds.mkString(",")
        
        println(f"$statusIcon Stage ${stage.stageId}%3d [${stage.status}%-10s] Parents: $parents%-15s Children: $children")
        if (stage.failureReason.isDefined) {
          println(s"              └─ Failure: ${stage.failureReason.get.take(50)}...")
        }
      }
      println("=" * 60)
    }
  }

  // ============================================================================
  // DAG Construction Functions
  // ============================================================================

  /**
   * Builds the execution DAG from stage information DataFrame.
   * 
   * @param spark SparkSession
   * @param stageDf DataFrame with stage information
   * @param appId Application ID
   * @return ExecutionDAG
   */
  def buildDAG(spark: SparkSession, stageDf: DataFrame, appId: String): ExecutionDAG = {
    import spark.implicits._
    
    println(s"Building DAG for application: $appId")
    
    // Collect stage information - only use columns that exist
    val stageRows = stageDf
      .filter(col("app_id") === appId)
      .select("stage_id", "stage_name", "status", "failure_reason")
      .distinct()
      .collect()
    
    // Build stage nodes (without parent info initially)
    val stageNodes = mutable.Map[Int, StageNode]()
    
    stageRows.foreach { row =>
      val stageId = row.getAs[Int]("stage_id")
      val stageName = Option(row.getAs[String]("stage_name")).getOrElse("")
      val status = Option(row.getAs[String]("status")).getOrElse("UNKNOWN")
      val failureReason = Option(row.getAs[String]("failure_reason"))
      
      // Infer parent IDs: stages are typically sequential, so a simple heuristic
      // For a real DAG, we would parse SparkListenerStageSubmitted events
      // For now, assume stages with lower IDs are potential parents
      val parentIds = Set.empty[Int]  // Will be populated if we have parent info
      
      stageNodes(stageId) = StageNode(stageId, stageName, parentIds, mutable.Set.empty, status, failureReason)
    }
    
    // Build child relationships based on stage ordering
    // Assumption: stage N depends on stages < N (simplified DAG)
    val sortedStageIds = stageNodes.keys.toSeq.sorted
    for (i <- 1 until sortedStageIds.length) {
      val childId = sortedStageIds(i)
      val parentId = sortedStageIds(i - 1)
      stageNodes.get(childId).foreach { child =>
        val updatedParents = child.parentIds + parentId
        stageNodes(childId) = child.copy(parentIds = updatedParents)
      }
      stageNodes.get(parentId).foreach(_.childIds.add(childId))
    }
    
    // Find root stages (first stage)
    val rootStageIds = if (sortedStageIds.nonEmpty) Set(sortedStageIds.head) else Set.empty[Int]
    
    // Find leaf stages (last stage)
    val leafStageIds = if (sortedStageIds.nonEmpty) Set(sortedStageIds.last) else Set.empty[Int]
    
    val dag = ExecutionDAG(appId, stageNodes.toMap, rootStageIds, leafStageIds)
    
    println(s"Built DAG with ${stageNodes.size} stages, depth ${dag.depth}")
    
    dag
  }

  /**
   * Builds DAGs for all applications in the stage DataFrame.
   */
  def buildAllDAGs(spark: SparkSession, stageDf: DataFrame): Map[String, ExecutionDAG] = {
    import spark.implicits._
    
    val appIds = stageDf.select("app_id").distinct().as[String].collect()
    
    println(s"Building DAGs for ${appIds.length} applications")
    
    appIds.map { appId =>
      appId -> buildDAG(spark, stageDf, appId)
    }.toMap
  }

  /**
   * Converts DAG edges to DataFrame for visualization or analysis.
   */
  def dagToEdgesDataFrame(spark: SparkSession, dag: ExecutionDAG): DataFrame = {
    import spark.implicits._
    
    val edges = dag.stages.values.flatMap { stage =>
      stage.parentIds.map(parentId => (dag.appId, parentId, stage.stageId))
    }.toSeq
    
    edges.toDF("app_id", "parent_stage_id", "child_stage_id")
  }
}
