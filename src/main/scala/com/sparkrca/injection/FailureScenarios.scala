package com.sparkrca.injection

import com.sparkrca.config.SparkConfig.Labels

/**
 * Failure Scenario Definitions for the RCA Training Pipeline.
 * Provides sealed trait hierarchy for type-safe failure handling.
 */
sealed trait FailureScenario {
  def label: Int
  def name: String
  def description: String
  def tpchQuery: String
  def injectionMethod: String
}

object FailureScenarios {

  /**
   * Baseline - Normal execution with no failures.
   * Context: TPC-H Q21 (Suppliers Who Kept Orders Waiting)
   */
  case object Baseline extends FailureScenario {
    val label = Labels.BASELINE
    val name = "BASELINE"
    val description = "Normal execution - no injected failures"
    val tpchQuery = "Q21"
    val injectionMethod = "None"
  }

  /**
   * RC1: OUT_OF_MEMORY (OOM)
   * Context: TPC-H Q9 (Product Type Profit Measure)
   * Injection: Forced BROADCAST hint on lineitem table
   */
  case object OutOfMemory extends FailureScenario {
    val label = Labels.OUT_OF_MEMORY
    val name = "OUT_OF_MEMORY"
    val description = "Container killed by YARN for exceeding memory limits"
    val tpchQuery = "Q9"
    val injectionMethod = "Forced BROADCAST hint on lineitem (largest table)"
  }

  /**
   * RC2: DATA_SKEW
   * Context: TPC-H Q18 (Large Volume Customer)
   * Injection: Salt a specific join key to be 99% of the dataset
   */
  case object DataSkew extends FailureScenario {
    val label = Labels.DATA_SKEW
    val name = "DATA_SKEW"
    val description = "Executor becomes a straggler due to skewed data distribution"
    val tpchQuery = "Q18"
    val injectionMethod = "Salt join key to create 99% skew"
  }

  /**
   * RC3: SERIALIZATION_FAILURE
   * Context: TPC-H Q2 (Minimum Cost Supplier)
   * Injection: UDF containing non-serializable object (Socket)
   */
  case object SerializationFailure extends FailureScenario {
    val label = Labels.SERIALIZATION
    val name = "SERIALIZATION_FAILURE"
    val description = "Task not serializable exception during map stage"
    val tpchQuery = "Q2"
    val injectionMethod = "UDF with non-serializable Socket in closure"
  }

  /**
   * RC4: METADATA_FAILURE
   * Context: Generic Iterative Join
   * Injection: HDFS path dropped/renamed before execution
   */
  case object MetadataFailure extends FailureScenario {
    val label = Labels.METADATA_FAILURE
    val name = "METADATA_FAILURE"
    val description = "FileNotFoundException or MetadataFetchFailed"
    val tpchQuery = "Iterative Join"
    val injectionMethod = "External thread drops/renames HDFS path before execution"
  }

  /**
   * RC5: DISK_SPACE_FAILURE
   * Context: Cartesian Product (crossJoin)
   * Injection: Massive shuffle spill exhausts local disk
   */
  case object DiskSpaceFailure extends FailureScenario {
    val label = Labels.DISK_SPACE
    val name = "DISK_SPACE_FAILURE"
    val description = "No space left on device during shuffle spill"
    val tpchQuery = "Cartesian Product"
    val injectionMethod = "crossJoin causing massive shuffle spill to /tmp/hadoop-yarn"
  }

  /**
   * RC6: NETWORK_TIMEOUT
   * Context: TPC-H Q1 (Pricing Summary)
   * Injection: UDF with prolonged execution (sleep > heartbeat timeout)
   */
  case object NetworkTimeout extends FailureScenario {
    val label = Labels.NETWORK_TIMEOUT
    val name = "NETWORK_TIMEOUT"
    val description = "ExecutorLost due to missed heartbeats"
    val tpchQuery = "Q1"
    val injectionMethod = "UDF with sleep exceeding heartbeat timeout"
  }

  // All scenarios list
  val allScenarios: Seq[FailureScenario] = Seq(
    Baseline,
    OutOfMemory,
    DataSkew,
    SerializationFailure,
    MetadataFailure,
    DiskSpaceFailure,
    NetworkTimeout
  )

  // Lookup by label
  def fromLabel(label: Int): Option[FailureScenario] = {
    allScenarios.find(_.label == label)
  }

  // Lookup by name
  def fromName(name: String): Option[FailureScenario] = {
    allScenarios.find(_.name.equalsIgnoreCase(name))
  }

  /**
   * Prints a summary table of all failure scenarios.
   */
  def printScenarioSummary(): Unit = {
    println("\n" + "=" * 80)
    println("FAILURE SCENARIOS SUMMARY")
    println("=" * 80)
    println(f"${"Label"}%-6s | ${"Name"}%-25s | ${"Query"}%-15s | ${"Injection"}%s")
    println("-" * 80)
    allScenarios.foreach { s =>
      println(f"${s.label}%-6d | ${s.name}%-25s | ${s.tpchQuery}%-15s | ${s.injectionMethod.take(30)}%s")
    }
    println("=" * 80)
  }

  /**
   * Runs a specified failure scenario and returns (label, name, result).
   * This bridges the Main.scala injection loop with TPCHFailureSuite execution methods.
   */
  def runScenario(spark: org.apache.spark.sql.SparkSession, scenario: FailureScenario): (Int, String, scala.util.Try[Long]) = {
    val runNum = 1  // default variation for ad-hoc testing
    val result = scenario match {
      case Baseline => TPCHFailureSuite.runBaseline(spark, runNum)
      case OutOfMemory => TPCHFailureSuite.runOOMFailure(spark, runNum)
      case DataSkew => TPCHFailureSuite.runSkewFailure(spark, runNum)
      case SerializationFailure => TPCHFailureSuite.runSerializationFailure(spark, runNum)
      case MetadataFailure => TPCHFailureSuite.runMetadataFailure(spark, runNum)
      case DiskSpaceFailure => TPCHFailureSuite.runDiskFailure(spark, runNum)
      case NetworkTimeout => TPCHFailureSuite.runNetworkTimeout(spark, runNum)
    }
    (scenario.label, scenario.name, result)
  }
}
