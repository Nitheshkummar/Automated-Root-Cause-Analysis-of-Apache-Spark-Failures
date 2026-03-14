package com.sparkrca.datalake

import org.apache.spark.sql.{SparkSession, DataFrame, SaveMode}
import org.apache.spark.sql.types._
import com.sparkrca.config.SparkConfig

/**
 * TPC-H Data Converter: Raw Text → Parquet.
 * Converts TPC-H benchmark data to columnar format for high-performance Deep DAG queries.
 */
object TPCHParquetConverter {

  // ============================================================================
  // TPC-H Table Schemas
  // ============================================================================
  
  val customerSchema = StructType(Seq(
    StructField("c_custkey", LongType, nullable = false),
    StructField("c_name", StringType),
    StructField("c_address", StringType),
    StructField("c_nationkey", LongType),
    StructField("c_phone", StringType),
    StructField("c_acctbal", DoubleType),
    StructField("c_mktsegment", StringType),
    StructField("c_comment", StringType)
  ))

  val lineitemSchema = StructType(Seq(
    StructField("l_orderkey", LongType, nullable = false),
    StructField("l_partkey", LongType),
    StructField("l_suppkey", LongType),
    StructField("l_linenumber", IntegerType),
    StructField("l_quantity", DoubleType),
    StructField("l_extendedprice", DoubleType),
    StructField("l_discount", DoubleType),
    StructField("l_tax", DoubleType),
    StructField("l_returnflag", StringType),
    StructField("l_linestatus", StringType),
    StructField("l_shipdate", StringType),
    StructField("l_commitdate", StringType),
    StructField("l_receiptdate", StringType),
    StructField("l_shipinstruct", StringType),
    StructField("l_shipmode", StringType),
    StructField("l_comment", StringType)
  ))

  val nationSchema = StructType(Seq(
    StructField("n_nationkey", LongType, nullable = false),
    StructField("n_name", StringType),
    StructField("n_regionkey", LongType),
    StructField("n_comment", StringType)
  ))

  val ordersSchema = StructType(Seq(
    StructField("o_orderkey", LongType, nullable = false),
    StructField("o_custkey", LongType),
    StructField("o_orderstatus", StringType),
    StructField("o_totalprice", DoubleType),
    StructField("o_orderdate", StringType),
    StructField("o_orderpriority", StringType),
    StructField("o_clerk", StringType),
    StructField("o_shippriority", IntegerType),
    StructField("o_comment", StringType)
  ))

  val partSchema = StructType(Seq(
    StructField("p_partkey", LongType, nullable = false),
    StructField("p_name", StringType),
    StructField("p_mfgr", StringType),
    StructField("p_brand", StringType),
    StructField("p_type", StringType),
    StructField("p_size", IntegerType),
    StructField("p_container", StringType),
    StructField("p_retailprice", DoubleType),
    StructField("p_comment", StringType)
  ))

  val partsuppSchema = StructType(Seq(
    StructField("ps_partkey", LongType, nullable = false),
    StructField("ps_suppkey", LongType, nullable = false),
    StructField("ps_availqty", IntegerType),
    StructField("ps_supplycost", DoubleType),
    StructField("ps_comment", StringType)
  ))

  val regionSchema = StructType(Seq(
    StructField("r_regionkey", LongType, nullable = false),
    StructField("r_name", StringType),
    StructField("r_comment", StringType)
  ))

  val supplierSchema = StructType(Seq(
    StructField("s_suppkey", LongType, nullable = false),
    StructField("s_name", StringType),
    StructField("s_address", StringType),
    StructField("s_nationkey", LongType),
    StructField("s_phone", StringType),
    StructField("s_acctbal", DoubleType),
    StructField("s_comment", StringType)
  ))

  // Table name to schema mapping
  val tableSchemas: Map[String, StructType] = Map(
    "customer" -> customerSchema,
    "lineitem" -> lineitemSchema,
    "nation" -> nationSchema,
    "orders" -> ordersSchema,
    "part" -> partSchema,
    "partsupp" -> partsuppSchema,
    "region" -> regionSchema,
    "supplier" -> supplierSchema
  )

  // ============================================================================
  // Conversion Functions
  // ============================================================================

  /**
   * Reads a raw TPC-H text file.
   * 
   * @param spark SparkSession
   * @param tableName Name of the TPC-H table
   * @param rawPath Path to raw data directory
   * @return DataFrame with proper schema
   */
  def readRawTable(spark: SparkSession, tableName: String, rawPath: String): DataFrame = {
    val schema = tableSchemas.getOrElse(
      tableName,
      throw new IllegalArgumentException(s"Unknown table: $tableName")
    )
    
    val filePath = s"$rawPath/$tableName.tbl"
    
    println(s"Reading raw table: $filePath")
    
    spark.read
      .option("header", "false")
      .option("delimiter", "|")
      .option("inferSchema", "false")
      .schema(schema)
      .csv(filePath)
  }

  /**
   * Converts a single table from raw text to Parquet.
   * 
   * @param spark SparkSession
   * @param tableName Name of the TPC-H table
   * @param rawPath Path to raw data directory
   * @param parquetPath Path to output Parquet directory
   * @param numPartitions Number of output partitions (for parallelism)
   */
  def convertTable(
    spark: SparkSession,
    tableName: String,
    rawPath: String,
    parquetPath: String,
    numPartitions: Int = 200
  ): Unit = {
    println(s"\n${"=" * 60}")
    println(s"Converting table: $tableName")
    println(s"${"=" * 60}")
    
    val startTime = System.currentTimeMillis()
    
    // Read raw data
    val df = readRawTable(spark, tableName, rawPath)
    
    // Show sample
    println(s"Sample data from $tableName:")
    df.show(5, truncate = false)
    
    // Repartition for optimal Parquet file sizes
    val repartitionedDf = if (tableName == "lineitem" || tableName == "orders") {
      // Large tables: more partitions
      df.repartition(numPartitions)
    } else {
      // Small tables: fewer partitions
      df.coalesce(Math.min(numPartitions, 10))
    }
    
    // Write to Parquet
    val outputPath = s"$parquetPath/$tableName"
    repartitionedDf.write
      .mode(SaveMode.Overwrite)
      .option("compression", "snappy")
      .parquet(outputPath)
    
    val endTime = System.currentTimeMillis()
    val rowCount = df.count()
    
    println(s"✓ Converted $tableName: $rowCount rows in ${(endTime - startTime) / 1000.0}s")
    println(s"  Output: $outputPath")
  }

  /**
   * Converts all TPC-H tables from raw text to Parquet.
   * 
   * @param spark SparkSession
   * @param rawPath Path to raw data directory
   * @param parquetPath Path to output Parquet directory
   */
  def convertAllTables(
    spark: SparkSession,
    rawPath: String = SparkConfig.Paths.TPCH_RAW,
    parquetPath: String = SparkConfig.Paths.TPCH_PARQUET
  ): Unit = {
    println("\n" + "=" * 70)
    println("TPC-H RAW TEXT → PARQUET CONVERSION")
    println("=" * 70)
    println(s"Source: $rawPath")
    println(s"Target: $parquetPath")
    println("=" * 70)
    
    val overallStart = System.currentTimeMillis()
    
    // Ensure output directory exists
    HDFSUtils.ensureDirectory(spark, parquetPath)
    
    // Convert each table
    tableSchemas.keys.foreach { tableName =>
      try {
        convertTable(spark, tableName, rawPath, parquetPath)
      } catch {
        case e: Exception =>
          println(s"✗ Failed to convert $tableName: ${e.getMessage}")
          e.printStackTrace()
      }
    }
    
    val overallEnd = System.currentTimeMillis()
    
    println("\n" + "=" * 70)
    println(s"CONVERSION COMPLETE in ${(overallEnd - overallStart) / 1000.0}s")
    println("=" * 70)
    
    // Print summary
    printTableSummary(spark, parquetPath)
  }

  /**
   * Prints summary of all converted tables.
   */
  def printTableSummary(spark: SparkSession, parquetPath: String): Unit = {
    println("\nTable Summary:")
    println("-" * 50)
    tableSchemas.keys.toSeq.sorted.foreach { tableName =>
      val path = s"$parquetPath/$tableName"
      if (HDFSUtils.pathExists(spark, path)) {
        val size = HDFSUtils.getSizeFormatted(spark, path)
        val df = spark.read.parquet(path)
        val count = df.count()
        println(f"  $tableName%-15s: $count%,12d rows ($size)")
      } else {
        println(f"  $tableName%-15s: NOT FOUND")
      }
    }
    println("-" * 50)
  }

  /**
   * Registers all Parquet tables as temporary views for SQL queries.
   */
  def registerTables(spark: SparkSession, parquetPath: String = SparkConfig.Paths.TPCH_PARQUET): Unit = {
    println("\nRegistering TPC-H tables as temporary views...")
    
    tableSchemas.keys.foreach { tableName =>
      val path = s"$parquetPath/$tableName"
      if (HDFSUtils.pathExists(spark, path)) {
        spark.read.parquet(path).createOrReplaceTempView(tableName)
        println(s"  ✓ Registered: $tableName")
      } else {
        println(s"  ✗ Skipped (not found): $tableName")
      }
    }
    
    println("Table registration complete.")
  }
}
