package com.sparkrca.injection

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.util.{Try, Success, Failure}

/**
 * TPC-H Failure Injection Suite with INTRA-CLASS VARIABILITY
 * All queries use DataFrame API with limits to finish within 5 minutes.
 */
object TPCHFailureSuite {

  val TPCH_PATH = "hdfs:///project/parquet"
  val TMP_META_PATH = "hdfs:///tmp/meta_test"
  
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Usage: TPCHFailureSuite <failureType> <runId>")
      println("Failure Types: BASELINE, OOM, SKEW, SER, META, DISK, NET")
      System.exit(1)
    }
    
    val failureType = args(0).toUpperCase
    val runId = args(1)
    val runNum = runId.filter(_.isDigit).toInt
    val appName = s"RCA_${failureType}_Run_$runId"
    
    println(s"\n${"=" * 70}")
    println(s"FAILURE INJECTION: $failureType (Run: $runId, Variation: $runNum)")
    println(s"Application Name: $appName")
    println(s"${"=" * 70}\n")
    
    val spark = SparkSession.builder()
      .appName(appName)
      .config("spark.eventLog.enabled", "true")
      .config("spark.eventLog.compress", "true")
      .config("spark.task.maxFailures", "4")
      .enableHiveSupport()
      .getOrCreate()
    
    try {
      val result = failureType match {
        case "BASELINE" => runBaseline(spark, runNum)
        case "OOM"      => runOOMFailure(spark, runNum)
        case "SKEW"     => runSkewFailure(spark, runNum)
        case "SER"      => runSerializationFailure(spark, runNum)
        case "META"     => runMetadataFailure(spark, runNum)
        case "DISK"     => runDiskFailure(spark, runNum)
        case "NET"      => runNetworkTimeout(spark, runNum)
        case _ =>
          println(s"ERROR: Unknown failure type: $failureType")
          Failure(new IllegalArgumentException(s"Unknown failure type: $failureType"))
      }
      
      result match {
        case Success(count) =>
          println(s"\n✓ SUCCESS: $failureType completed with $count rows")
        case Failure(e) =>
          println(s"\n✗ EXPECTED FAILURE: $failureType failed with ${e.getClass.getSimpleName}")
          println(s"  Message: ${e.getMessage}")
          throw e
      }
    } finally {
      spark.stop()
    }
  }

  // ============================================================================
  // BASELINE: Fast DataFrame API queries with variation (no SQL subqueries)
  // ============================================================================
  
  def runBaseline(spark: SparkSession, runNum: Int): Try[Long] = Try {
    import spark.implicits._
    
    val variation = runNum % 4
    println(s">>> BASELINE: Variation $variation (runNum=$runNum)")
    
    val orders = spark.read.parquet(s"$TPCH_PATH/orders")
    val lineitem = spark.read.parquet(s"$TPCH_PATH/lineitem")
    val customer = spark.read.parquet(s"$TPCH_PATH/customer")
    val supplier = spark.read.parquet(s"$TPCH_PATH/supplier")
    val nation = spark.read.parquet(s"$TPCH_PATH/nation")
    val part = spark.read.parquet(s"$TPCH_PATH/part")
    
    val result: org.apache.spark.sql.DataFrame = variation match {
      case 0 =>
        // Q3-style: Shipping priority (3 tables, fast)
        println("  Query: Orders-Lineitem-Customer join")
        customer.filter(col("c_mktsegment") === "BUILDING")
          .join(orders, col("c_custkey") === col("o_custkey"))
          .filter(col("o_orderdate") < "1995-03-15")
          .join(lineitem, col("o_orderkey") === col("l_orderkey"))
          .filter(col("l_shipdate") > "1995-03-15")
          .groupBy("l_orderkey", "o_orderdate", "o_shippriority")
          .agg(sum(col("l_extendedprice") * (lit(1) - col("l_discount"))).as("revenue"))
          .orderBy(col("revenue").desc, col("o_orderdate"))
          .limit(100)
          
      case 1 =>
        // Q4-style: Order priority (2 tables, fast)
        println("  Query: Orders-Lineitem priority check")
        val lateItems = lineitem.filter(col("l_commitdate") < col("l_receiptdate"))
          .select("l_orderkey").distinct()
        orders.filter(col("o_orderdate") >= "1993-07-01")
          .filter(col("o_orderdate") < "1993-10-01")
          .join(lateItems, col("o_orderkey") === col("l_orderkey"))
          .groupBy("o_orderpriority")
          .agg(sum(lit(1)).as("order_count"))
          .orderBy("o_orderpriority")
          
      case 2 =>
        // Q10-style: Customer reporting (4 tables, moderate)
        println("  Query: Customer-Orders-Lineitem-Nation reporting")
        customer
          .join(orders, col("c_custkey") === col("o_custkey"))
          .filter(col("o_orderdate") >= "1993-10-01")
          .filter(col("o_orderdate") < "1994-01-01")
          .join(lineitem, col("o_orderkey") === col("l_orderkey"))
          .filter(col("l_returnflag") === "R")
          .join(nation, col("c_nationkey") === col("n_nationkey"))
          .groupBy("c_custkey", "c_name", "c_acctbal", "n_name")
          .agg(sum(col("l_extendedprice") * (lit(1) - col("l_discount"))).as("revenue"))
          .orderBy(col("revenue").desc)
          .limit(50)
          
      case 3 =>
        // Q12-style: Shipping mode analysis (2 tables, fast)
        println("  Query: Lineitem-Orders shipping mode")
        lineitem
          .filter(col("l_shipmode").isin("MAIL", "SHIP"))
          .filter(col("l_commitdate") < col("l_receiptdate"))
          .filter(col("l_shipdate") < col("l_commitdate"))
          .filter(col("l_receiptdate") >= "1994-01-01")
          .filter(col("l_receiptdate") < "1995-01-01")
          .join(orders, col("l_orderkey") === col("o_orderkey"))
          .groupBy("l_shipmode")
          .agg(
            sum(when(col("o_orderpriority").isin("1-URGENT", "2-HIGH"), 1).otherwise(0)).as("high_line_count"),
            sum(when(!col("o_orderpriority").isin("1-URGENT", "2-HIGH"), 1).otherwise(0)).as("low_line_count")
          )
          .orderBy("l_shipmode")
    }
    
    result.cache()
    val count = result.count()
    result.show(10)
    count
  }

  // ============================================================================
  // OOM: Broadcast join with variable table size
  // ============================================================================
  
  def runOOMFailure(spark: SparkSession, runNum: Int): Try[Long] = Try {
    import spark.implicits._
    
    val variation = runNum % 3
    println(s">>> OOM: Variation $variation (runNum=$runNum)")
    
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    
    val lineitem = spark.read.parquet(s"$TPCH_PATH/lineitem")
    val orders = spark.read.parquet(s"$TPCH_PATH/orders")
    val supplier = spark.read.parquet(s"$TPCH_PATH/supplier")
    val nation = spark.read.parquet(s"$TPCH_PATH/nation")
    val part = spark.read.parquet(s"$TPCH_PATH/part")
    val partsupp = spark.read.parquet(s"$TPCH_PATH/partsupp")
    
    val result = variation match {
      case 0 =>
        println("  Broadcast: lineitem (full)")
        part.filter(col("p_name").contains("green"))
          .join(broadcast(lineitem), col("p_partkey") === col("l_partkey"))
          .join(supplier, col("l_suppkey") === col("s_suppkey"))
          .join(orders, col("l_orderkey") === col("o_orderkey"))
          .join(nation, col("s_nationkey") === col("n_nationkey"))
          .groupBy("n_name").agg(sum("l_extendedprice").as("total"))
          .orderBy(col("total").desc)
      case 1 =>
        println("  Broadcast: orders (full)")
        lineitem
          .join(broadcast(orders), col("l_orderkey") === col("o_orderkey"))
          .join(supplier, col("l_suppkey") === col("s_suppkey"))
          .join(nation, col("s_nationkey") === col("n_nationkey"))
          .groupBy("n_name").agg(sum("l_extendedprice").as("total_price"))
          .orderBy(col("total_price").desc)
      case 2 =>
        println("  Broadcast: lineitem + partsupp")
        val bigJoin = lineitem.join(broadcast(partsupp),
          col("l_partkey") === col("ps_partkey") && col("l_suppkey") === col("ps_suppkey"))
        bigJoin
          .join(orders, col("l_orderkey") === col("o_orderkey"))
          .groupBy("o_orderstatus").agg(count("*").as("cnt"))
          .orderBy(col("cnt").desc)
    }
    
    result.count()
  }

  // ============================================================================
  // SKEW: Variable skew intensity (capped at 3M max for speed)
  // ============================================================================
  
  def runSkewFailure(spark: SparkSession, runNum: Int): Try[Long] = Try {
    import spark.implicits._
    
    val skewRows = 200000 + (runNum % 5) * 200000   // 200K, 400K, 600K, 800K, 1M
    val skewKey = (runNum % 3) + 1                    // key 1, 2, or 3
    val qtyValue = 50.0 + (runNum % 4) * 25.0         // 50, 75, 100, 125
    
    println(s">>> SKEW: ${skewRows/1000}K rows, key=$skewKey, qty=$qtyValue (runNum=$runNum)")
    
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    
    val customer = spark.read.parquet(s"$TPCH_PATH/customer")
    val orders = spark.read.parquet(s"$TPCH_PATH/orders")
    val lineitem = spark.read.parquet(s"$TPCH_PATH/lineitem")
    
    val skewData = spark.range(0, skewRows).toDF("id")
      .withColumn("l_orderkey", lit(skewKey.toLong))
      .withColumn("l_partkey", lit(1L))
      .withColumn("l_suppkey", lit(1L))
      .withColumn("l_linenumber", lit(1))
      .withColumn("l_quantity", lit(qtyValue))
      .withColumn("l_extendedprice", lit(1000.0 + runNum * 100.0))
      .withColumn("l_discount", lit(0.05 + (runNum % 5) * 0.02))
      .withColumn("l_tax", lit(0.08))
      .withColumn("l_returnflag", lit("N"))
      .withColumn("l_linestatus", lit("O"))
      .withColumn("l_shipdate", lit("2024-01-01"))
      .withColumn("l_commitdate", lit("2024-01-15"))
      .withColumn("l_receiptdate", lit("2024-01-20"))
      .withColumn("l_shipinstruct", lit("DELIVER IN PERSON"))
      .withColumn("l_shipmode", lit("TRUCK"))
      .withColumn("l_comment", lit("skew injection"))
      .drop("id")
    
    val skewedLineitem = lineitem.union(skewData.select(lineitem.columns.map(col): _*))
    
    val thresholdQty = 200 + (runNum % 3) * 50
    val largeOrders = skewedLineitem
      .groupBy("l_orderkey")
      .agg(sum("l_quantity").as("total_qty"))
      .filter(col("total_qty") > thresholdQty)
    
    val result = customer
      .join(orders, col("c_custkey") === col("o_custkey"))
      .join(largeOrders, col("o_orderkey") === col("l_orderkey"))
      .join(skewedLineitem, col("o_orderkey") === skewedLineitem("l_orderkey"))
      .groupBy("c_name", "c_custkey", "o_orderkey", "o_orderdate", "o_totalprice")
      .agg(sum(skewedLineitem("l_quantity")).as("sum_qty"))
      .orderBy(col("o_totalprice").desc, col("o_orderdate"))
      .limit(100)
    
    result.count()
  }

  // ============================================================================
  // SER: Non-Serializable UDF with variable objects and queries
  // ============================================================================
  
  def runSerializationFailure(spark: SparkSession, runNum: Int): Try[Long] = Try {
    import spark.implicits._
    
    val variation = runNum % 3
    println(s">>> SER: Variation $variation (runNum=$runNum)")
    
    // =========================================================================
    // PHASE 1: Heavy compute to generate meaningful metrics BEFORE crash
    // =========================================================================
    println("  Phase 1: Running heavy compute (shuffles, GC pressure)...")
    
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    
    val lineitem = spark.read.parquet(s"$TPCH_PATH/lineitem")
    val orders = spark.read.parquet(s"$TPCH_PATH/orders")
    val customer = spark.read.parquet(s"$TPCH_PATH/customer")
    val supplier = spark.read.parquet(s"$TPCH_PATH/supplier")
    val nation = spark.read.parquet(s"$TPCH_PATH/nation")
    
    // Vary the compute intensity
    val heavyResult = variation match {
      case 0 =>
        // Heavy groupBy with large shuffle
        println("  Heavy compute: lineitem-orders join + groupBy")
        lineitem
          .join(orders, col("l_orderkey") === col("o_orderkey"))
          .join(customer, col("o_custkey") === col("c_custkey"))
          .groupBy("c_mktsegment", "o_orderpriority")
          .agg(
            sum("l_extendedprice").as("total_price"),
            sum("l_quantity").as("total_qty"),
            avg("l_discount").as("avg_disc"),
            sum("l_tax").as("total_tax")
          )
          .repartition(50 + runNum * 10)  // Force shuffle with variable partitions
          .orderBy(col("total_price").desc)
          .limit(200)
      case 1 =>
        // Multi-join with sort
        println("  Heavy compute: 4-table join + sort")
        lineitem
          .join(orders, col("l_orderkey") === col("o_orderkey"))
          .join(supplier, col("l_suppkey") === col("s_suppkey"))
          .join(nation, col("s_nationkey") === col("n_nationkey"))
          .groupBy("n_name", "o_orderstatus")
          .agg(
            sum(col("l_extendedprice") * (lit(1) - col("l_discount"))).as("revenue"),
            count("*").as("cnt")
          )
          .repartition(80 + runNum * 5)
          .orderBy(col("revenue").desc)
          .limit(100)
      case 2 =>
        // Aggregate with cross-table join
        println("  Heavy compute: customer-orders aggregation")
        customer
          .join(orders, col("c_custkey") === col("o_custkey"))
          .join(lineitem, col("o_orderkey") === col("l_orderkey"))
          .groupBy("c_custkey", "c_name", "c_mktsegment")
          .agg(
            sum("l_extendedprice").as("total_spent"),
            count("*").as("line_count"),
            sum("l_quantity").as("total_qty")
          )
          .repartition(40 + runNum * 15)
          .orderBy(col("total_spent").desc)
          .limit(150)
    }
    
    // Force Phase 1 computation
    val phase1Count = heavyResult.count()
    println(s"  Phase 1 complete: $phase1Count rows")
    
    // =========================================================================
    // PHASE 2: Non-serializable UDF crash
    // =========================================================================
    println("  Phase 2: Applying non-serializable UDF...")
    
    val badUdf: UserDefinedFunction = variation match {
      case 0 =>
        println("  Non-serializable: java.net.Socket")
        val socket = new java.net.Socket()
        udf((name: String) => { if (socket != null) name.toUpperCase else name })
      case 1 =>
        println("  Non-serializable: java.io.FileInputStream")
        val fis = new java.io.FileInputStream("/dev/null")
        udf((name: String) => { if (fis != null) name.toLowerCase else name })
      case 2 =>
        println("  Non-serializable: java.net.ServerSocket")
        val ss = new java.net.ServerSocket(0)
        val port = ss.getLocalPort
        udf((name: String) => { if (ss != null) s"$name:$port" else name })
    }
    
    // Apply UDF to fresh data to trigger serialization failure
    val part = spark.read.parquet(s"$TPCH_PATH/part")
    val partsupp = spark.read.parquet(s"$TPCH_PATH/partsupp")
    val region = spark.read.parquet(s"$TPCH_PATH/region")
    
    val result = if (runNum % 2 == 0) {
      part.filter(col("p_size") === 15)
        .filter(col("p_type").endsWith("BRASS"))
        .join(partsupp, col("p_partkey") === col("ps_partkey"))
        .join(supplier, col("ps_suppkey") === col("s_suppkey"))
        .join(nation, col("s_nationkey") === col("n_nationkey"))
        .join(region, col("n_regionkey") === col("r_regionkey"))
        .filter(col("r_name") === "EUROPE")
        .select("s_acctbal", "s_name", "n_name", "p_partkey", "p_mfgr")
        .orderBy(col("s_acctbal").desc)
        .limit(100)
        .withColumn("s_name_upper", badUdf(col("s_name")))
    } else {
      customer
        .join(orders, col("c_custkey") === col("o_custkey"))
        .filter(col("o_orderdate") >= "1993-10-01")
        .filter(col("o_orderdate") < "1994-01-01")
        .join(lineitem, col("o_orderkey") === col("l_orderkey"))
        .filter(col("l_returnflag") === "R")
        .join(nation, col("c_nationkey") === col("n_nationkey"))
        .groupBy("c_custkey", "c_name")
        .agg(sum(col("l_extendedprice") * (lit(1) - col("l_discount"))).as("revenue"))
        .orderBy(col("revenue").desc)
        .limit(20)
        .withColumn("c_name_mod", badUdf(col("c_name")))
    }
    
    result.count()
  }

  // ============================================================================
  // META: Metadata failure with variable file sizes
  // ============================================================================
  
  def runMetadataFailure(spark: SparkSession, runNum: Int): Try[Long] = Try {
    import spark.implicits._
    
    val tempRows = 20000 + (runNum % 5) * 20000   // 20K, 40K, 60K, 80K, 100K
    val numCategories = 5 + (runNum % 6)
    
    println(s">>> META: ${tempRows/1000}K rows, $numCategories cats (runNum=$runNum)")
    
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)
    val metaPath = new Path(TMP_META_PATH)
    
    // Phase 1: Run a fast TPC-H join
    println("  Phase 1: Running TPC-H join...")
    val orders = spark.read.parquet(s"$TPCH_PATH/orders")
    val lineitem = spark.read.parquet(s"$TPCH_PATH/lineitem")
    val customer = spark.read.parquet(s"$TPCH_PATH/customer")
    
    val phase1Result = if (runNum % 2 == 0) {
      orders
        .join(lineitem, col("o_orderkey") === col("l_orderkey"))
        .join(customer, col("o_custkey") === col("c_custkey"))
        .groupBy("c_name", "c_custkey")
        .agg(sum("l_extendedprice").as("total_price"), count("*").as("order_count"))
        .orderBy(col("total_price").desc)
        .limit(100)
    } else {
      val supplier = spark.read.parquet(s"$TPCH_PATH/supplier")
      orders
        .join(lineitem, col("o_orderkey") === col("l_orderkey"))
        .join(customer, col("o_custkey") === col("c_custkey"))
        .join(supplier, col("l_suppkey") === col("s_suppkey"))
        .groupBy("c_name", "s_name")
        .agg(sum("l_extendedprice").as("total_price"))
        .orderBy(col("total_price").desc)
        .limit(50)
    }
    
    val phase1Count = phase1Result.count()
    println(s"  Phase 1 complete: $phase1Count rows")
    
    // Phase 2: Create temp file, then delete before read
    println(s"  Phase 2: Creating temp parquet ($tempRows rows)...")
    val testData = spark.range(1, tempRows)
      .withColumn("value", rand(runNum.toLong))
      .withColumn("category", (col("id") % numCategories).cast("string"))
    testData.write.mode("overwrite").parquet(TMP_META_PATH)
    
    val lazyDf = spark.read.parquet(TMP_META_PATH)
    
    println("  INJECTION: Deleting source parquet file...")
    fs.delete(metaPath, true)
    
    println("  Triggering read on deleted file...")
    lazyDf.groupBy("category").agg(sum("value").as("total")).count()
  }

  // ============================================================================
  // DISK: Cross join with variable sizes (capped for speed)
  // ============================================================================
  
  def runDiskFailure(spark: SparkSession, runNum: Int): Try[Long] = Try {
    import spark.implicits._
    
    val limitSize = 10000 + (runNum % 5) * 5000  // 10K, 15K, 20K, 25K, 30K
    val variation = runNum % 2
    
    println(s">>> DISK: limit=$limitSize, var=$variation (runNum=$runNum)")
    
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.crossJoin.enabled", "true")
    
    val result = variation match {
      case 0 =>
        println(s"  Cross join: orders($limitSize) x orders")
        val orders = spark.read.parquet(s"$TPCH_PATH/orders").limit(limitSize)
        orders.crossJoin(orders.withColumnRenamed("o_orderkey", "o_orderkey2"))
          .agg(count("*").as("total_rows"), sum("o_totalprice").as("total_price"))
      case 1 =>
        println(s"  Cross join: lineitem($limitSize) x customer")
        val lineitem = spark.read.parquet(s"$TPCH_PATH/lineitem").limit(limitSize)
        val customer = spark.read.parquet(s"$TPCH_PATH/customer")
        lineitem.crossJoin(customer)
          .agg(count("*").as("total_rows"), sum("l_extendedprice").as("total_price"))
    }
    
    result.collect().head.getLong(0)
  }

  // ============================================================================
  // NET: Sleep UDF with variable duration (capped for reasonable timeout)
  // ============================================================================
  
  def runNetworkTimeout(spark: SparkSession, runNum: Int): Try[Long] = Try {
    import spark.implicits._
    
    val sleepTimeMs = 15000L + (runNum % 4) * 10000L  // 15s, 25s, 35s, 45s
    val rowLimit = 3 + (runNum % 3) * 2                // 3, 5, 7 rows
    
    println(s">>> NET: sleep=${sleepTimeMs/1000}s, limit=$rowLimit (runNum=$runNum)")
    
    val slowUdf: UserDefinedFunction = udf((qty: Double) => {
      Thread.sleep(sleepTimeMs)
      qty * 2
    })
    
    val lineitem = spark.read.parquet(s"$TPCH_PATH/lineitem")
      .filter(col("l_shipdate") <= lit("1998-09-02"))
      .limit(rowLimit)
    
    val result = lineitem
      .withColumn("disc_price", col("l_extendedprice") * (lit(1) - col("l_discount")))
      .withColumn("charge", col("l_extendedprice") * (lit(1) - col("l_discount")) * (lit(1) + col("l_tax")))
      .withColumn("slow_qty", slowUdf(col("l_quantity")))
      .groupBy("l_returnflag", "l_linestatus")
      .agg(
        sum("l_quantity").as("sum_qty"),
        sum("l_extendedprice").as("sum_base_price"),
        sum("disc_price").as("sum_disc_price"),
        sum("charge").as("sum_charge"),
        count("*").as("count_order")
      )
      .orderBy("l_returnflag", "l_linestatus")
    
    result.count()
  }
}
