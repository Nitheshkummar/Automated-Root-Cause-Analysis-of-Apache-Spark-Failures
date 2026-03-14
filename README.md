# 🚀 Spark Root Cause Analysis (RCA) Project

[![Scala](https://img.shields.io/badge/Scala-2.12.18-red.svg)](https://www.scala-lang.org/)
[![Spark](https://img.shields.io/badge/Apache%20Spark-3.5.1-orange.svg)](https://spark.apache.org/)
[![HDFS](https://img.shields.io/badge/HDFS-Hadoop%203.2.1-yellow.svg)](https://hadoop.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-blue.svg)](https://docs.docker.com/compose/)
[![Python](https://img.shields.io/badge/Python-PySpark%20ML-green.svg)](https://spark.apache.org/docs/latest/api/python/)

**Automated Root Cause Analysis of Spark Failures using DAG Propagation & Machine Learning**

---

## 📌 Overview

This project implements an automated system for diagnosing Spark job failures by:

1. **Generating labeled training data** — 80 TPC-H benchmark runs across 7 failure categories (1 baseline + 6 injected failures), orchestrated by `CampaignRunner`
2. **Analyzing failure propagation** through Spark's DAG (Directed Acyclic Graph) using Reverse BFS
3. **Classifying root causes** using Random Forest (Scala MLlib) and a multi-model Python pipeline (RF, Decision Tree, Logistic Regression)

The entire infrastructure runs on **Docker Compose** with a Hadoop/YARN cluster, Spark workers, and Jupyter for interactive analysis.

---

## 🛠️ Technology Stack

| Component | Technology |
|-----------|------------|
| **Compute Engine** | Apache Spark 3.5.1 on YARN |
| **Storage** | HDFS (Hadoop 3.2.1) |
| **Container Platform** | Docker Compose (Hadoop bde2020 images + Apache Spark) |
| **Language** | Scala 2.12.18 (pipeline) + Python (ML training) |
| **ML Library** | Spark MLlib (Scala) + PySpark ML (Python) |
| **Data Benchmark** | TPC-H (25GB scale) |
| **Build Tool** | sbt 1.9.7 |
| **Database** | MongoDB 6 (metadata) |
| **Notebook** | Jupyter (via python-lab container) |

---





## 🚦 Quick Start

### Prerequisites

- Docker Desktop with 16GB+ RAM allocated
- Java 11 (for local SBT builds)
- sbt 1.9.x
- PowerShell (Windows)

### 1. Start Infrastructure & Build

```bash
# Start Docker cluster
docker compose up -d

# Build the assembly JAR
cd spark-rca-project
sbt clean assembly
```

### 2. Deploy JAR

```powershell
# Copy JAR to Spark container
docker cp target/scala-2.12/spark-rca-assembly.jar spark-shell:/opt/spark/jars/

# Copy to namenode and upload to HDFS
docker cp target/scala-2.12/spark-rca-assembly.jar namenode:/tmp/
docker exec namenode hdfs dfs -mkdir -p /project/lib
docker exec namenode hdfs dfs -put -f /tmp/spark-rca-assembly.jar /project/lib/
```

### 3. Generate & Ingest TPC-H Data

```powershell
# Generate TPC-H data (25GB scale)
cd scripts
.\generate_tpch_data.ps1 -ScaleFactor 25

# Upload to HDFS
docker exec namenode hdfs dfs -mkdir -p /project/parquet
# (Or use the Parquet converter via spark-submit)
```

### 4. Run Failure Injection Campaign (80 runs)

```powershell
docker exec -e HADOOP_CONF_DIR=/etc/hadoop -e HADOOP_USER_NAME=root spark-shell `
  /opt/spark/bin/spark-submit `
  --master yarn --deploy-mode client `
  --class com.sparkrca.injection.CampaignRunner `
  --driver-memory 512m `
  --conf spark.ui.enabled=false `
  --conf spark.eventLog.enabled=true `
  --conf "spark.eventLog.dir=hdfs:///project/spark-logs/event-logs" `
  /opt/spark/jars/spark-rca-assembly.jar
```

### 5. Run Preprocessing

```powershell
docker exec -e HADOOP_CONF_DIR=/etc/hadoop spark-shell `
  /opt/spark/bin/spark-submit `
  --master yarn --deploy-mode client `
  --class com.sparkrca.PreprocessRunner `
  /opt/spark/jars/spark-rca-assembly.jar /project/spark-logs /project/preprocess
```

### 6. Train ML Models

```powershell
# Scala MLlib classifier
docker exec spark-shell /opt/spark/bin/spark-submit `
  --master yarn --class com.sparkrca.Main `
  /opt/spark/jars/spark-rca-assembly.jar train

# Python multi-model pipeline (RF, DT, LR)
docker exec python-lab spark-submit spark_rca_ml.py
```

---

## 🔬 The 7 Failure Categories

The `CampaignRunner` orchestrates 80 total runs distributed across 7 categories:

| Label | Name | Runs | TPC-H Query | Injection Method |
|-------|------|------|-------------|------------------|
| 0 | BASELINE | 12 | Q21 | Normal execution (no failure) |
| 1 | OUT_OF_MEMORY | 12 | Q9 | BROADCAST on lineitem table |
| 2 | DATA_SKEW | 12 | Q18 | Salted join key producing 99% skew |
| 3 | SERIALIZATION | 11 | Q2 | Non-serializable Socket object in UDF |
| 4 | METADATA_FAILURE | 11 | Iterative | HDFS path deleted mid-read |
| 5 | DISK_SPACE | 11 | CrossJoin | Massive shuffle spill via cross join |
| 6 | NETWORK_TIMEOUT | 11 | Q1 | UDF sleep exceeding heartbeat timeout |

Each run uses `SparkLauncher` in YARN cluster mode, with event logs written to scenario-specific HDFS directories (`/project/spark-logs/<scenario>/`).

---

## 🧠 How It Works

### Phase 1: Data Lake Foundation
- Convert TPC-H text files to Parquet format
- Optimize for columnar scanning in Deep DAGs

### Phase 2: Failure Factory
- Execute TPC-H queries with injected failures
- Generate Spark event logs with ground truth labels

### Phase 3: Intelligence Core
1. **Log Parser**: Extracts metrics from `SparkListenerTaskEnd` events
2. **DAG Builder**: Reconstructs stage dependency graph
3. **Propagation Analyzer**: Uses **Reverse BFS** to find root cause
4. **Feature Extractor**: Computes `skew_index`, `spill_ratio`, `network_pressure`

### Phase 4: Machine Learning

**Scala MLlib** (`RCAClassifier.scala`):
- Random Forest Classifier (100 trees, depth 10)
- 80/20 train/validation split
- Per-class precision, recall, F1 (`ModelEvaluator.scala`)

**Python PySpark** (`spark_rca_ml.py` / `spark_rca_ml.ipynb`):
- Three models: Random Forest, Decision Tree, Logistic Regression
- StandardScaler + VectorAssembler pipeline
- Cross-validation and confusion matrix output

---

## 📊 Feature Set (25 Features)

Extracted per-application by `FeatureExtractor.scala` from aggregated task metrics across all stages:

| Group | Features |
|-------|----------|
| **Stage-Level Aggregates (8)** | `mean_task_duration`, `std_task_duration`, `max_task_duration`, `total_memory_spilled`, `total_disk_spilled`, `total_shuffle_read`, `total_shuffle_write`, `total_gc_time` |
| **Structural Features (4)** | `total_stages`, `failed_stages`, `max_stage_parallelism`, `stage_depth_of_failure` |
| **Ratio Features (4)** | `completed_stage_ratio`, `failed_stage_ratio`, `spill_per_stage`, `gc_per_stage` |
| **Derived Features (9)** | `skew_index`, `duration_variance`, `max_min_duration_ratio`, `spill_ratio`, `disk_spill_ratio`, `peak_memory_ratio`, `gc_time_ratio`, `network_pressure`, `shuffle_write_ratio` |

---


PREDICTIONS
+----------------------+-------------------+-------+------------------+
|app_id                |root_cause_stage_id|predict|predicted_label   |
+----------------------+-------------------+-------+------------------+
|app_1234567890_0001   |5                  |1.0    |OUT_OF_MEMORY     |
+----------------------+-------------------+-------+------------------+
```

---

## 🔧 Configuration

### HDFS Paths (SparkConfig.scala)

```scala
object Paths {
  val HDFS_BASE = "hdfs://namenode:8020/project"
  val TPCH_PARQUET = s"$HDFS_BASE/parquet"
  val EVENT_LOGS = s"$HDFS_BASE/spark-logs"
  val ML_MODELS = s"$HDFS_BASE/models"
  val PREPROCESS = s"$HDFS_BASE/preprocess"
}
```

### YARN Tuning

The capacity scheduler's `maximum-am-resource-percent` may need to be increased for running many concurrent jobs:

```bash
# On resourcemanager container
docker exec resourcemanager bash -c \
  "sed -i 's/<value>0.1<\/value>/<value>0.8<\/value>/' /etc/hadoop/capacity-scheduler.xml"
docker exec resourcemanager yarn rmadmin -refreshQueues
```

## ⚠️ Limitations
- Requires Spark event logging to be enabled
- Focuses on batch Spark workloads (not streaming)
- Assumes a single dominant root cause per job


---

## 📚 References

- [TPC-H Benchmark](http://www.tpc.org/tpch/)
- [Apache Spark MLlib](https://spark.apache.org/mllib/)
- [Spark Monitoring and Instrumentation](https://spark.apache.org/docs/latest/monitoring.html)

---

## 📄 License

This project is for academic purposes.

---

## 👥 Authors

Jainithissh S       & CB.SC.U4AIE23129 
Krishna Prakash S   & CB.SC.U4AIE23138 
Nitheshkummar C     & CB.SC.U4AIE23155 
Akhilesh Kumar S    & CB.SC.U4AIE23170 
