// ============================================================================
// Spark Root Cause Analysis Project
// Build Configuration
// ============================================================================

name := "spark-rca-project"
version := "1.0.0"
scalaVersion := "2.12.18"

// ----------------------------------------------------------------------------
// Spark & Hadoop Dependencies
// ----------------------------------------------------------------------------
val sparkVersion = "3.5.1"
val hadoopVersion = "3.3.4"

libraryDependencies ++= Seq(
  // Spark Core
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  
  // Hadoop HDFS Client
  "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
  "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion % "provided",
  
  // JSON Parsing for Log Analysis (using native backend to avoid Jackson conflicts with Spark)
  "org.json4s" %% "json4s-native" % "4.0.6",
  
  // CLI Argument Parsing
  "com.github.scopt" %% "scopt" % "4.1.0",
  
  // Testing
  "org.scalatest" %% "scalatest" % "3.2.17" % Test
)

// ----------------------------------------------------------------------------
// Assembly Configuration (Fat JAR)
// ----------------------------------------------------------------------------
assembly / assemblyJarName := "spark-rca-assembly.jar"

// Merge Strategy to handle duplicate files in dependencies
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "services", xs @ _*) => MergeStrategy.concat
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case PathList("META-INF", "NOTICE") => MergeStrategy.discard
  case PathList("META-INF", "NOTICE.txt") => MergeStrategy.discard
  case PathList("META-INF", "LICENSE") => MergeStrategy.discard
  case PathList("META-INF", "LICENSE.txt") => MergeStrategy.discard
  case PathList("META-INF", "DEPENDENCIES") => MergeStrategy.discard
  case PathList("META-INF", xs @ _*) if xs.lastOption.exists(_.endsWith(".SF")) => MergeStrategy.discard
  case PathList("META-INF", xs @ _*) if xs.lastOption.exists(_.endsWith(".DSA")) => MergeStrategy.discard
  case PathList("META-INF", xs @ _*) if xs.lastOption.exists(_.endsWith(".RSA")) => MergeStrategy.discard
  case PathList("META-INF", xs @ _*) => MergeStrategy.first
  case PathList("reference.conf") => MergeStrategy.concat
  case PathList("application.conf") => MergeStrategy.concat
  case PathList("module-info.class") => MergeStrategy.discard
  case "log4j.properties" => MergeStrategy.first
  case "log4j2.properties" => MergeStrategy.first
  case x if x.contains("hadoop") => MergeStrategy.first
  case x if x.contains("javax") => MergeStrategy.first
  case x if x.endsWith(".proto") => MergeStrategy.first
  case x if x.endsWith(".class") => MergeStrategy.first
  case _ => MergeStrategy.first
}

// Exclude Scala library from assembly (provided by Spark)
assembly / assemblyOption := (assembly / assemblyOption).value.withIncludeScala(false)

// ----------------------------------------------------------------------------
// Compiler Options
// ----------------------------------------------------------------------------
scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature",
  "-unchecked",
  "-Xlint",
  "-Ywarn-dead-code"
)

// ----------------------------------------------------------------------------
// Run Options
// ----------------------------------------------------------------------------
Compile / run / fork := true
Compile / run / javaOptions ++= Seq(
  "-Xmx4g",
  "-Xms1g"
)

libraryDependencies += "com.github.luben" % "zstd-jni" % "1.5.5-1"

// Main class for assembly
assembly / mainClass := Some("com.sparkrca.Main")
