package com.sparkrca.datalake

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, FileStatus}
import org.apache.spark.sql.SparkSession
import scala.util.Try

/**
 * HDFS Utility Functions for the RCA Project.
 * Provides file operations, path validation, and connection management.
 */
object HDFSUtils {

  /**
   * Gets the HDFS FileSystem instance.
   */
  def getFileSystem(spark: SparkSession): FileSystem = {
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    FileSystem.get(hadoopConf)
  }
  
  /**
   * Gets a new Configuration for standalone operations.
   */
  def getConfiguration: Configuration = {
    val conf = new Configuration()
    conf.set("fs.defaultFS", "hdfs://localhost:9000") // Default, override as needed
    conf
  }

  // ============================================================================
  // Path Validation
  // ============================================================================
  
  /**
   * Checks if a path exists on HDFS.
   */
  def pathExists(spark: SparkSession, path: String): Boolean = {
    val fs = getFileSystem(spark)
    fs.exists(new Path(path))
  }
  
  /**
   * Checks if a path is a directory.
   */
  def isDirectory(spark: SparkSession, path: String): Boolean = {
    val fs = getFileSystem(spark)
    val p = new Path(path)
    fs.exists(p) && fs.getFileStatus(p).isDirectory
  }
  
  /**
   * Checks if a path is a file.
   */
  def isFile(spark: SparkSession, path: String): Boolean = {
    val fs = getFileSystem(spark)
    val p = new Path(path)
    fs.exists(p) && fs.getFileStatus(p).isFile
  }

  // ============================================================================
  // Directory Operations
  // ============================================================================
  
  /**
   * Creates a directory on HDFS (with parent directories).
   */
  def createDirectory(spark: SparkSession, path: String): Boolean = {
    val fs = getFileSystem(spark)
    fs.mkdirs(new Path(path))
  }
  
  /**
   * Lists files in an HDFS directory.
   * 
   * @param path Directory path
   * @param recursive Whether to list recursively
   * @return Array of FileStatus objects
   */
  def listFiles(spark: SparkSession, path: String, recursive: Boolean = false): Array[FileStatus] = {
    val fs = getFileSystem(spark)
    val p = new Path(path)
    
    if (!fs.exists(p)) {
      Array.empty[FileStatus]
    } else if (recursive) {
      val iter = fs.listFiles(p, true)
      val files = scala.collection.mutable.ArrayBuffer[FileStatus]()
      while (iter.hasNext) {
        files += iter.next()
      }
      files.toArray
    } else {
      fs.listStatus(p)
    }
  }
  
  /**
   * Lists only subdirectories in an HDFS directory.
   */
  def listDirectories(spark: SparkSession, path: String): Array[FileStatus] = {
    listFiles(spark, path).filter(_.isDirectory)
  }

  // ============================================================================
  // File Operations
  // ============================================================================
  
  /**
   * Deletes a path on HDFS.
   * 
   * @param path Path to delete
   * @param recursive Delete recursively if directory
   * @return true if deletion succeeded
   */
  def delete(spark: SparkSession, path: String, recursive: Boolean = true): Boolean = {
    val fs = getFileSystem(spark)
    val p = new Path(path)
    if (fs.exists(p)) {
      fs.delete(p, recursive)
    } else {
      true // Already doesn't exist
    }
  }
  
  /**
   * Renames/moves a path on HDFS.
   */
  def rename(spark: SparkSession, srcPath: String, dstPath: String): Boolean = {
    val fs = getFileSystem(spark)
    fs.rename(new Path(srcPath), new Path(dstPath))
  }
  
  /**
   * Copies a file within HDFS.
   */
  def copy(spark: SparkSession, srcPath: String, dstPath: String, overwrite: Boolean = false): Boolean = {
    val fs = getFileSystem(spark)
    val src = new Path(srcPath)
    val dst = new Path(dstPath)
    
    if (!overwrite && fs.exists(dst)) {
      false
    } else {
      org.apache.hadoop.fs.FileUtil.copy(fs, src, fs, dst, false, spark.sparkContext.hadoopConfiguration)
      true
    }
  }

  // ============================================================================
  // Size & Stats
  // ============================================================================
  
  /**
   * Gets the size of a file or directory in bytes.
   */
  def getSize(spark: SparkSession, path: String): Long = {
    val fs = getFileSystem(spark)
    val p = new Path(path)
    if (fs.exists(p)) {
      fs.getContentSummary(p).getLength
    } else {
      0L
    }
  }
  
  /**
   * Gets the size in human-readable format.
   */
  def getSizeFormatted(spark: SparkSession, path: String): String = {
    val bytes = getSize(spark, path)
    formatBytes(bytes)
  }
  
  /**
   * Formats bytes to human-readable format.
   */
  def formatBytes(bytes: Long): String = {
    if (bytes < 1024) s"$bytes B"
    else if (bytes < 1024 * 1024) f"${bytes / 1024.0}%.2f KB"
    else if (bytes < 1024 * 1024 * 1024) f"${bytes / (1024.0 * 1024)}%.2f MB"
    else f"${bytes / (1024.0 * 1024 * 1024)}%.2f GB"
  }

  // ============================================================================
  // Utility Functions
  // ============================================================================
  
  /**
   * Ensures a directory exists, creating it if necessary.
   */
  def ensureDirectory(spark: SparkSession, path: String): Unit = {
    if (!pathExists(spark, path)) {
      createDirectory(spark, path)
      println(s"Created directory: $path")
    }
  }
  
  /**
   * Safely executes an operation, returning Success/Failure.
   */
  def safeOperation[T](operation: => T): Try[T] = {
    Try(operation)
  }
  
  /**
   * Prints directory tree structure.
   */
  def printTree(spark: SparkSession, path: String, indent: String = ""): Unit = {
    val files = listFiles(spark, path)
    files.foreach { file =>
      val name = file.getPath.getName
      val suffix = if (file.isDirectory) "/" else ""
      println(s"$indent├── $name$suffix")
      if (file.isDirectory) {
        printTree(spark, file.getPath.toString, indent + "│   ")
      }
    }
  }
}
