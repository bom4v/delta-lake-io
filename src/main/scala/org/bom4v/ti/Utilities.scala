package org.bom4v.ti

/**
  * Utility methods
  */
object Utilities {
  /**
    * Retrieve and return the version of Scala
    */
  def getScalaVersion(): String = {
    val version:String = util.Properties.versionString
    return version
  }

  /**
    * Retrieve and return the version of Scala
    */
  def getSparkVersion (spark: org.apache.spark.sql.SparkSession): String = {
    val version:String = spark.version
    return version
  }

  /**
    * Display the versions
    */
  def displayVersions (spark: org.apache.spark.sql.SparkSession) {
    println ("Spark: " + getSparkVersion(spark))
    println ("Scala: " + getScalaVersion())
  }

  /**
    * Extract the Hive database name, potentially given as
    * command line parameter
    */
  def getDBName (
    defaultDBName: String,
    argList: Array[String]): String = {
    var dbName : String = defaultDBName
    val dbNamePattern = new scala.util.matching.Regex ("^[a-zA-Z0-9_]+$")
    for (arg <- argList) {
      val dbNameMatch = dbNamePattern.findFirstIn (arg)
      dbNameMatch.foreach { _ =>
        dbName = arg
      }
    }
    return dbName
  }

  /**
    * Extract the file-path of the Delta Lake-stored data frame,
    * potentially given as command line parameter
    */
  def getDeltaLakeFilepath (
    defaultFilepath: String,
    argList: Array[String]): String= {

    var dlkFilepath : String = defaultFilepath
    val dlkFilePattern = new scala.util.matching.Regex ("[.]dlk$")
    for (arg <- argList) {
      val dlkMatch = dlkFilePattern.findFirstIn (arg)
      dlkMatch.foreach { _ =>
        dlkFilepath = arg
      }
    }
    return dlkFilepath
  }

  /**
    * Extract the file-path of the CSV data file,
    * potentially given as command line parameter
    */
  def getCSVFilePath (
    defaultFilePath:String,
    argList:Array[String]): String= {

    var csvFile : String = defaultFilePath
    val csvFilePattern = new scala.util.matching.Regex ("[.]csv(|.bz2)$")
    for (arg <- argList) {
      val csvMatch = csvFilePattern.findFirstIn (arg)
      csvMatch.foreach { _ =>
        csvFile = arg
      }
    }
    return csvFile
  }

  /**
    * Merge several file chunks into a single output file
    * https://www.oreilly.com/library/view/hadoop-the-definitive/9780596521974/ch04.html
    */
  def merge (srcPath: String, dstPath: String): Unit = {
    // The "true" setting deletes the source files once they are merged
    // into the new output
    val hadoopConfig = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get (hadoopConfig)
    val shouldDelete = true
    UtilityForHadoop3
      .copyMerge (hdfs, new org.apache.hadoop.fs.Path (srcPath),
        hdfs, new org.apache.hadoop.fs.Path (dstPath),
        shouldDelete, hadoopConfig)
  }

  /**
    * Sandbox to interactively debug with spark-shell
    */
  def interactiveMerge (srcPathStr: String, dstPathStr: String): Unit = {
    val hadoopConfig = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get (hadoopConfig)
    val srcFS = hdfs
    val dstFS = hdfs
    val dstPath = new org.apache.hadoop.fs.Path (dstPathStr)
    val outputFile = hdfs.create (dstPath)
    val srcPath = new org.apache.hadoop.fs.Path (srcPathStr)
    val factory = new org.apache.hadoop.io.compress.
      CompressionCodecFactory (hadoopConfig)
    val codec = factory.getCodec (srcPath)
    val inputStream = codec.createInputStream (hdfs.open(srcPath))
    org.apache.hadoop.io.
      IOUtils.copyBytes (inputStream, outputFile, hadoopConfig, false)
  }
  
}

