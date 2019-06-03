package org.bom4v.ti

//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.fs._

/**
  * Simple Spark job aimed at checking that everything is working.
  * It is aimed at being launched in a standalone mode,
  * for instance within the SBT JVM.
  */
object DeltaLakeVersionDisplayer extends App {
  //
  val spark = org.apache.spark.sql.SparkSession
    .builder()
    .appName("DeltaLakeVersionDisplayer")
    .master("local[*]")
    .getOrCreate()

  // Display versions
  Utilities.displayVersions (spark)

  // End of the Spark session
  spark.stop()
}

/**
  * Spark job aimed at storing data from a CSV file into Delta Lake.
  * It is aimed at being launched in a Spark cluster,
  * typically with YARN or Mesos
  */
object DeltaLakeStorer extends App {
  //
  val spark = org.apache.spark.sql.SparkSession
    .builder()
    .appName("DeltaLakeStorer")
    .master("local[*]")
    .getOrCreate()

  // Display versions
  Utilities.displayVersions (spark)

  // CSV file (input)
  val defaultCSVFile = "data/cdr_example.csv"

  // Delta Lake directory (output)
  val defaultDlkFilepath = "/tmp/delta-lake/table.dlk"

  // Retrieve the expected filename of the resulting CSV file,
  // if given as command line parameter
  val inputCSVFile = Utilities.getCSVFilePath (defaultCSVFile, args)
  println ("File-path for the CSV data file: " + inputCSVFile)

  // Retrieve the file-path of the Delta Lake-stored data frame,
  // if given as command line parameter
  val dlkFilepath = Utilities.getDeltaLakeFilepath (defaultDlkFilepath, args)
  println ("File-path for the Delta Lake table/data-frame: " + dlkFilepath)

  // Read the data from the CSV file
  val df = spark.read
    .option ("inferSchema", "true")
    .option ("header", "true")
    .option ("delimiter", "^")
    .csv (inputCSVFile)

  // Dump the data-frame into Delta Lake
  df.write.format("delta").mode("overwrite").save(dlkFilepath)

  // End of the Spark session
  spark.stop()
}

/**
  * Spark job aimed at retrieving CSV data files from Delta Lake.
  * It is aimed at being launched in a Spark cluster,
  * typically with YARN or Mesos
  */
object DeltaLakeRetriever extends App {
  //
  val spark = org.apache.spark.sql.SparkSession
    .builder()
    .appName("DeltaLakeRetriever")
    .master("local[*]")
    .getOrCreate()

  // Display versions
  Utilities.displayVersions (spark)

  // Delta Lake directory (input)
  val defaultDlkFilepath = "/tmp/delta-lake/table.dlk"

  // CSV file (output)
  val tmpOutputDir = "delta-extract-tmp"
  val defaultCSVFile = "delta-extract.csv"

  // Retrieve the file-path of the Delta Lake-stored data frame,
  // if given as command line parameter
  val dlkFilepath = Utilities.getDeltaLakeFilepath (defaultDlkFilepath, args)
  println ("File-path for the Delta Lake table/data-frame: " + dlkFilepath)

  // Retrieve the expected filename of the resulting CSV file,
  // if given as command line parameter
  val outputCSVFile = Utilities.getCSVFilePath (defaultCSVFile, args)
  println ("File-path for the expected CSV file: " + outputCSVFile)

  // Read latest version of the data
  val dfLatest = spark.read.format("delta").load(dlkFilepath)

  // Read older versions of data using time travel
  //val dfOlder = spark.read.format("delta").option("versionAsOf", 0).load(dlkFilepath)

  // Dump the resulting DataFrame into a (list of) CSV file(s)
  // The write() method on a Spark DataFrame (DF)indeed creates
  // a directory with all the chunks, which then need
  // to be re-assembled thanks to HDFS utilities (here,
  // the Utilities.merge() method)
  dfLatest.write
    .format ("com.databricks.spark.csv")
    .option ("header", "true")
    .option ("delimiter", "^")
    .option ("quote", "\u0000")
    .mode ("overwrite")
    .save (tmpOutputDir)
  //Utilities.merge (tmpOutputDir, outputCSVFile)
  dfLatest.unpersist()

  // End of the Spark session
  spark.stop()
}


