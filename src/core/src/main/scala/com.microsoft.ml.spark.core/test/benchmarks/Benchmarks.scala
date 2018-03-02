// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.core.test.benchmarks

import java.io.File

import com.microsoft.ml.spark.core.env.FileUtilities.{File, readFile, writeFile}
import com.microsoft.ml.spark.core.test.base.TestBase
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ArrayBuffer

abstract class Benchmarks extends TestBase {
  val moduleName: String
  val targetDirectory = new File("target")
  val resourcesDirectory = new File(new File(getClass.getResource("/").toURI), "../../../src/test/resources")
  val historicMetricsFile  = new File(resourcesDirectory, "benchmarkMetrics.csv")
  val benchmarkMetricsFile = new File(targetDirectory, s"newMetrics_${System.currentTimeMillis}_.csv")

  val accuracyResults = ArrayBuffer.empty[String]
  def addAccuracyResult(items: Any*): Unit = {
    val line = items.map(_.toString).mkString(",")
    println(s"... $line")
    accuracyResults += line
    ()
  }

  def getFileSystem(): FileSystem = {
    val config = sc.hadoopConfiguration
    import org.apache.hadoop.fs.Path
    val dfs_cwd = new Path(".")
    dfs_cwd.getFileSystem(config)
  }

  /** Reads a CSV file given the file name and file location.
    * @param fileName The name of the csv file.
    * @param fileLocation The full path to the csv file.
    * @return A dataframe from read CSV file.
    */
  def readCSV(fileName: String, fileLocation: String): DataFrame = {
    session.read
      .option("header", "true").option("inferSchema", "true")
      .option("treatEmptyValuesAsNulls", "false")
      .option("delimiter", if (fileName.endsWith(".csv")) "," else "\t")
      .csv(fileLocation)
  }

  /** Rounds the given metric to 2 decimals.
    * @param metric The metric to round.
    * @return The rounded metric.
    */
  def round(metric: Double, decimals: Int): Double = {
    BigDecimal(metric)
      .setScale(decimals, BigDecimal.RoundingMode.HALF_UP).toDouble
  }

  def compareBenchmarkFiles(): Unit = {
    writeFile(benchmarkMetricsFile, accuracyResults.mkString("\n") + "\n")
    val historicMetrics = readFile(historicMetricsFile, _.getLines.toList)
    if (historicMetrics.length != accuracyResults.length)
      throw new Exception(s"Mis-matching number of lines in new benchmarks file: $benchmarkMetricsFile")
    for (((hist,acc),i) <- (historicMetrics zip accuracyResults).zipWithIndex) {
      assert(hist == acc,
        s"""Lines do not match on file comparison:
           |  $historicMetricsFile:$i:
           |    $hist
           |  $benchmarkMetricsFile:$i:
           |    $acc
           |.""".stripMargin)
    }
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

}

object RegressionTestUtils {

  def regressionTrainFile(name: String): File =
    new File(s"${sys.env("DATASETS_HOME")}/Regression/Train", name)

}

object ClassifierTestUtils {

  def classificationTrainFile(name: String): File =
    new File(s"${sys.env("DATASETS_HOME")}/Binary/Train", name)

  def multiclassClassificationTrainFile(name: String): File =
    new File(s"${sys.env("DATASETS_HOME")}/Multiclass/Train", name)

}
