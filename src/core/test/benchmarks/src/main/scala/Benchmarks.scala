// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import com.microsoft.ml.spark.FileUtilities.{File, readFile, writeFile}
import org.apache.hadoop.fs.FileSystem

import scala.collection.mutable.ArrayBuffer

abstract class Benchmarks extends TestBase {
  val moduleName: String
  val targetDirectory = new File("target")
  val thisDirectory = {
    // intellij runs from a different directory
    val d = new File("src/test/scala")
    if (d.isDirectory) d else new File(s"$moduleName/src/test/scala")
  }
  val historicMetricsFile  = new File(thisDirectory, "benchmarkMetrics.csv")
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

  /** Rounds the given metric to 2 decimals.
    * @param metric The metric to round.
    * @return The rounded metric.
    */
  def round(metric: Double, decimals: Int): Double = {
    BigDecimal(metric)
      .setScale(decimals, BigDecimal.RoundingMode.HALF_UP).toDouble
  }

  def compareBenchmarkFiles(): Unit = {
    try writeFile(benchmarkMetricsFile, accuracyResults.mkString("\n") + "\n")
    catch {
      case e: java.io.IOException => throw new Exception("Not able to process benchmarks file")
    }
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

object ClassifierTestUtils {
  def classificationTrainFile(name: String): File =
    new File(s"${sys.env("DATASETS_HOME")}/Binary/Train", name)

  def multiclassClassificationTrainFile(name: String): File =
    new File(s"${sys.env("DATASETS_HOME")}/Multiclass/Train", name)
}
