// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.test.benchmarks

import com.microsoft.azure.synapse.ml.build.BuildInfo
import com.microsoft.azure.synapse.ml.core.env.{FileUtilities, StreamUtilities}
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.sql.Row
import org.scalatest.Assertion

import java.io.{File, PrintWriter}
import scala.collection.mutable.ListBuffer

case class Benchmark(name: String,
                     value: Double,
                     precision: Double,
                     higherIsBetter: Boolean = true) {
  def toCSVEntry: String = {
    s"$name,$value,$precision,$higherIsBetter"
  }
}

object Benchmark {
  def fromRow(r: Row): Benchmark = {
    Benchmark(r.getString(0), r.getDouble(1), r.getDouble(2), r.getBoolean(3))
  }

  def csvHeader: String = {
    "name,value,precision,higherIsBetter"
  }

}

trait Benchmarks extends TestBase {

  lazy val resourcesDirectory = new File(getClass.getResource("/").toURI)
  lazy val oldBenchmarkFile = new File(new File(resourcesDirectory, "benchmarks"),
                                 s"benchmarks_$this.csv")
  lazy val newBenchmarkFile = new File(new File(resourcesDirectory, "new_benchmarks"),
                                 s"new_benchmarks_$this.csv")
  lazy val newBenchmarks: ListBuffer[Benchmark] = ListBuffer[Benchmark]()

  def addBenchmark(name: String,
                   value: Double,
                   precision: Double,
                   higherIsBetter: Boolean): Unit = {
    assert(!newBenchmarks.map(_.name).contains(name), s"Benchmark $name already exists")
    newBenchmarks.append(Benchmark(name, value, precision, higherIsBetter))
  }

  def addBenchmark(name: String,
                   value: Double,
                   precision: Int,
                   higherIsBetter: Boolean): Unit = {
    addBenchmark(name, value, scala.math.pow(10, -precision.toDouble), higherIsBetter)
  }

  def addBenchmark(name: String,
                   value: Double,
                   precision: Int): Unit = {
    addBenchmark(name, value, precision, higherIsBetter = true)
  }

  def addBenchmark(name: String,
                   value: Double,
                   precision: Double): Unit = {
    addBenchmark(name, value, precision, higherIsBetter = true)
  }

  def compareBenchmark(bmNew: Benchmark, bmOld: Benchmark): Assertion = {
    assert(bmNew.name == bmOld.name, "Benchmark names do not match")
    assert(bmNew.higherIsBetter == bmOld.higherIsBetter, "higherIsBetter does not match")
    assert(bmNew.precision === bmNew.precision, "precision does not match")
    assert(bmNew.precision >= 0, "precision needs to be positive")

    val diff = bmNew.value - bmOld.value
    assert(
      if (bmNew.higherIsBetter) {
        diff + bmNew.precision > 0
      } else {
        -1.0 * diff + bmNew.precision > 0
      }, s"new benchmark on ${bmNew.name} ${bmNew.value} and " +
        s"old benchmark ${bmOld.value} " +
        s"are not within ${bmNew.precision}")
  }

  def writeCSV(benchmarks: ListBuffer[Benchmark], file: File): Unit = {
    val lines = Seq(Benchmark.csvHeader) ++ benchmarks.map(_.toCSVEntry)
    if (!file.exists()) file.getParentFile.mkdirs()
    StreamUtilities.using(new PrintWriter(file)) { pw =>
      pw.write(lines.mkString("\n"))
    }.get
  }

  def verifyBenchmarks(): Unit = {
    if (newBenchmarkFile.exists()) newBenchmarkFile.delete()
    writeCSV(newBenchmarks, newBenchmarkFile)

    val oldBenchmarks = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv(oldBenchmarkFile.getAbsolutePath)
      .collect().map(Benchmark.fromRow)
    val newMap = newBenchmarks.map(bm => (bm.name, bm)).toMap
    val oldMap = oldBenchmarks.map(bm => (bm.name, bm)).toMap

    assert(Set(newMap.keys) === Set(oldMap.keys))
    newMap.foreach { case (k, newBM) => compareBenchmark(newBM, oldMap(k)) }
  }

}

object DatasetUtils {

  def binaryTrainFile(name: String): File =
    FileUtilities.join(BuildInfo.datasetDir,"Binary","Train", name)

  def multiclassTrainFile(name: String): File =
    FileUtilities.join(BuildInfo.datasetDir,"Multiclass","Train", name)

  def multiclassTestFile(name: String): File =
    FileUtilities.join(BuildInfo.datasetDir,"Multiclass","Test", name)

  def regressionTrainFile(name: String): File =
    FileUtilities.join(BuildInfo.datasetDir,"Regression","Train", name)

  def rankingTrainFile(name: String): File =
    FileUtilities.join(BuildInfo.datasetDir,"Ranking","Train", name)

  def rankingTestFile(name: String): File =
    FileUtilities.join(BuildInfo.datasetDir,"Ranking","Test", name)

  def madTestFile(name: String): File =
    FileUtilities.join(BuildInfo.datasetDir, "MultivariateAnomalyDetection", name)

  def causalTrainFile(name: String): File=
    FileUtilities.join(BuildInfo.datasetDir, "Causal", name)

}
