// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split1

import com.microsoft.azure.synapse.ml.core.test.base.{SparkSessionManagement, TestBase}
import com.microsoft.azure.synapse.ml.lightgbm.{LightGBMRegressor, LightGBMUtils, NativeOmpCallSite}
import com.microsoft.azure.synapse.ml.lightgbm.dataset.ReferenceDatasetUtils
import org.apache.commons.io.FileUtils
import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.config.Configurator
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

import java.io.File
import java.net.{ServerSocket, URLClassLoader}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._

class StreamingOmpRegressionSuite extends TestBase {
  private def runtimeClasspath: String = {
    val loaderPaths = Iterator.iterate(Option(getClass.getClassLoader))(_.flatMap(loader => Option(loader.getParent)))
      .takeWhile(_.isDefined).flatten
      .collect { case loader: URLClassLoader => loader }
      .flatMap(_.getURLs.iterator)
      .filter(_.getProtocol == "file")
      .map(url => new File(url.toURI))
      .toVector
    val systemPaths = System.getProperty("java.class.path").split(File.pathSeparator).map(new File(_))
    (loaderPaths ++ systemPaths).map(_.getCanonicalPath).distinct.mkString(File.pathSeparator)
  }

  Seq("dense", "sparse", "stacked", "limited").foreach { scenario =>
    test(s"streaming $scenario ingestion covers a wider ambient OpenMP team", TestBase.LinuxOnly) {
      assume(System.getProperty("os.name").startsWith("Linux"), "Requires the bundled Linux OpenMP runtime")
      val directory = Files.createTempDirectory("streaming-omp-").toFile
      try {
        val log = new File(directory, "probe.log")
        val java = new File(System.getProperty("java.home"), "bin/java").getAbsolutePath
        val crashOptions = if (System.getProperty("java.specification.version") == "1.8") {
          Seq.empty[String]
        } else {
          Seq("-XX:-CreateCoredumpOnCrash")
        }
        val command = Seq("/bin/sh", "-c", "ulimit -c 0 && exec \"$@\"", "streaming-omp",
          java, "-Xms256m", "-Xmx2g", "-XX:ActiveProcessorCount=8") ++ crashOptions ++ Seq(
          s"-Djava.io.tmpdir=${directory.getAbsolutePath}",
          s"-XX:ErrorFile=${new File(directory, "native-error.log").getAbsolutePath}",
          "-cp", runtimeClasspath, StreamingOmpRegressionProbe.getClass.getName.stripSuffix("$"), scenario)
        val builder = new ProcessBuilder(command: _*)
          .directory(directory).redirectErrorStream(true).redirectOutput(log)
        // A native failure must not put inherited CI credentials into a crash report.
        builder.environment().clear()
        builder.environment().put("HOME", directory.getAbsolutePath)
        builder.environment().put("SPARK_LOCAL_IP", "127.0.0.1")
        builder.environment().put("OMP_NUM_THREADS", if (scenario == "stacked") "8" else "32")
        builder.environment().put("OMP_THREAD_LIMIT", if (scenario == "limited") "16" else "32")
        builder.environment().put("OMP_DYNAMIC", "FALSE")
        builder.environment().put("OMP_PROC_BIND", "FALSE")
        val process = builder.start()
        val timeoutMinutes = 5L
        val completed = try {
          process.waitFor(timeoutMinutes, TimeUnit.MINUTES)
        } finally {
          if (process.isAlive) {
            process.destroyForcibly()
            process.waitFor()
          }
        }
        val output = FileUtils.readFileToString(log, StandardCharsets.UTF_8)
        val diagnosticLimit = 8000
        val diagnostic = output.takeRight(diagnosticLimit)
        assert(completed, s"OpenMP regression timed out:\n$diagnostic")
        assert(process.exitValue() == 0, s"OpenMP regression exited ${process.exitValue()}:\n$diagnostic")
        val writers = if (scenario == "stacked") 1 else 4
        val width = if (scenario == "stacked" || scenario == "limited") 16 else 32
        assert(output.contains(
          s"externalThreads=$writers, configuredMaxStreamingOMPThreads=16, allocationBound=$width"), diagnostic)
        assert(output.contains(s"STREAMING_OMP_OK scenario=$scenario"), diagnostic)
      } finally {
        FileUtils.deleteDirectory(directory)
      }
    }
  }
}

object StreamingOmpRegressionProbe extends SparkSessionManagement {
  private val Rows = 16000
  private val Columns = 16
  private val InitialAllocationWidth = 16
  private val NonzeroInterval = 97
  private val ValueCycle = 7
  private val PredictionTolerance = 1e-12

  private def freePort(): Int = {
    val socket = new ServerSocket(0)
    try socket.getLocalPort finally socket.close()
  }

  private def estimator(mode: String, matrix: String, numTasks: Int): LightGBMRegressor =
    new LightGBMRegressor()
      .setDataTransferMode(mode)
      .setMatrixType(matrix)
      .setNumTasks(numTasks)
      .setNumThreads(if (numTasks == 1) 2 else 1)
      .setMaxStreamingOMPThreads(InitialAllocationWidth)
      .setUseSingleDatasetMode(true)
      .setNumIterations(3)
      .setNumLeaves(3)
      .setMinDataInLeaf(1)
      .setVerbosity(2)
      .setPassThroughArgs(
        "enable_bundle=false min_data_in_bin=1 feature_pre_filter=false deterministic=true force_col_wise=true")
      .setDefaultListenPort(freePort())

  private def predictions(data: DataFrame, mode: String, matrix: String, numTasks: Int): Array[Double] = {
    val model = estimator(mode, matrix, numTasks).fit(data)
    try {
      val output = model.transform(data).orderBy("id").select("prediction").collect().map(_.getDouble(0))
      assert(output.length == Rows)
      assert(output.forall(value => !value.isNaN && !value.isInfinity))
      output
    } finally {
      model.getModel.freeNativeMemory()
    }
  }

  private def inputData(matrix: String): DataFrame = {
    val session = spark
    import session.implicits._

    // Overlapping sparse columns prevent feature bundling from hiding SparseBin writes.
    (0 until Rows).map { index =>
      val values = (0 until Columns).map { column =>
        if (index % NonzeroInterval == 0) (1 + (index + column) % ValueCycle).toDouble else 0.0
      }.toArray
      val features = if (matrix == "sparse") Vectors.dense(values).toSparse else Vectors.dense(values)
      (index, if (values(0) > 0) 1.0 else 0.0, features)
    }.toDF("id", "label", "features")
  }

  private def retainOversizedRequest(data: DataFrame, matrix: String, numTasks: Int): Unit = {
    val requestedThreads = 1000000
    val trainer = estimator("bulk", matrix, numTasks)
    val model = trainer.setPassThroughArgs(
      s"${trainer.getPassThroughArgs} num_threads=1 n_jobs=$requestedThreads").fit(data)
    try {
      assert(LightGBMUtils.nativeOmpThreadHighWaterMark >= requestedThreads)
    } finally {
      model.getModel.freeNativeMemory()
    }
    // Fail before an unsafe allocation if the configured native thread limit is not respected.
    val allocation = ReferenceDatasetUtils.streamingOmpAllocationBound(16, 1, 4)
    assert(allocation == 16, s"Retained request exceeded the native thread limit: allocationBound=$allocation")
  }

  private def verifyHistory(scenario: String): Unit = {
    if (scenario == "dense" || scenario == "sparse") {
      assert(ReferenceDatasetUtils.streamingOmpAllocationBound(16, 1, 4) == 32)
      assert(ReferenceDatasetUtils.streamingOmpAllocationBound(16, 0, 1) == -1)
      LightGBMUtils.registerNativeOmpThreads(NativeOmpCallSite.BoosterResetParameter, "num_threads=40")
      assert(ReferenceDatasetUtils.streamingOmpAllocationBound(16, 1, 4) == 32)
    }
  }

  def main(args: Array[String]): Unit = {
    require(args.length == 1 && Set("dense", "sparse", "stacked", "limited").contains(args(0)))
    val scenario = args(0)
    val stacked = scenario == "stacked"
    val limited = scenario == "limited"
    val matrix = if (scenario == "sparse") "sparse" else "dense"
    val numTasks = if (stacked) 1 else 4
    require(sys.env.get("OMP_NUM_THREADS").contains(if (stacked) "8" else "32"))
    val coreLimit = Files.readAllLines(Paths.get("/proc/self/limits")).asScala
      .find(_.startsWith("Max core file size")).get.stripPrefix("Max core file size").trim.split("\\s+")
    require(coreLimit.take(2).sameElements(Array("0", "0")), "Child must have zero soft and hard core limits")
    resetSparkSession(numCores = Some(4))
    Configurator.setLevel(ReferenceDatasetUtils.getClass.getName, Level.INFO)
    try {
      val data = inputData(matrix).repartition(4).cache()
      try {
        val upstream = if (stacked) Some(estimator("bulk", matrix, numTasks).fit(data.coalesce(1))) else None
        try {
          // Keep scoring lazy: prediction can widen the same thread after streaming initialization.
          val input = upstream.map(_.transform(data.coalesce(1))
            .withColumn("label", col("prediction")).drop("prediction")).getOrElse(data)
          if (limited) retainOversizedRequest(input, matrix, numTasks)
          // In fresh scenarios, stream before bulk initialization can narrow the worker teams.
          val streaming = predictions(input, "streaming", matrix, numTasks)
          val bulk = predictions(input, "bulk", matrix, numTasks)
          val difference = streaming.zip(bulk).map { case (left, right) => math.abs(left - right) }.max
          assert(difference < PredictionTolerance, s"Streaming/bulk prediction difference: $difference")
          assert(streaming.distinct.length > 1, "Fixture must produce nonconstant predictions")
          verifyHistory(scenario)
          if (limited) {
            val repeated = predictions(input, "streaming", matrix, numTasks)
            assert(repeated.zip(bulk).forall { case (left, right) => math.abs(left - right) < PredictionTolerance })
          }
          println(s"STREAMING_OMP_OK scenario=$scenario rows=$Rows maxDifference=$difference")
        } finally {
          upstream.foreach(_.getModel.freeNativeMemory())
        }
      } finally {
        data.unpersist()
      }
    } finally {
      stopSparkSession()
    }
  }
}
