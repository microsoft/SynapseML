// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split1

import com.microsoft.azure.synapse.ml.core.test.base.{SparkSessionManagement, TestBase}
import com.microsoft.azure.synapse.ml.lightgbm.LightGBMRegressor
import org.apache.commons.io.FileUtils
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.DataFrame

import java.io.File
import java.net.{ServerSocket, URLClassLoader}
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.TimeUnit

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

  Seq("dense", "sparse").foreach { matrix =>
    test(s"streaming $matrix ingestion covers a wider ambient OpenMP team", TestBase.LinuxOnly) {
      assume(System.getProperty("os.name").startsWith("Linux"), "Requires the bundled Linux OpenMP runtime")
      val directory = Files.createTempDirectory("streaming-omp-").toFile
      try {
        val log = new File(directory, "probe.log")
        val java = new File(System.getProperty("java.home"), "bin/java").getAbsolutePath
        val builder = new ProcessBuilder(
          java, "-Xms256m", "-Xmx2g", "-XX:ActiveProcessorCount=8", "-XX:-CreateCoredumpOnCrash",
          s"-Djava.io.tmpdir=${directory.getAbsolutePath}",
          s"-XX:ErrorFile=${new File(directory, "native-error.log").getAbsolutePath}",
          "-cp", runtimeClasspath, StreamingOmpRegressionProbe.getClass.getName.stripSuffix("$"), matrix)
          .directory(directory).redirectErrorStream(true).redirectOutput(log)
        // A native failure must not put inherited CI credentials into a crash report.
        builder.environment().clear()
        builder.environment().put("HOME", directory.getAbsolutePath)
        builder.environment().put("SPARK_LOCAL_IP", "127.0.0.1")
        builder.environment().put("OMP_NUM_THREADS", "32")
        builder.environment().put("OMP_THREAD_LIMIT", "32")
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
        assert(output.contains(s"STREAMING_OMP_OK matrix=$matrix"), diagnostic)
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

  private def predictions(data: DataFrame, mode: String, matrix: String): Array[Double] = {
    val estimator = new LightGBMRegressor()
      .setDataTransferMode(mode)
      .setMatrixType(matrix)
      .setNumTasks(4)
      .setNumThreads(2)
      .setMaxStreamingOMPThreads(InitialAllocationWidth)
      .setUseSingleDatasetMode(true)
      .setNumIterations(3)
      .setNumLeaves(3)
      .setMinDataInLeaf(1)
      .setVerbosity(2)
      .setPassThroughArgs(
        "enable_bundle=false min_data_in_bin=1 feature_pre_filter=false deterministic=true force_col_wise=true")
      .setDefaultListenPort(freePort())
    val model = estimator.fit(data)
    try {
      val output = model.transform(data).orderBy("id").select("prediction").collect().map(_.getDouble(0))
      assert(output.length == Rows)
      assert(output.forall(value => !value.isNaN && !value.isInfinity))
      output
    } finally {
      model.getModel.freeNativeMemory()
    }
  }

  def main(args: Array[String]): Unit = {
    require(args.length == 1 && Set("dense", "sparse").contains(args(0)))
    require(sys.env.get("OMP_NUM_THREADS").contains("32"))
    val matrix = args(0)
    resetSparkSession(numCores = Some(4))
    try {
      val session = spark
      import session.implicits._

      // Overlapping sparse columns prevent feature bundling from hiding SparseBin writes.
      val data = (0 until Rows).map { index =>
        val values = (0 until Columns).map { column =>
          if (index % NonzeroInterval == 0) (1 + (index + column) % ValueCycle).toDouble else 0.0
        }.toArray
        val features = if (matrix == "sparse") Vectors.dense(values).toSparse else Vectors.dense(values)
        (index, if (values(0) > 0) 1.0 else 0.0, features)
      }.toDF("id", "label", "features").repartition(4).cache()
      try {
        // Streaming goes first so bulk initialization cannot narrow the fresh worker teams.
        val streaming = predictions(data, "streaming", matrix)
        val bulk = predictions(data, "bulk", matrix)
        val difference = streaming.zip(bulk).map { case (left, right) => math.abs(left - right) }.max
        assert(difference < PredictionTolerance, s"Streaming/bulk prediction difference: $difference")
        assert(streaming.distinct.length > 1, "Fixture must produce nonconstant predictions")
        println(s"STREAMING_OMP_OK matrix=$matrix rows=$Rows maxDifference=$difference")
      } finally {
        data.unpersist()
      }
    } finally {
      stopSparkSession()
    }
  }
}
