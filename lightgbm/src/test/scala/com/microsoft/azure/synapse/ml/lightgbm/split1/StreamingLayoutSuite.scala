// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split1

import com.microsoft.azure.synapse.ml.io.http.SharedSingleton
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.lightgbm._
import com.microsoft.azure.synapse.ml.lightgbm.booster.LightGBMBooster
import com.microsoft.azure.synapse.ml.lightgbm.dataset.ReferenceDatasetUtils
import com.microsoft.azure.synapse.ml.lightgbm.params.BaseTrainParams
import org.apache.spark.ml.linalg.{SQLDataTypes, Vectors}
import org.apache.spark.sql.types.{StructField, StructType}
import org.slf4j.Logger

import java.nio.charset.StandardCharsets
import java.nio.file.Files

class NativeOmpResetDelegate(threadCount: Int,
                             parameterName: String = "num_threads") extends LightGBMDelegate {
  override def beforeTrainIteration(batchIndex: Int,
                                    partitionId: Int,
                                    curIters: Int,
                                    log: Logger,
                                    trainParams: BaseTrainParams,
                                    booster: LightGBMBooster,
                                    hasValid: Boolean): Unit = {
    if (curIters == 0) booster.resetParameter(s"$parameterName=$threadCount")
  }
}

class StreamingLayoutSuite extends LightGBMTestUtils {
  private def context(partitionId: Int, localPartitions: Array[Int], counts: Array[Long]): PartitionTaskContext = {
    val features = StructField("features", SQLDataTypes.VectorType)
    val params = new LightGBMRegressor().getTrainParams(counts.length, features, localPartitions.length)
    val training = TrainingContext(
      batchIndex = 0,
      sharedStateSingleton = SharedSingleton(new SharedState(params)),
      schema = StructType(Seq(features)),
      numCols = 1,
      numInitScoreClasses = 0,
      trainingParams = params,
      networkParams = NetworkParams(12400, "127.0.0.1", 12400, barrierExecutionMode = false),
      columnParams = ColumnParams("label", "features", None, None, None),
      datasetParams = "",
      featureNames = None,
      numTasksPerExecutor = localPartitions.length,
      validationData = None,
      serializedReferenceDataset = None,
      partitionCounts = Some(counts))
    PartitionTaskContext(
      trainingCtx = training,
      partitionId = partitionId,
      taskId = partitionId.toLong,
      measures = new TaskInstrumentationMeasures(partitionId),
      networkTopologyInfo = NetworkTopologyInfo("127.0.0.1:12400", localPartitions, 12400),
      shouldExecuteTraining = true,
      isEmptyPartition = false,
      shouldReturnBooster = true,
      shouldCalcValidationDataset = false)
  }

  test("streaming buffer count uses noncontiguous executor-local partitions") {
    val localPartitions = Array(7, 2, 11)
    val task = context(7, localPartitions, Array.fill(12)(10L))
    assert(task.executorPartitionCount == localPartitions.length)
    assert(task.threadIndex == 1)
    assert(task.executorRowCount == 30)
    assert(task.streamingPartitionOffset == 10)
    assert(task.totalRowCount == 120)
    localPartitions.foreach { id =>
      val localTask = context(id, localPartitions, Array.fill(12)(10L))
      assert(localTask.threadIndex >= 0 && localTask.threadIndex < localTask.executorPartitionCount)
    }
  }

  test("streaming buffer count preserves thread-index slots for empty local partitions") {
    val task = context(2, Array(0, 2, 3), Array(0L, 9L, 5L, 0L, 7L, 8L))
    assert(task.executorPartitionCount == 3)
    assert(task.threadIndex == 1)
    assert(task.executorRowCount == 5)
    assert(task.streamingPartitionOffset == 0)
  }

  test("streaming buffer count is unchanged when all partitions are on one executor") {
    val counts = Array(2L, 3L, 4L)
    val task = context(2, counts.indices.toArray, counts)
    assert(task.executorPartitionCount == counts.length)
    assert(task.threadIndex == 2)
    assert(task.executorRowCount == 9)
    assert(task.streamingPartitionOffset == 5)
  }

  test("streaming OpenMP allocation covers the configured native thread team") {
    val warnings = scala.collection.mutable.ArrayBuffer.empty[String]
    def bound(externalThreads: Int,
              maxThreads: Int,
              numThreads: Int,
              ompThreads: Option[String],
              affinity: Option[Int],
              osProcessors: Option[Int],
              availableProcessors: Int,
              registered: Int): Int =
      ReferenceDatasetUtils.streamingOmpAllocationBound(
        externalThreads,
        maxThreads,
        numThreads,
        ompThreads,
        affinity,
        osProcessors,
        availableProcessors,
        registered,
        warnings += _)

    assert(bound(1, 16, 0, Option("8"), Option(4), Option(64), 64, 2) == -1)
    assert(bound(1, 16, 32, Option("8"), Option(4), Option(64), 64, 2) == 32)
    assert(bound(1, 16, 2, Option("8"), Option(4), Option(64), 64, 2) == 16)
    assert(bound(4, 16, 32, Option("8"), Option(4), Option(64), 64, 2) == 32)
    assert(bound(4, 32, 16, Option("8"), Option(4), Option(64), 64, 2) == 32)
    assert(bound(4, -1, 2, Option("32,8"), Option(4), Option(64), 64, 0) == 32)
    assert(bound(4, 0, 2, Option("invalid,64"), Option(24), Option(64), 64, 0) == 24)
    assert(bound(4, 0, 0, None, Option(8), Option(64), 64, 40) == 40)
    assert(bound(4, 0, 0, None, None, Option(32), 8, 0) == 32)
    assert(bound(4, 0, 0, None, None, None, 8, 0) == LightGBMUtils.MinStreamingOmpThreads)
    assert(bound(4, 0, 0, None, None, None, -1, 0) == LightGBMUtils.MinStreamingOmpThreads)
    assert(bound(4, 0, 0, Option("8"), None, Option(64), 64, 0) == LightGBMUtils.MinStreamingOmpThreads)
    assert(bound(4, 0, 0, None, Option(8), Option(64), 64, 0) == LightGBMUtils.MinStreamingOmpThreads)
    assert(bound(4, 0, 0, None, None, Option(8), 48, 0) == 48)
    assert(bound(4, 0, 0, Option("8,"), Option(48), None, 8, 0) == 48)
    assert(warnings.size == 4)
    assertThrows[IllegalArgumentException](bound(4, Int.MaxValue, 2, Option("8"), None, None, 8, 0))
    assertThrows[IllegalArgumentException](bound(0, 16, 2, Option("8"), None, None, 8, 0))
  }

  test("streaming OpenMP helpers parse environment and affinity inputs") {
    assert(LightGBMUtils.firstOmpTeamSize(Option("32,8,4")) == Option(32))
    assert(LightGBMUtils.firstOmpTeamSize(Option(" +32, 8 ")) == Option(32))
    Seq("", "8,", "8,0", "8,x", "8,-1", "8,,2").foreach { value =>
      assert(LightGBMUtils.firstOmpTeamSize(Option(value)).isEmpty)
    }
    assert(LightGBMUtils.firstOmpTeamSize(Option("0,32")).isEmpty)
    assert(LightGBMUtils.firstOmpTeamSize(Option("invalid,32")).isEmpty)
    assert(LightGBMUtils.parseCpuAffinityList("0-3,8,10-11") == Option(7))
    assert(LightGBMUtils.parseCpuAffinityList("7") == Option(1))
    assert(LightGBMUtils.parseCpuAffinityList("3-1").isEmpty)
    assert(LightGBMUtils.parseCpuAffinityList("0-3,bad").isEmpty)
    assert(LightGBMUtils.osReportedProcessorCount("Windows 11", Option("32"), None) == Option(32))
    assert(LightGBMUtils.osReportedProcessorCount("Mac OS X", None, Option("24")) == Option(24))
    assert(LightGBMUtils.osReportedProcessorCount("Linux", Option("64"), Option("64")).isEmpty)
    assert(LightGBMUtils.osReportedProcessorCount("Windows 11", Option("invalid"), None).isEmpty)
  }

  test("streaming allocation bounds dynamic teams and evaluates host probes only when needed") {
    def unusedProbe: Option[Int] = fail("Unused processor probe was evaluated")
    assert(LightGBMUtils.streamingOmpAllocationBound(
      1, 16, 0, None, unusedProbe, unusedProbe, 8, 0, _ => ()) == -1)
    assert(LightGBMUtils.streamingOmpAllocationBound(
      4, 16, 1, Option("32"), unusedProbe, unusedProbe, 8, 0, _ => ()) == 32)
    assert(LightGBMUtils.streamingOmpAllocationBound(
      1, 16, 0, Option("32"), unusedProbe, unusedProbe, 8, 0, _ => (), dynamicThreads = true) == 32)
  }

  test("Linux affinity reader handles valid, missing and malformed status files") {
    val directory = Files.createTempDirectory("streaming-affinity-")
    val status = directory.resolve("status")
    try {
      assert(LightGBMUtils.linuxProcessAffinityCount(status).isEmpty)
      Files.write(status, "Name:\tprobe\nCpus_allowed_list:\t0-3,8\n".getBytes(StandardCharsets.UTF_8))
      assert(LightGBMUtils.linuxProcessAffinityCount(status).contains(5))
      Files.write(status, "Name:\tprobe\n".getBytes(StandardCharsets.UTF_8))
      assert(LightGBMUtils.linuxProcessAffinityCount(status).isEmpty)
      Files.write(status, "Cpus_allowed_list:\t0-3,bad\n".getBytes(StandardCharsets.UTF_8))
      assert(LightGBMUtils.linuxProcessAffinityCount(status).isEmpty)
    } finally {
      Files.deleteIfExists(status)
      Files.delete(directory)
    }
  }

  test("OS processor probe handles output, failure and timeout", TestBase.LinuxOnly) {
    assert(LightGBMUtils.firstCommandOutput(Seq("/bin/sh", "-c", "printf '24\\n'")).contains("24"))
    assert(LightGBMUtils.firstCommandOutput(Seq("/bin/sh", "-c", "exit 7")).isEmpty)
    assert(LightGBMUtils.firstCommandOutput(Seq("/bin/sh", "-c", "exec sleep 30"), timeoutMillis = 100).isEmpty)
  }

  test("native OpenMP registry covers the six configured call sites monotonically") {
    val registry = new NativeOmpThreadRegistry
    assert(NativeOmpCallSite.Values.map(_.name).distinct.size == 6)
    NativeOmpCallSite.Values.zipWithIndex.foreach { case (site, index) =>
      assert(registry.register(site, s"verbosity=1 num_threads=${index + 2}") == index + 2)
    }
    assert(registry.current == 7)
    assert(registry.register(NativeOmpCallSite.BoosterCreate, "num_threads=3") == 7)
    assert(registry.register(NativeOmpCallSite.DenseDataset, "num_threads=invalid") == 7)
    assert(registry.register(NativeOmpCallSite.SparseDataset, "num_threads=-1") == 7)
    NativeOmpCallSite.Values.zipWithIndex.foreach { case (site, index) =>
      assert(registry.current(site) == index + 2)
    }
  }

  test("native OpenMP registry treats LightGBM thread aliases as safety bounds") {
    val aliases = Seq("num_threads", "num_thread", "nthread", "nthreads", "n_jobs")
    aliases.zipWithIndex.foreach { case (name, index) =>
      val registry = new NativeOmpThreadRegistry
      assert(registry.register(
        NativeOmpCallSite.BoosterResetParameter,
        s"$name=${index + 2}") == index + 2)
    }

    val canonicalAndAlias = new NativeOmpThreadRegistry
    assert(canonicalAndAlias.register(
      NativeOmpCallSite.BoosterResetParameter,
      "num_threads=2 n_jobs=32") == 32)

    val twoAliases = new NativeOmpThreadRegistry
    assert(twoAliases.register(
      NativeOmpCallSite.BoosterResetParameter,
      "nthread=7 num_thread=19") == 19)

    val repeatedKey = new NativeOmpThreadRegistry
    assert(repeatedKey.register(
      NativeOmpCallSite.BoosterResetParameter,
      "n_jobs=11 n_jobs=29") == 11)

    val ignoredValues = new NativeOmpThreadRegistry
    assert(ignoredValues.register(
      NativeOmpCallSite.BoosterResetParameter,
      "num_threads=invalid n_jobs=0 nthread=-1") == 0)
    Seq("2147483648", "4294967360", "-2147483649").foreach { value =>
      val error = intercept[IllegalArgumentException] {
        ignoredValues.register(NativeOmpCallSite.BoosterResetParameter, s"n_jobs=$value")
      }
      assert(error.getMessage.contains("32-bit integer"))
      assert(ignoredValues.current == 0)
    }
    Seq("2147483648", "+4294967360", "8,4294967360").foreach { value =>
      assertThrows[IllegalArgumentException](LightGBMUtils.firstOmpTeamSize(Some(value)))
    }
    assert(LightGBMUtils.positiveNumThreads(s"num_threads=${Int.MaxValue}").contains(Int.MaxValue))
    assert(LightGBMUtils.positiveNumThreads(s"num_threads=${Int.MinValue}").isEmpty)
  }

  test("production native call paths register their OpenMP thread counts") {
    import spark.implicits._

    def nextThreadCount(sites: NativeOmpCallSite*): Int =
      math.max(2, sites.map(LightGBMUtils.nativeOmpThreadHighWaterMark).max + 1)

    def fit(mode: String,
            matrixType: String,
            threadCount: Int,
            numTasks: Int = 1,
            delegate: Option[LightGBMDelegate] = None): Unit = {
      val rows = (0 until 64).map { index =>
        val label = (index % 2).toDouble
        val features = if (matrixType == "sparse") {
          Vectors.sparse(3, Array(0, 2), Array(index % 7, label))
        } else {
          Vectors.dense(index % 7, (index * 3) % 11, label)
        }
        (label, features)
      }
      val data = rows.toDF(labelCol, featuresCol).repartition(numTasks).cache()
      try {
        val estimator = new LightGBMClassifier()
          .setLabelCol(labelCol)
          .setFeaturesCol(featuresCol)
          .setDataTransferMode(mode)
          .setMatrixType(matrixType)
          .setUseSingleDatasetMode(true)
          .setNumTasks(numTasks)
          .setNumThreads(threadCount)
          .setNumLeaves(3)
          .setNumIterations(1)
          .setDefaultListenPort(getAndIncrementPort())
        delegate.foreach(estimator.setDelegate)
        val model = estimator.fit(data)
        try {
          val registered = LightGBMUtils.nativeOmpThreadHighWaterMark
          assertThrows[IllegalArgumentException](model.getModel.resetParameter("n_jobs=4294967360"))
          assert(LightGBMUtils.nativeOmpThreadHighWaterMark == registered)
        } finally {
          model.getModel.freeNativeMemory()
        }
      } finally {
        data.unpersist()
      }
    }

    val streamingSites = Seq(
      NativeOmpCallSite.SampledColumnDataset,
      NativeOmpCallSite.SerializedReferenceDataset,
      NativeOmpCallSite.BoosterCreate)
    // Keep shared-JVM history small; the separate child-JVM regression covers wider teams.
    val streamingThreads = nextThreadCount(streamingSites: _*)
    fit(LightGBMConstants.StreamingDataTransferMode, "dense", streamingThreads, numTasks = numPartitions)
    streamingSites.foreach(site => assert(LightGBMUtils.nativeOmpThreadHighWaterMark(site) == streamingThreads))

    val denseSites = Seq(
      NativeOmpCallSite.DenseDataset,
      NativeOmpCallSite.BoosterCreate,
      NativeOmpCallSite.BoosterResetParameter)
    val denseThreads = nextThreadCount(denseSites: _*)
    fit(
      LightGBMConstants.BulkDataTransferMode,
      "dense",
      denseThreads,
      delegate = Option(new NativeOmpResetDelegate(denseThreads, "n_jobs")))
    denseSites.foreach(site => assert(LightGBMUtils.nativeOmpThreadHighWaterMark(site) == denseThreads))

    val sparseThreads = nextThreadCount(NativeOmpCallSite.SparseDataset, NativeOmpCallSite.BoosterCreate)
    fit(LightGBMConstants.BulkDataTransferMode, "sparse", sparseThreads)
    assert(LightGBMUtils.nativeOmpThreadHighWaterMark(NativeOmpCallSite.SparseDataset) == sparseThreads)
    assert(LightGBMUtils.nativeOmpThreadHighWaterMark(NativeOmpCallSite.BoosterCreate) == sparseThreads)
  }
}
