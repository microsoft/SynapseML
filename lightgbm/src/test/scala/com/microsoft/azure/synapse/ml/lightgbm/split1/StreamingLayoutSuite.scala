// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split1

import com.microsoft.azure.synapse.ml.io.http.SharedSingleton
import com.microsoft.azure.synapse.ml.lightgbm._
import com.microsoft.azure.synapse.ml.lightgbm.booster.LightGBMBooster
import com.microsoft.azure.synapse.ml.lightgbm.dataset.ReferenceDatasetUtils
import com.microsoft.azure.synapse.ml.lightgbm.params.BaseTrainParams
import org.apache.spark.ml.linalg.{SQLDataTypes, Vectors}
import org.apache.spark.sql.types.{StructField, StructType}
import org.slf4j.Logger

class NativeOmpResetDelegate(threadCount: Int) extends LightGBMDelegate {
  override def beforeTrainIteration(batchIndex: Int,
                                    partitionId: Int,
                                    curIters: Int,
                                    log: Logger,
                                    trainParams: BaseTrainParams,
                                    booster: LightGBMBooster,
                                    hasValid: Boolean): Unit = {
    if (curIters == 0) booster.resetParameter(s"num_threads=$threadCount")
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
    def bound(maxThreads: Int,
              numThreads: Int,
              ompThreads: Option[String],
              affinity: Option[Int],
              availableProcessors: Int,
              registered: Int): Int =
      ReferenceDatasetUtils.streamingOmpAllocationBound(
        maxThreads,
        numThreads,
        ompThreads,
        affinity,
        availableProcessors,
        registered,
        warnings += _)

    assert(bound(16, 32, Option("8"), Option(4), 64, 2) == 32)
    assert(bound(32, 16, Option("8"), Option(4), 64, 2) == 32)
    assert(bound(-1, 2, Option("32,8"), Option(4), 64, 0) == 32)
    assert(bound(0, 2, Option("invalid,64"), Option(24), 64, 0) == 24)
    assert(bound(0, 0, None, Option(8), 64, 40) == 40)
    assert(bound(0, 0, None, None, 48, 0) == 48)
    assert(bound(0, 0, None, None, 8, 0) == LightGBMUtils.MinStreamingOmpThreads)
    assert(bound(0, 0, None, None, -1, 0) == LightGBMUtils.MinStreamingOmpThreads)
    assert(warnings.size == 3)
  }

  test("streaming OpenMP helpers parse environment and affinity inputs") {
    assert(LightGBMUtils.firstOmpTeamSize(Option("32,8,4")) == Option(32))
    assert(LightGBMUtils.firstOmpTeamSize(Option("0,32")).isEmpty)
    assert(LightGBMUtils.firstOmpTeamSize(Option("invalid,32")).isEmpty)
    assert(LightGBMUtils.parseCpuAffinityList("0-3,8,10-11") == Option(7))
    assert(LightGBMUtils.parseCpuAffinityList("7") == Option(1))
    assert(LightGBMUtils.parseCpuAffinityList("3-1").isEmpty)
    assert(LightGBMUtils.parseCpuAffinityList("0-3,bad").isEmpty)
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

  test("production native call paths register their OpenMP thread counts") {
    import spark.implicits._

    def nextThreadCount(sites: NativeOmpCallSite*): Int =
      math.max(2, sites.map(LightGBMUtils.nativeOmpThreadHighWaterMark).max + 1)

    def fit(mode: String,
            matrixType: String,
            threadCount: Int,
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
      val data = rows.toDF(labelCol, featuresCol).repartition(1).cache()
      try {
        val estimator = new LightGBMClassifier()
          .setLabelCol(labelCol)
          .setFeaturesCol(featuresCol)
          .setDataTransferMode(mode)
          .setMatrixType(matrixType)
          .setUseSingleDatasetMode(true)
          .setNumTasks(1)
          .setNumThreads(threadCount)
          .setNumLeaves(3)
          .setNumIterations(1)
          .setDefaultListenPort(getAndIncrementPort())
        delegate.foreach(estimator.setDelegate)
        val model = estimator.fit(data)
        model.getModel.freeNativeMemory()
      } finally {
        data.unpersist()
      }
    }

    val streamingSites = Seq(
      NativeOmpCallSite.SampledColumnDataset,
      NativeOmpCallSite.SerializedReferenceDataset,
      NativeOmpCallSite.BoosterCreate)
    val streamingThreads = nextThreadCount(streamingSites: _*)
    fit(LightGBMConstants.StreamingDataTransferMode, "dense", streamingThreads)
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
      Option(new NativeOmpResetDelegate(denseThreads)))
    denseSites.foreach(site => assert(LightGBMUtils.nativeOmpThreadHighWaterMark(site) == denseThreads))

    val sparseThreads = nextThreadCount(NativeOmpCallSite.SparseDataset, NativeOmpCallSite.BoosterCreate)
    fit(LightGBMConstants.BulkDataTransferMode, "sparse", sparseThreads)
    assert(LightGBMUtils.nativeOmpThreadHighWaterMark(NativeOmpCallSite.SparseDataset) == sparseThreads)
    assert(LightGBMUtils.nativeOmpThreadHighWaterMark(NativeOmpCallSite.BoosterCreate) == sparseThreads)
  }
}
