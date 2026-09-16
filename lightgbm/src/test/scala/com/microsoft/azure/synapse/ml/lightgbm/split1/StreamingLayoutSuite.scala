// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split1

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.io.http.SharedSingleton
import com.microsoft.azure.synapse.ml.lightgbm._
import com.microsoft.azure.synapse.ml.lightgbm.dataset.ReferenceDatasetUtils
import org.apache.spark.ml.linalg.SQLDataTypes
import org.apache.spark.sql.types.{StructField, StructType}

class StreamingLayoutSuite extends TestBase {
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
    assert(ReferenceDatasetUtils.streamingOmpAllocationBound(16, 32) == 32)
    assert(ReferenceDatasetUtils.streamingOmpAllocationBound(32, 16) == 32)
    assert(ReferenceDatasetUtils.streamingOmpAllocationBound(-1, 32) == -1)
    assert(ReferenceDatasetUtils.streamingOmpAllocationBound(0, 32) == -1)
    assert(ReferenceDatasetUtils.streamingOmpAllocationBound(16, 0) == -1)
  }
}
