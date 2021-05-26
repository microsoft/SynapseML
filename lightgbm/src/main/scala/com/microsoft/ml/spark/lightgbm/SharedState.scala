// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.lightgbm

import java.util.concurrent.CountDownLatch

import com.microsoft.ml.spark.lightgbm.dataset.{DatasetAggregator, DenseSyncAggregatedColumns,
  SparseSyncAggregatedColumns}
import com.microsoft.ml.spark.lightgbm.params.TrainParams
import org.apache.spark.sql.types.StructType
import org.slf4j.Logger

class SharedState(columnParams: ColumnParams,
                  schema: StructType,
                  trainParams: TrainParams) {
  var mainExecutorWorker: Long = LightGBMUtils.getTaskId()

  @volatile lazy val denseConsolidatorHolder = new DenseSyncAggregatedColumns(trainParams.executionParams.chunkSize)

  @volatile lazy val sparseConsolidatorHolder = new SparseSyncAggregatedColumns(trainParams.executionParams.chunkSize)

  @volatile var datasetAggregator: DatasetAggregator = new DatasetAggregator(columnParams, schema,
    trainParams.executionParams.useSingleDatasetMode, trainParams.executionParams.chunkSize,
    trainParams.executionParams.matrixType, this)

  @volatile var isSparse: Option[Boolean] = None
  def linkIsSparse(isSparse: Boolean): Unit = {
    if (this.isSparse.isEmpty) {
      this.synchronized {
        if (this.isSparse.isEmpty) {
          this.isSparse = Some(isSparse)
        }
      }
    }
  }

  @volatile var arrayProcessedSignal: CountDownLatch = new CountDownLatch(0)
  def incrementArrayProcessedSignal(log: Logger): Int = {
    this.synchronized {
      val count = arrayProcessedSignal.getCount().toInt + 1
      arrayProcessedSignal = new CountDownLatch(count)
      log.info(s"Task incrementing ArrayProcessedSignal to $count")
      count
    }
  }

  @volatile var doneSignal: CountDownLatch = new CountDownLatch(0)
  def incrementDoneSignal(log: Logger): Unit = {
    this.synchronized {
      val count = doneSignal.getCount().toInt + 1
      doneSignal = new CountDownLatch(count)
      log.info(s"Task incrementing DoneSignal to $count")
    }
  }

  @volatile var helperStartSignal: CountDownLatch = new CountDownLatch(1)
}
