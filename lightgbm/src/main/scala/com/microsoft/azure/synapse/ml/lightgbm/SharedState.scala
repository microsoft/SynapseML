// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm

import com.microsoft.azure.synapse.ml.lightgbm.dataset._
import com.microsoft.azure.synapse.ml.lightgbm.params.BaseTrainParams
import org.slf4j.Logger

import java.util.concurrent.CountDownLatch

class SharedDatasetState(trainParams: BaseTrainParams, isForValidation: Boolean) {
  val chunkSize: Int = trainParams.executionParams.chunkSize
  val useSingleDataset: Boolean = trainParams.executionParams.useSingleDatasetMode
  val matrixType: String = trainParams.executionParams.matrixType

  @volatile var streamingDataset: Option[LightGBMDataset] = None

  lazy val denseAggregatedColumns: BaseDenseAggregatedColumns = new DenseSyncAggregatedColumns(chunkSize)

  lazy val sparseAggregatedColumns: BaseSparseAggregatedColumns = new SparseSyncAggregatedColumns(chunkSize)

  @volatile var arrayProcessedSignal: CountDownLatch = new CountDownLatch(0)

  def incrementArrayProcessedSignal(log: Logger): Int = {
    this.synchronized {
      val count = arrayProcessedSignal.getCount.toInt + 1
      arrayProcessedSignal = new CountDownLatch(count)
      log.info(s"Task incrementing ArrayProcessedSignal to $count")
      count
    }
  }
}

class SharedState(trainParams: BaseTrainParams) {
  val datasetState: SharedDatasetState = new SharedDatasetState(trainParams, isForValidation = false)
  val validationDatasetState: SharedDatasetState = new SharedDatasetState(trainParams, isForValidation = true)

  @volatile var isSparse: Option[Boolean] = None
  @volatile var mainExecutorWorker: Option[Long] = None
  @volatile var validationDatasetWorker: Option[Long] = None

  def linkIsSparse(isSparse: Boolean): Unit = {
    if (this.isSparse.isEmpty) {
      this.synchronized {
        if (this.isSparse.isEmpty) {
          this.isSparse = Some(isSparse)
        }
      }
    }
  }

  def linkMainExecutorWorker(): Unit = {
    if (this.mainExecutorWorker.isEmpty) {
      this.synchronized {
        if (this.mainExecutorWorker.isEmpty) {
          this.mainExecutorWorker = Some(LightGBMUtils.getTaskId)
        }
      }
    }
  }

  def linkValidationDatasetWorker(): Unit = {
    if (this.validationDatasetWorker.isEmpty) {
      this.synchronized {
        if (this.validationDatasetWorker.isEmpty) {
          this.validationDatasetWorker = Some(LightGBMUtils.getTaskId)
        }
      }
    }
  }

  def incrementArrayProcessedSignal(log: Logger): Int = {
    datasetState.incrementArrayProcessedSignal(log)
    validationDatasetState.incrementArrayProcessedSignal(log)
  }

  @volatile var dataPreparationDoneSignal: CountDownLatch = new CountDownLatch(0)

  def incrementDataPrepDoneSignal(log: Logger): Unit = {
    this.synchronized {
      val count = dataPreparationDoneSignal.getCount.toInt + 1
      dataPreparationDoneSignal = new CountDownLatch(count)
      log.info(s"Task incrementing DataPrepDoneSignal to $count")
    }
  }

  @volatile var helperStartSignal: CountDownLatch = new CountDownLatch(1)
}
