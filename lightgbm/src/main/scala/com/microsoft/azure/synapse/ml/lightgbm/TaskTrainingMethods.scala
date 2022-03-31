// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm

import com.microsoft.azure.synapse.ml.lightgbm.dataset.BaseAggregatedColumns
import com.microsoft.azure.synapse.ml.lightgbm.params.BaseTrainParams
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row
import org.slf4j.Logger

object TaskTrainingMethods {
  /** If using single dataset mode, only returns one task in JVM.
    * Otherwise, returns true for all tasks.
    * @param trainParams The training parameters.
    * @param log The logger.
    * @param sharedState The shared state.
    * @return Whether the current task is enabled.
    */
  def isWorkerEnabled(trainParams: BaseTrainParams, log: Logger, sharedState: SharedState): Boolean = {
    if (trainParams.executionParams.useSingleDatasetMode) {
      // Find all workers in current JVM
      val isMainWorker = isCurrentTaskMainWorker(log, sharedState)
      sharedState.incrementArrayProcessedSignal(log)
      if (!isMainWorker) {
        sharedState.incrementDoneSignal(log)
      }
      isMainWorker
    } else {
      true
    }
  }

  /** Determines if the current task is the main worker in the current JVM.
    *
    * @param log The logger.
    * @param sharedState The shared state.
    * @return True if the current task in the main worker, false otherwise.
    */
  def isCurrentTaskMainWorker(log: Logger, sharedState: SharedState): Boolean = {
    val mainExecutorWorker = sharedState.mainExecutorWorker.get
    val myTaskId = LightGBMUtils.getTaskId
    val isMainWorker = mainExecutorWorker == myTaskId
    log.info(s"Using singleDatasetMode.  " +
      s"Is main worker: ${isMainWorker} for task id: ${myTaskId} and main task id: ${mainExecutorWorker}")
    isMainWorker
  }

  def prepareDatasets(inputRows: Iterator[Row],
                      validationData: Option[Broadcast[Array[Row]]],
                      sharedState: SharedState): (BaseAggregatedColumns, Option[BaseAggregatedColumns]) = {
    val aggregatedColumns = {
      val prepAggregatedColumns = sharedState.datasetState.prep(inputRows)
      sharedState.datasetState.merge(prepAggregatedColumns)
    }
    val aggregatedValidationColumns = validationData.map { data =>
      val prepAggregatedColumns = sharedState.validationDatasetState.prep(data.value.toIterator)
      sharedState.validationDatasetState.merge(prepAggregatedColumns)
    }
    (aggregatedColumns, aggregatedValidationColumns)
  }


}
