// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.lightgbm

import com.microsoft.ml.spark.lightgbm.dataset.BaseAggregatedColumns
import com.microsoft.ml.spark.lightgbm.params.TrainParams
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row
import org.slf4j.Logger

object TaskTrainingMethods {
  /** If using single dataset mode, only returns one task in JVM.
    * Otherwise, returns true for all tasks.
    * @param trainParams The training parameters.
    * @param log The logger.
    * @return Whether the current task is enabled.
    */
  def isWorkerEnabled(trainParams: TrainParams, log: Logger, sharedState: SharedState): Boolean = {
    if (trainParams.executionParams.useSingleDatasetMode) {
      // Find all workers in current JVM
      val mainExecutorWorker = sharedState.mainExecutorWorker
      val myTaskId = LightGBMUtils.getTaskId
      val isMainWorker = mainExecutorWorker == myTaskId
      log.info(s"Using singleDatasetMode.  " +
        s"Is main worker: ${isMainWorker} for task id: ${myTaskId} and main task id: ${mainExecutorWorker}")
      sharedState.incrementArrayProcessedSignal(log)
      if (!isMainWorker) {
        sharedState.incrementDoneSignal(log)
      }
      isMainWorker
    } else {
      true
    }
  }

  def prepareDatasets(inputRows: Iterator[Row],
                      validationData: Option[Broadcast[Array[Row]]],
                      sharedState: SharedState): (BaseAggregatedColumns, Option[BaseAggregatedColumns]) = {
    val aggregatedColumns = {
      val prepAggregatedColumns = sharedState.prep(inputRows)
      sharedState.merge(prepAggregatedColumns)
    }

    val aggregatedValidationColumns = validationData.map { data =>
      val prepAggregatedColumns = sharedState.prep(data.value.toIterator)
      sharedState.merge(prepAggregatedColumns)
    }
    (aggregatedColumns, aggregatedValidationColumns)
  }


}
