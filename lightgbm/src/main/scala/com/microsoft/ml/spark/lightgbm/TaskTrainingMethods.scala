// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.lightgbm

import com.microsoft.ml.lightgbm.lightgbmlib
import com.microsoft.ml.spark.lightgbm.TrainUtils.{afterGenerateTrainDataset, afterGenerateValidDataset,
  beforeGenerateTrainDataset, beforeGenerateValidDataset, createBooster, getNetworkInfo, getReturnBooster,
  networkInit, trainCore}
import com.microsoft.ml.spark.lightgbm.booster.LightGBMBooster
import com.microsoft.ml.spark.lightgbm.dataset.{BaseAggregatedColumns, DatasetUtils, LightGBMDataset}
import com.microsoft.ml.spark.lightgbm.params.TrainParams
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
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
      val myTaskId = LightGBMUtils.getTaskId()
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
                      columnParams: ColumnParams,
                      schema: StructType,
                      trainParams: TrainParams,
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

  def trainLightGBM(batchIndex: Int, networkParams: NetworkParams, columnParams: ColumnParams,
                    validationData: Option[Broadcast[Array[Row]]], log: Logger,
                    trainParams: TrainParams, numTasksPerExec: Int, schema: StructType,
                    sharedState: SharedState)
                   (inputRows: Iterator[Row]): Iterator[LightGBMBooster] = {
    val useSingleDatasetMode = trainParams.executionParams.useSingleDatasetMode
    val emptyPartition = !inputRows.hasNext
    val isEnabledWorker = if (!emptyPartition) isWorkerEnabled(trainParams, log, sharedState) else false
    // Initialize the native library
    LightGBMUtils.initializeNativeLibrary()
    // Initialize the network communication
    val (nodes, localListenPort) = getNetworkInfo(networkParams, numTasksPerExec, log, isEnabledWorker)
    if (emptyPartition) {
      log.warn("LightGBM task encountered empty partition, for best performance ensure no partitions empty")
      List[LightGBMBooster]().toIterator
    } else {
      if (isEnabledWorker) {
        log.info(s"LightGBM task listening on: $localListenPort")
        if (useSingleDatasetMode) sharedState.helperStartSignal.countDown()
      } else {
        sharedState.helperStartSignal.await()
      }
      val (aggregatedColumns, aggregatedValidationColumns) = prepareDatasets(inputRows,
        validationData, columnParams, schema, trainParams, sharedState)
      // Return booster only from main worker to reduce network communication overhead
      val returnBooster = getReturnBooster(isEnabledWorker, nodes, log, numTasksPerExec, localListenPort)
      try {
        if (isEnabledWorker) {
          // If worker enabled, initialize the network ring of communication
          networkInit(nodes, localListenPort, log, LightGBMConstants.NetworkRetries, LightGBMConstants.InitialDelay)
          if (useSingleDatasetMode) sharedState.doneSignal.await()
          translate(batchIndex, columnParams, aggregatedValidationColumns, log, trainParams, returnBooster,
            sharedState.isSparse.get, schema, aggregatedColumns)
        } else {
          log.info("Helper task finished processing rows")
          sharedState.doneSignal.countDown()
          List[LightGBMBooster]().toIterator
        }
      } finally {
        // Finalize network when done
        if (isEnabledWorker) LightGBMUtils.validate(lightgbmlib.LGBM_NetworkFree(), "Finalize network")
      }
    }
  }

  def translate(batchIndex: Int, columnParams: ColumnParams, validationData: Option[BaseAggregatedColumns],
                log: Logger, trainParams: TrainParams, returnBooster: Boolean, isSparse: Boolean, schema: StructType,
                aggregatedColumns: BaseAggregatedColumns): Iterator[LightGBMBooster] = {
    var trainDatasetOpt: Option[LightGBMDataset] = None
    var validDatasetOpt: Option[LightGBMDataset] = None
    try {
      beforeGenerateTrainDataset(batchIndex, columnParams, schema, log, trainParams)
      trainDatasetOpt = DatasetUtils.generateDataset(aggregatedColumns, columnParams, None, isSparse, schema,
        log, trainParams)
      afterGenerateTrainDataset(batchIndex, columnParams, schema, log, trainParams)

      if (validationData.isDefined) {
        beforeGenerateValidDataset(batchIndex, columnParams, schema, log, trainParams)
        validDatasetOpt = DatasetUtils.generateDataset(validationData.get, columnParams,
          trainDatasetOpt, isSparse, schema, log, trainParams)
        afterGenerateValidDataset(batchIndex, columnParams, schema, log, trainParams)
      }

      var boosterOpt: Option[LightGBMBooster] = None
      try {
        val booster = createBooster(trainParams, trainDatasetOpt.get, validDatasetOpt)
        boosterOpt = Some(booster)
        val bestIterResult = trainCore(batchIndex, trainParams, booster, log, validDatasetOpt.isDefined)
        if (returnBooster) {
          val model = booster.saveToString()
          val modelBooster = new LightGBMBooster(model)
          // Set best iteration on booster if hit early stopping criteria in trainCore
          bestIterResult.foreach(modelBooster.setBestIteration(_))
          Iterator.single(modelBooster)
        } else {
          Iterator.empty
        }
      } finally {
        // Free booster
        boosterOpt.foreach(_.freeNativeMemory())
      }
    } finally {
      // Free datasets
      trainDatasetOpt.foreach(_.close())
      validDatasetOpt.foreach(_.close())
    }
  }
}
