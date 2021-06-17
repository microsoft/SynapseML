// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.lightgbm

import com.microsoft.ml.lightgbm.lightgbmlib
import com.microsoft.ml.spark.lightgbm.TrainUtils.{afterGenerateTrainDataset, afterGenerateValidDataset,
  beforeGenerateTrainDataset, beforeGenerateValidDataset, createBooster, getNetworkInfo, getReturnBooster,
  networkInit, trainCore}
import com.microsoft.ml.spark.lightgbm.booster.LightGBMBooster
import com.microsoft.ml.spark.lightgbm.dataset.{DatasetUtils, LightGBMDataset}
import com.microsoft.ml.spark.lightgbm.params.TrainParams
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.slf4j.Logger

object PartitionProcessor {
  def trainLightGBM(batchIndex: Int, networkParams: NetworkParams, columnParams: ColumnParams,
                    validationData: Option[Broadcast[Array[Row]]], log: Logger,
                    trainParams: TrainParams, numTasksPerExec: Int, schema: StructType)
                   (inputRows: Iterator[Row]): Iterator[LightGBMBooster] = {
    val emptyPartition = !inputRows.hasNext
    val isEnabledWorker = !emptyPartition
    // Initialize the native library
    LightGBMUtils.initializeNativeLibrary()
    // Initialize the network communication
    val (nodes, localListenPort) = getNetworkInfo(networkParams, numTasksPerExec, log, isEnabledWorker)
    if (emptyPartition) {
      log.warn("LightGBM task encountered empty partition, for best performance ensure no partitions empty")
      List[LightGBMBooster]().toIterator
    } else {
      log.info(s"LightGBM task listening on: $localListenPort")
      // Return booster only from main worker to reduce network communication overhead
      val returnBooster = getReturnBooster(isEnabledWorker, nodes, log, numTasksPerExec, localListenPort)
      try {
        // If worker enabled, initialize the network ring of communication
        networkInit(nodes, localListenPort, log, LightGBMConstants.NetworkRetries, LightGBMConstants.InitialDelay)
        translate(batchIndex, columnParams, validationData, log, trainParams, returnBooster, schema, inputRows)
      } finally {
        // Finalize network when done
        if (isEnabledWorker) LightGBMUtils.validate(lightgbmlib.LGBM_NetworkFree(), "Finalize network")
      }
    }
  }

  def translate(batchIndex: Int, columnParams: ColumnParams, validationData: Option[Broadcast[Array[Row]]],
                log: Logger, trainParams: TrainParams, returnBooster: Boolean,
                schema: StructType, inputRows: Iterator[Row]): Iterator[LightGBMBooster] = {
    var trainDatasetOpt: Option[LightGBMDataset] = None
    var validDatasetOpt: Option[LightGBMDataset] = None
    try {
      beforeGenerateTrainDataset(batchIndex, columnParams, schema, log, trainParams)
      trainDatasetOpt = DatasetUtils.generateDataset(inputRows, columnParams, None, schema,
        log, trainParams)
      afterGenerateTrainDataset(batchIndex, columnParams, schema, log, trainParams)

      if (validationData.isDefined) {
        beforeGenerateValidDataset(batchIndex, columnParams, schema, log, trainParams)
        validDatasetOpt = DatasetUtils.generateDataset(validationData.get.value.toIterator, columnParams,
          trainDatasetOpt, schema, log, trainParams)
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
