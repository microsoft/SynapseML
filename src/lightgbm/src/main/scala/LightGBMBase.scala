// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.{Dataset, Encoders}

import scala.concurrent.Await
import scala.concurrent.duration.{Duration, SECONDS}
import scala.math.min
import org.apache.spark.ml.param.shared.{HasLabelCol => HasLabelColSpark, HasFeaturesCol => HasFeaturesColSpark}

trait LightGBMBase[TrainedModel <: Model[TrainedModel]] extends Estimator[TrainedModel]
  with LightGBMParams with HasFeaturesColSpark with HasLabelColSpark {

  /** Trains the LightGBM model.
    *
    * @param dataset The input dataset to train.
    * @return The trained model.
    */
  protected def train(dataset: Dataset[_]): TrainedModel = {
    val sc = dataset.sparkSession.sparkContext
    val numCoresPerExec = LightGBMUtils.getNumCoresPerExecutor(dataset)
    val numExecutorCores = LightGBMUtils.getNumExecutorCores(dataset, numCoresPerExec)
    val numWorkers = min(numExecutorCores, dataset.rdd.getNumPartitions)
    // Reduce number of partitions to number of executor cores
    val df = dataset.toDF().coalesce(numWorkers)
    val (inetAddress, port, future) =
      LightGBMUtils.createDriverNodesThread(numWorkers, df, log, getTimeout)

    /* Run a parallel job via map partitions to initialize the native library and network,
     * translate the data to the LightGBM in-memory representation and train the models
     */
    val encoder = Encoders.kryo[LightGBMBooster]

    val categoricalSlotIndexesArr = get(categoricalSlotIndexes).getOrElse(Array.empty[Int])
    val categoricalSlotNamesArr = get(categoricalSlotNames).getOrElse(Array.empty[String])
    val categoricalIndexes = LightGBMUtils.getCategoricalIndexes(df, getFeaturesCol,
      categoricalSlotIndexesArr, categoricalSlotNamesArr)
    val trainParams = getTrainParams(numWorkers, categoricalIndexes, dataset)
    log.info(s"LightGBM parameters: ${trainParams.toString()}")
    val networkParams = NetworkParams(getDefaultListenPort, inetAddress, port)
    val validationData =
      if (get(validationIndicatorCol).isDefined && dataset.columns.contains(getValidationIndicatorCol))
        Some(sc.broadcast(df.filter(x => x.getBoolean(x.fieldIndex(getValidationIndicatorCol))).collect()))
      else None
    val lightGBMBooster = df
      .mapPartitions(TrainUtils.trainLightGBM(networkParams, getLabelCol, getFeaturesCol, get(weightCol),
        validationData, log, trainParams, numCoresPerExec))(encoder)
      .reduce((booster1, _) => booster1)
    // Wait for future to complete (should be done by now)
    Await.result(future, Duration(getTimeout, SECONDS))
    getModel(trainParams, lightGBMBooster)
  }

  protected def getModel(trainParams: TrainParams, lightGBMBooster: LightGBMBooster): TrainedModel

  protected def getTrainParams(numWorkers: Int, categoricalIndexes: Array[Int], dataset: Dataset[_]): TrainParams
}
