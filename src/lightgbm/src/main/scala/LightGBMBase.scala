// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders}

import scala.concurrent.Await
import scala.concurrent.duration.{Duration, SECONDS}
import scala.math.min
import org.apache.spark.ml.param.shared.{HasFeaturesCol => HasFeaturesColSpark, HasLabelCol => HasLabelColSpark}

import scala.language.existentials

trait LightGBMBase[TrainedModel <: Model[TrainedModel]] extends Estimator[TrainedModel]
  with LightGBMParams with HasFeaturesColSpark with HasLabelColSpark {

  /** Trains the LightGBM model.
    *
    * @param dataset The input dataset to train.
    * @return The trained model.
    */
  protected def train(dataset: Dataset[_]): TrainedModel = {
    if (getNumBatches > 0) {
      val ratio = 1.0 / getNumBatches
      val datasets = dataset.randomSplit((0 until getNumBatches).map(_=> ratio).toArray)
      datasets.foldLeft(None: Option[TrainedModel]) { (model, dataset) =>
        if (!model.isEmpty) {
          setModelString(stringFromTrainedModel(model.get))
        }
        Some(innerTrain(dataset))
      }.get
    } else {
      innerTrain(dataset)
    }
  }

  protected def innerTrain(dataset: Dataset[_]): TrainedModel = {
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
    val lightGBMBooster = preprocessData(df)
      .mapPartitions(TrainUtils.trainLightGBM(networkParams, getLabelCol, getFeaturesCol, get(weightCol),
        get(initScoreCol), getOptGroupCol, validationData, log, trainParams, numCoresPerExec))(encoder)
      .reduce((booster1, _) => booster1)
    // Wait for future to complete (should be done by now)
    Await.result(future, Duration(getTimeout, SECONDS))
    getModel(trainParams, lightGBMBooster)
  }

  /** Optional group column for Ranking, set to None by default.
    * @return None
    */
  protected def getOptGroupCol: Option[String] = None

  /** Gets the trained model given the train parameters and booster.
    * @return trained model.
    */
  protected def getModel(trainParams: TrainParams, lightGBMBooster: LightGBMBooster): TrainedModel

  /** Gets the training parameters.
    * @return train parameters.
    */
  protected def getTrainParams(numWorkers: Int, categoricalIndexes: Array[Int], dataset: Dataset[_]): TrainParams

  protected def stringFromTrainedModel(model: TrainedModel): String

  /** Allow algorithm specific preprocessing of dataset.
    * @param dataset The dataset to preprocess prior to training.
    * @return The preprocessed data.
    */
  protected def preprocessData(dataset: DataFrame): DataFrame = dataset
}
