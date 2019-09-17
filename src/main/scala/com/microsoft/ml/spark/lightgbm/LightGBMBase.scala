// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.lightgbm

import com.microsoft.ml.spark.core.utils.ClusterUtil
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoders}

import scala.concurrent.Await
import scala.concurrent.duration.{Duration, SECONDS}
import scala.math.min
import org.apache.spark.ml.param.shared.{HasFeaturesCol => HasFeaturesColSpark, HasLabelCol => HasLabelColSpark}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType, IntegerType, NumericType, StringType}
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType

import scala.collection.mutable.ListBuffer
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
      val datasets = dataset.randomSplit((0 until getNumBatches).map(_ => ratio).toArray)
      datasets.foldLeft(None: Option[TrainedModel]) { (model, dataset) =>
        if (model.isDefined) {
          setModelString(stringFromTrainedModel(model.get))
        }
        Some(innerTrain(dataset))
      }.get
    } else {
      innerTrain(dataset)
    }
  }

  protected def castColumns(dataset: Dataset[_], trainingCols: Array[(String, DataType)]): DataFrame = {
    val schema = dataset.schema
    // Cast columns to correct types
    dataset.select(
      trainingCols.map {
        case (name, datatype) => {
          val index = schema.fieldIndex(name)
          // Note: We only want to cast if original column was of numeric type
          schema.fields(index).dataType match {
            case numericDataType: NumericType =>
              if (numericDataType != datatype) dataset(name).cast(datatype)
              else dataset(name)
            case _ => dataset(name)
          }
        }
      }: _*
    ).toDF()
  }

  protected def prepareDataframe(dataset: Dataset[_], trainingCols: Array[(String, DataType)],
                                 numWorkers: Int): DataFrame = {
    val df = castColumns(dataset, trainingCols)
    // Reduce number of partitions to number of executor cores
    /* Note: with barrier execution mode we must use repartition instead of coalesce when
     * running on spark standalone.
     * Using coalesce, we get the error:
     *
     * org.apache.spark.scheduler.BarrierJobUnsupportedRDDChainException:
     * [SPARK-24820][SPARK-24821]: Barrier execution mode does not allow the following
     * pattern of RDD chain within a barrier stage:
     * 1. Ancestor RDDs that have different number of partitions from the resulting
     * RDD (eg. union()/coalesce()/first()/take()/PartitionPruningRDD). A workaround
     * for first()/take() can be barrierRdd.collect().head (scala) or barrierRdd.collect()[0] (python).
     * 2. An RDD that depends on multiple barrier RDDs (eg. barrierRdd1.zip(barrierRdd2)).
     *
     * Without repartition, we may hit the error:
     * org.apache.spark.scheduler.BarrierJobSlotsNumberCheckFailed: [SPARK-24819]:
     * Barrier execution mode does not allow run a barrier stage that requires more
     * slots than the total number of slots in the cluster currently. Please init a
     * new cluster with more CPU cores or repartition the input RDD(s) to reduce the
     * number of slots required to run this barrier stage.
     *
     * Hence we still need to estimate the number of workers and repartition even when using
     * barrier execution, which is unfortunate as repartiton is more expensive than coalesce.
     */
    if (getUseBarrierExecutionMode) {
      val numPartitions = df.rdd.getNumPartitions
      if (numPartitions > numWorkers) {
        df.repartition(numWorkers)
      } else {
        df
      }
    } else {
      df.coalesce(numWorkers)
    }
  }

  protected def getTrainingCols(): Array[(String, DataType)] = {
    val colsToCheck = Array((Some(getLabelCol), DoubleType), (Some(getFeaturesCol), VectorType),
      (get(weightCol), DoubleType),
      (getOptGroupCol, IntegerType), (get(validationIndicatorCol), BooleanType),
      (get(initScoreCol), DoubleType))
    colsToCheck.flatMap { case (col: Option[String], colType: DataType) => {
      if (col.isDefined) Some(col.get, colType) else None
    }}
  }

  protected def innerTrain(dataset: Dataset[_]): TrainedModel = {
    val sc = dataset.sparkSession.sparkContext
    val numCoresPerExec = ClusterUtil.getNumCoresPerExecutor(dataset, log)
    val numExecutorCores = ClusterUtil.getNumExecutorCores(dataset, numCoresPerExec, log)
    val numWorkers = min(numExecutorCores, dataset.rdd.getNumPartitions)
    // Only get the relevant columns
    val trainingCols = getTrainingCols()

    val df = prepareDataframe(dataset, trainingCols, numWorkers)

    val (inetAddress, port, future) =
      LightGBMUtils.createDriverNodesThread(numWorkers, df, log, getTimeout, getUseBarrierExecutionMode)

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
    val networkParams = NetworkParams(getDefaultListenPort, inetAddress, port, getUseBarrierExecutionMode)
    val validationData =
      if (get(validationIndicatorCol).isDefined && dataset.columns.contains(getValidationIndicatorCol))
        Some(sc.broadcast(df.filter(x => x.getBoolean(x.fieldIndex(getValidationIndicatorCol))).collect()))
      else None
    val preprocessedDF = preprocessData(df)
    val schema = preprocessedDF.schema
    val mapPartitionsFunc = TrainUtils.trainLightGBM(networkParams, getLabelCol, getFeaturesCol,
      get(weightCol), get(initScoreCol), getOptGroupCol, validationData, log, trainParams, numCoresPerExec, schema)(_)
    val lightGBMBooster =
      if (getUseBarrierExecutionMode) {
        preprocessedDF.rdd.barrier().mapPartitions(mapPartitionsFunc).reduce((booster1, _) => booster1)
      } else {
        preprocessedDF.mapPartitions(mapPartitionsFunc)(encoder).reduce((booster1, _) => booster1)
      }
    // Wait for future to complete (should be done by now)
    Await.result(future, Duration(getTimeout, SECONDS))
    getModel(trainParams, lightGBMBooster)
  }

  /** Optional group column for Ranking, set to None by default.
    *
    * @return None
    */
  protected def getOptGroupCol: Option[String] = None

  /** Gets the trained model given the train parameters and booster.
    *
    * @return trained model.
    */
  protected def getModel(trainParams: TrainParams, lightGBMBooster: LightGBMBooster): TrainedModel

  /** Gets the training parameters.
    *
    * @return train parameters.
    */
  protected def getTrainParams(numWorkers: Int, categoricalIndexes: Array[Int], dataset: Dataset[_]): TrainParams

  protected def stringFromTrainedModel(model: TrainedModel): String

  /** Allow algorithm specific preprocessing of dataset.
    *
    * @param dataset The dataset to preprocess prior to training.
    * @return The preprocessed data.
    */
  protected def preprocessData(dataset: DataFrame): DataFrame = dataset
}
