// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.ml.classification.{ProbabilisticClassificationModel, ProbabilisticClassifier}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.sql._

import scala.concurrent.Await
import scala.concurrent.duration.{Duration, SECONDS}
import scala.reflect.runtime.universe.{TypeTag, typeTag}

object LightGBMClassifier extends DefaultParamsReadable[LightGBMClassifier]

/** Trains a LightGBM Binary Classification model, a fast, distributed, high performance gradient boosting
  * framework based on decision tree algorithms.
  * For more information please see here: https://github.com/Microsoft/LightGBM.
  * For parameter information see here: https://github.com/Microsoft/LightGBM/blob/master/docs/Parameters.rst
  * @param uid The unique ID.
  */
@InternalWrapper
class LightGBMClassifier(override val uid: String)
  extends ProbabilisticClassifier[Vector, LightGBMClassifier, LightGBMClassificationModel]
  with LightGBMParams {
  def this() = this(Identifiable.randomUID("LightGBMClassifier"))

  /** Trains the LightGBM Classification model.
    *
    * @param dataset The input dataset to train.
    * @return The trained model.
    */
  override protected def train(dataset: Dataset[_]): LightGBMClassificationModel = {
    val numCoresPerExec = LightGBMUtils.getNumCoresPerExecutor(dataset)
    val numExecutorCores = LightGBMUtils.getNumExecutorCores(dataset, numCoresPerExec)
    // Reduce number of partitions to number of executor cores
    val df = dataset.toDF().coalesce(numExecutorCores).cache()
    val (inetAddress, port, future) =
      LightGBMUtils.createDriverNodesThread(numExecutorCores, df, log, getTimeout)

    val nodes = LightGBMUtils.getNodes(df, getDefaultListenPort, numCoresPerExec)
    /* Run a parallel job via map partitions to initialize the native library and network,
     * translate the data to the LightGBM in-memory representation and train the models
     */
    val encoder = Encoders.kryo[LightGBMBooster]
    log.info(s"Nodes used for LightGBM: ${nodes.mkString(",")}")
    val trainParams = ClassifierTrainParams(getParallelism, getNumIterations, getLearningRate, getNumLeaves,
      getMaxBin, getBaggingFraction, getBaggingFreq, getBaggingSeed, getFeatureFraction,
      getMaxDepth, getMinSumHessianInLeaf, numExecutorCores)
    val networkParams = NetworkParams(nodes.toMap, getDefaultListenPort, inetAddress, port)
    val lightGBMBooster = df
      .mapPartitions(TrainUtils.trainLightGBM(networkParams, getLabelCol, getFeaturesCol,
        log, trainParams, numCoresPerExec))(encoder)
      .reduce((booster1, booster2) => booster1)
    // Wait for future to complete (should be done by now)
    Await.result(future, Duration(getTimeout, SECONDS))
    new LightGBMClassificationModel(uid, lightGBMBooster, getLabelCol, getFeaturesCol,
      getPredictionCol, getProbabilityCol, getRawPredictionCol,
      if (isDefined(thresholds)) Some(getThresholds) else None)
  }

  override def copy(extra: ParamMap): LightGBMClassifier = defaultCopy(extra)
}

/** Model produced by [[LightGBMClassifier]]. */
@InternalWrapper
class LightGBMClassificationModel(
  override val uid: String, model: LightGBMBooster, labelColName: String,
  featuresColName: String, predictionColName: String, probColName: String,
  rawPredictionColName: String, thresholdValues: Option[Array[Double]])
    extends ProbabilisticClassificationModel[Vector, LightGBMClassificationModel]
    with ConstructorWritable[LightGBMClassificationModel] {

  // Update the underlying Spark ML params
  // (for proper serialization to work we put them on constructor instead of using copy as in Spark ML)
  set(labelCol, labelColName)
  set(featuresCol, featuresColName)
  set(predictionCol, predictionColName)
  set(probabilityCol, probColName)
  set(rawPredictionCol, rawPredictionColName)
  if (thresholdValues.isDefined) set(thresholds, thresholdValues.get)

  override protected def raw2probabilityInPlace(rawPrediction: Vector): Vector = {
    rawPrediction match {
      case dv: DenseVector =>
        dv.values(0) = 1.0 / (1.0 + math.exp(-2.0 * dv.values(0)))
        dv.values(1) = 1.0 - dv.values(0)
        dv
      case sv: SparseVector =>
        throw new RuntimeException("Unexpected error in LightGBMClassificationModel:" +
          " raw2probabilityInPlace encountered SparseVector")
    }
  }

  override def numClasses: Int = model.numClasses()

  override protected def predictRaw(features: Vector): Vector = {
    val prediction = model.score(features, true)
    Vectors.dense(Array(-prediction, prediction))
  }

  override def copy(extra: ParamMap): LightGBMClassificationModel =
    defaultCopy(extra)

  override val ttag: TypeTag[LightGBMClassificationModel] =
    typeTag[LightGBMClassificationModel]

  override def objectsToSave: List[Any] =
    List(uid, model, getLabelCol, getFeaturesCol, getPredictionCol,
         getProbabilityCol, getRawPredictionCol, thresholdValues)

  def saveNativeModel(session: SparkSession, filename: String): Unit = {
    model.saveNativeModel(session, filename)
  }
}

object LightGBMClassificationModel extends ConstructorReadable[LightGBMClassificationModel]
