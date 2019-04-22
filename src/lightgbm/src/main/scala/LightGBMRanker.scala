// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.ml.ranker.{Ranker, RankerModel}
import org.apache.spark.sql._

import scala.reflect.runtime.universe.{TypeTag, typeTag}

object LightGBMRanker extends DefaultParamsReadable[LightGBMRanker]

/** Trains a LightGBMRanker model, a fast, distributed, high performance gradient boosting
  * framework based on decision tree algorithms.
  * For more information please see here: https://github.com/Microsoft/LightGBM.
  * For parameter information see here: https://github.com/Microsoft/LightGBM/blob/master/docs/Parameters.rst
  * @param uid The unique ID.
  */
@InternalWrapper
class LightGBMRanker(override val uid: String)
  extends Ranker[Vector, LightGBMRanker, LightGBMRankerModel]
    with LightGBMBase[LightGBMRankerModel] {
  def this() = this(Identifiable.randomUID("LightGBMRanker"))

  // Set default objective to be ranking classification
  setDefault(objective -> LightGBMConstants.rankObjective)

  val maxPosition = new IntParam(this, "maxPosition", "optimized NDCG at this position")
  setDefault(maxPosition -> 20)

  def getMaxPosition: Int = $(maxPosition)
  def setMaxPosition(value: Int): this.type = set(maxPosition, value)

  val labelGain = new DoubleArrayParam(this, "labelGain", "parameter for Huber loss and Quantile regression")
  setDefault(labelGain -> Array.empty[Double])

  def getLabelGain: Array[Double] = $(labelGain)
  def setLabelGain(value: Array[Double]): this.type = set(labelGain, value)

  def getTrainParams(numWorkers: Int, categoricalIndexes: Array[Int], dataset: Dataset[_]): TrainParams = {
    val modelStr = if (getModelString == null || getModelString.isEmpty) None else get(modelString)
    RankerTrainParams(getParallelism, getNumIterations, getLearningRate, getNumLeaves,
      getObjective, getMaxBin, getBaggingFraction, getBaggingFreq, getBaggingSeed, getEarlyStoppingRound,
      getFeatureFraction, getMaxDepth, getMinSumHessianInLeaf, numWorkers, modelStr,
      getVerbosity, categoricalIndexes, getBoostingType, getLambdaL1, getLambdaL2, getMaxPosition, getLabelGain)
  }

  def getModel(trainParams: TrainParams, lightGBMBooster: LightGBMBooster): LightGBMRankerModel = {
    new LightGBMRankerModel(uid, lightGBMBooster, getLabelCol, getFeaturesCol, getPredictionCol)
  }

  def stringFromTrainedModel(model: LightGBMRankerModel): String = {
    model.getModel.model
  }

  override def getOptGroupCol: Option[String] = Some(getGroupCol)

  /** For Ranking, we need to sort the data within partitions by group prior to training to ensure training succeeds.
    * @param dataset The dataset to preprocess prior to training.
    * @return The preprocessed data, sorted within partiton by group.
    */
  override def preprocessData(dataset: DataFrame): DataFrame = {
    dataset.sortWithinPartitions(getOptGroupCol.get)
  }

  override def copy(extra: ParamMap): LightGBMRanker = defaultCopy(extra)
}

/** Model produced by [[LightGBMRanker]]. */
@InternalWrapper
class LightGBMRankerModel(override val uid: String, model: LightGBMBooster, labelColName: String,
                          featuresColName: String, predictionColName: String)
  extends RankerModel[Vector, LightGBMRankerModel]
    with ConstructorWritable[LightGBMRankerModel] {

  // Update the underlying Spark ML params
  // (for proper serialization to work we put them on constructor instead of using copy as in Spark ML)
  set(labelCol, labelColName)
  set(featuresCol, featuresColName)
  set(predictionCol, predictionColName)

  override def predict(features: Vector): Double = {
    model.score(features, false, false)(0)
  }

  override def copy(extra: ParamMap): LightGBMRankerModel =
    new LightGBMRankerModel(uid, model, labelColName, featuresColName, predictionColName)

  override val ttag: TypeTag[LightGBMRankerModel] =
    typeTag[LightGBMRankerModel]

  override def objectsToSave: List[Any] =
    List(uid, model, getLabelCol, getFeaturesCol, getPredictionCol)

  def saveNativeModel(filename: String, overwrite: Boolean): Unit = {
    val session = SparkSession.builder().getOrCreate()
    model.saveNativeModel(session, filename, overwrite)
  }

  def getFeatureImportances(importanceType: String): Array[Double] = {
    model.getFeatureImportances(importanceType)
  }

  def getModel: LightGBMBooster = this.model
}

object LightGBMRankerModel extends ConstructorReadable[LightGBMRankerModel] {
  def loadNativeModelFromFile(filename: String, labelColName: String = "label",
                              featuresColName: String = "features",
                              predictionColName: String = "prediction"): LightGBMRankerModel = {
    val uid = Identifiable.randomUID("LightGBMRanker")
    val session = SparkSession.builder().getOrCreate()
    val textRdd = session.read.text(filename)
    val text = textRdd.collect().map { row => row.getString(0) }.mkString("\n")
    val lightGBMBooster = new LightGBMBooster(text)
    new LightGBMRankerModel(uid, lightGBMBooster, labelColName, featuresColName, predictionColName)
  }

  def loadNativeModelFromString(model: String, labelColName: String = "label",
                                featuresColName: String = "features",
                                predictionColName: String = "prediction"): LightGBMRankerModel = {
    val uid = Identifiable.randomUID("LightGBMRanker")
    val lightGBMBooster = new LightGBMBooster(model)
    new LightGBMRankerModel(uid, lightGBMBooster, labelColName, featuresColName, predictionColName)
  }
}
