// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.ml.classification.{ProbabilisticClassificationModel, ProbabilisticClassifier}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.shared.HasProbabilityCol
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, udf}

import scala.math.exp

object VowpalWabbitClassifier extends DefaultParamsReadable[VowpalWabbitClassifier]

@InternalWrapper
class VowpalWabbitClassifier(override val uid: String)
  extends ProbabilisticClassifier[Row, VowpalWabbitClassifier, VowpalWabbitClassificationModel]
  with VowpalWabbitBase
{
  def this() = this(Identifiable.randomUID("VowpalWabbitClassifier"))

  override protected def train(dataset: Dataset[_]): VowpalWabbitClassificationModel = {

    val binaryModel = trainInternal(dataset)

    new VowpalWabbitClassificationModel(uid, binaryModel)
      .setFeaturesCol(getFeaturesCol)
      .setAdditionalFeatures(getAdditionalFeatures)
  }

  override def copy(extra: ParamMap): VowpalWabbitClassifier = defaultCopy(extra)
}

// Preparation for multi-class learning, though it no fun as numClasses is spread around multiple reductions
@InternalWrapper
class VowpalWabbitClassificationModel(
    override val uid: String,
    val model: Array[Byte])
  extends ProbabilisticClassificationModel[Row, VowpalWabbitClassificationModel]
    with VowpalWabbitBaseModel with HasProbabilityCol // TODO: HasThresholds
{
  def numClasses: Int = 2

  override def transform(dataset: Dataset[_]): DataFrame = {
    val df = transformImplInternal(dataset)

    // which mode one once to use depends a bit on how this should be deployed
    // 1. if you stay in spark w/o link=logistic is probably more convenient as it also returns the raw prediction
    // 2. if you want to export the model *and* get probabilities at scoring term w/ link=logistic is preferable

    // convert raw prediction to probability (if needed)
    val probabilityUdf = if (vwArgs.getArgs.contains("--link=logistic"))
      udf { (pred: Double) => Vectors.dense(Array(1 - pred, pred)) }
    else
      udf { (pred: Double) => {
        val prob = 1.0 / (1.0 + exp(-pred))
        Vectors.dense(Array(1 - prob, prob))
      } }

    val df2 = df.withColumn($(probabilityCol), probabilityUdf(col($(rawPredictionCol))))

    // convert probability to prediction
    val probability2predictionUdf = udf(probability2prediction _)
    df2.withColumn($(predictionCol), probability2predictionUdf(col($(probabilityCol))))
  }

  override def copy(extra: ParamMap): VowpalWabbitClassificationModel =
    new VowpalWabbitClassificationModel(uid, model)
      .setFeaturesCol(getFeaturesCol)
    .setAdditionalFeatures(getAdditionalFeatures)

  protected override def predictRaw(features: Row): Vector = {
    throw new NotImplementedError("Not implement")
  }

  protected override def raw2probabilityInPlace(rawPrediction: Vector): Vector= {
    throw new NotImplementedError("Not implement")
  }

  /*
    override val ttag: TypeTag[VowpalWabbitClassificationModel] =
      typeTag[VowpalWabbitClassificationModel]

    override def objectsToSave: List[Any] =
      List(uid, model, getLabelCol, getFeaturesCol, getPredictionCol,
        getProbabilityCol, getRawPredictionCol)*/
}
/*
@InternalWrapper
class VowpalWabbitBinaryExternalClassificationModel(override val uid: String,
                                            override val model: Array[Byte])
  extends VowpalWabbitClassificationModel(uid, model)
    with ConstructorWritable[VowpalWabbitBinaryExternalClassificationModel]
{
  setThresholds(Array(0.5, 0.5))

  override def numClasses: Int = 2

  override protected def raw2probabilityInPlace(rawPrediction: Vector): Vector = {
    val prob = 1.0 / (1.0 + exp(-rawPrediction.apply(0)))

    Vectors.dense(Array(1 - prob, prob))
  }

  override protected def predictRaw(features: Vector): Vector = {
    val pred = predictInternal(features).asInstanceOf[ScalarPrediction].getValue.toDouble

    // TODO: validate with Gianluca
    Vectors.dense(Array(-pred, pred))
  }

  override protected def predictProbability(features: Vector): Vector = {
   raw2probabilityInPlace(predictRaw(features))
  }

  override def copy(extra: ParamMap): VowpalWabbitBinaryExternalClassificationModel =
    new VowpalWabbitBinaryExternalClassificationModel(uid, model)

  override val ttag: TypeTag[VowpalWabbitBinaryExternalClassificationModel] =
    typeTag[VowpalWabbitBinaryExternalClassificationModel]

  override def objectsToSave: List[Any] =
    List(uid, model, getLabelCol, getFeaturesCol, getPredictionCol,
      getProbabilityCol, getRawPredictionCol)
}

@InternalWrapper
class VowpalWabbitBinaryClassificationModel(override val uid: String,
                                            override val model: Array[Byte])
  extends VowpalWabbitClassificationModel(uid, model)
    with ConstructorWritable[VowpalWabbitBinaryClassificationModel]
{

  override def numClasses: Int = 2

  override protected def raw2probabilityInPlace(rawPrediction: Vector): Vector = {
    rawPrediction
  }

  override protected def predictRaw(features: Vector): Vector = {
    // do not have access to the underlying raw prediction anymore
    predictProbability(features)
  }

  override protected def predictProbability(features: Vector): Vector = {
    val pred = predictInternal(features).asInstanceOf[ScalarPrediction].getValue.toDouble
    val arr = Array(1 - pred, pred)

    Vectors.dense(arr)
  }

  override def copy(extra: ParamMap): VowpalWabbitBinaryClassificationModel =
    new VowpalWabbitBinaryClassificationModel(uid, model)

  override val ttag: TypeTag[VowpalWabbitBinaryClassificationModel] =
    typeTag[VowpalWabbitBinaryClassificationModel]

  override def objectsToSave: List[Any] =
    List(uid, model, getLabelCol, getFeaturesCol, getPredictionCol,
      getProbabilityCol, getRawPredictionCol)
}*/

// TODO: I got binary models not text
/*
object VowpalWabbitClassificationModel extends ConstructorReadable[VowpalWabbitClassificationModel] {
  def loadNativeModelFromFile(filename: String, labelColName: String = "label",
  featuresColName: String = "features", predictionColName: String = "prediction",
  probColName: String = "probability",
  rawPredictionColName: String = "rawPrediction"): VowpalWabbitClassificationModel = {
  val uid = Identifiable.randomUID("LightGBMClassifier")
  val session = SparkSession.builder().getOrCreate()
  val textRdd = session.read.text(filename)
  val text = textRdd.collect().map { row => row.getString(0) }.mkString("\n")
  val lightGBMBooster = new LightGBMBooster(text)
  val actualNumClasses = lightGBMBooster.getNumClasses()
  new LightGBMClassificationModel(uid, lightGBMBooster, labelColName, featuresColName,
  predictionColName, probColName, rawPredictionColName, None, actualNumClasses)
}

  def loadNativeModelFromString(model: String, labelColName: String = "label",
  featuresColName: String = "features", predictionColName: String = "prediction",
  probColName: String = "probability",
  rawPredictionColName: String = "rawPrediction"): LightGBMClassificationModel = {
  val uid = Identifiable.randomUID("LightGBMClassifier")
  val lightGBMBooster = new LightGBMBooster(model)
  val actualNumClasses = lightGBMBooster.getNumClasses()
  new LightGBMClassificationModel(uid, lightGBMBooster, labelColName, featuresColName,
  predictionColName, probColName, rawPredictionColName, None, actualNumClasses)
}
}*/
