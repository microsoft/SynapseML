// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.ml.classification.{ProbabilisticClassificationModel, ProbabilisticClassifier}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql._

import scala.reflect.runtime.universe.{TypeTag, typeTag}

import scala.math.exp
import vowpalwabbit.spark.prediction.ScalarPrediction

@InternalWrapper
class VowpalWabbitClassifier(override val uid: String)
  extends ProbabilisticClassifier[Vector, VowpalWabbitClassifier, VowpalWabbitClassificationModel]
  with VowpalWabbitBase
{
  def this() = this(Identifiable.randomUID("VowpalWabbitClassifier"))

  override protected def train(dataset: Dataset[_]): VowpalWabbitClassificationModel = {

    val binaryModel = trainInternal(dataset)

    // which mode one once to use depends a bit on how this should be deployed
    // 1. if you stay in spark w/o link=logistic is probably more convenient as it also returns the raw prediction
    // 2. if you want to export the model *and* get probabilities at scoring term w/ link=logistic is preferable
    val model = if (getArgs.contains("--link=logistic"))
      new VowpalWabbitBinaryClassificationModel(uid, binaryModel)
    else
      new VowpalWabbitBinaryExternalClassificationModel(uid, binaryModel)

    model
      .setFeaturesCol(getFeaturesCol)
  }

  override def copy(extra: ParamMap): VowpalWabbitClassifier = defaultCopy(extra)
}

// Preparation for multi-class learning, though it no fun as numClasses is spread around multiple reductions
@InternalWrapper
abstract class VowpalWabbitClassificationModel(
    override val uid: String,
    val model: Array[Byte])
  extends ProbabilisticClassificationModel[Vector, VowpalWabbitClassificationModel]
    with VowpalWabbitBaseModel
{
}

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
}
