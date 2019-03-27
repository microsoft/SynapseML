// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col}
import org.apache.spark.ml.regression.{BaseRegressor, RegressionModel}

import scala.reflect.runtime.universe.{TypeTag, typeTag}

@InternalWrapper
class VowpalWabbitRegressor(override val uid: String)
  extends BaseRegressor[Row, VowpalWabbitRegressor, VowpalWabbitRegressorModel]
    with VowpalWabbitBase
{
  def this() = this(Identifiable.randomUID("VowpalWabbitRegressor"))

  override def train(dataset: Dataset[_]): VowpalWabbitRegressorModel = {
    val binaryModel = trainInternal(dataset)

    new VowpalWabbitRegressorModel(uid, binaryModel)
      .setFeaturesCol(getFeaturesCol)
      .setAdditionalFeatures(getAdditionalFeatures)
  }

  override def copy(extra: ParamMap): VowpalWabbitRegressor = defaultCopy(extra)
}

@InternalWrapper
class VowpalWabbitRegressorModel(override val uid: String, val model: Array[Byte])
  extends RegressionModel[Row, VowpalWabbitRegressorModel]
    with VowpalWabbitBaseModel
    with ConstructorWritable[VowpalWabbitRegressorModel]
{
  protected override def transformImpl(dataset: Dataset[_]): DataFrame = {
    transformImplInternal(dataset)
      .withColumn($(predictionCol), col($(rawPredictionCol)))
  }

  override def predict(features: Row): Double = {
    throw new NotImplementedError("Not implement")
  }

  override def copy(extra: ParamMap): VowpalWabbitRegressorModel =
    new VowpalWabbitRegressorModel(uid, model)
      .setFeaturesCol(getFeaturesCol)
      .setAdditionalFeatures(getAdditionalFeatures)

  override val ttag: TypeTag[VowpalWabbitRegressorModel] =
    typeTag[VowpalWabbitRegressorModel]

  override def objectsToSave: List[Any] =
    List(uid, model, getLabelCol, getFeaturesCol, getPredictionCol)
}
