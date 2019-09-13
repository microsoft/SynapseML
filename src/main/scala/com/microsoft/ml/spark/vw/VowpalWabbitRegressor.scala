// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.vw

import com.microsoft.ml.spark.core.env.InternalWrapper
import com.microsoft.ml.spark.core.serialize.ConstructorReadable
import org.apache.spark.ml.{BaseRegressor, ComplexParamsReadable, ComplexParamsWritable}
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.ml.regression.RegressionModel

object VowpalWabbitRegressor extends DefaultParamsReadable[VowpalWabbitRegressor]

class VowpalWabbitRegressor(override val uid: String)
  extends BaseRegressor[Row, VowpalWabbitRegressor, VowpalWabbitRegressorModel]
    with VowpalWabbitBase
{
  def this() = this(Identifiable.randomUID("VowpalWabbitRegressor"))

  override def train(dataset: Dataset[_]): VowpalWabbitRegressorModel = {
    val model = new VowpalWabbitRegressorModel(uid)
      .setFeaturesCol(getFeaturesCol)
      .setAdditionalFeatures(getAdditionalFeatures)
      .setPredictionCol(getPredictionCol)

    trainInternal(dataset, model)
  }

  override def copy(extra: ParamMap): VowpalWabbitRegressor = defaultCopy(extra)
}

class VowpalWabbitRegressorModel(override val uid: String)
  extends RegressionModel[Row, VowpalWabbitRegressorModel]
    with VowpalWabbitBaseModel
    with ComplexParamsWritable
{
  protected override def transformImpl(dataset: Dataset[_]): DataFrame = {
    transformImplInternal(dataset)
      .withColumn($(predictionCol), col($(rawPredictionCol)))
  }

  override def predict(features: Row): Double = {
    throw new NotImplementedError("Not implement")
  }

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)
}

object VowpalWabbitRegressorModel extends ComplexParamsReadable[VowpalWabbitRegressorModel]
