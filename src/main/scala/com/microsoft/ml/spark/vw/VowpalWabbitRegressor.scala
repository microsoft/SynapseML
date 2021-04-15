// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.vw

import com.microsoft.ml.spark.codegen.Wrappable
import com.microsoft.ml.spark.logging.BasicLogging
import org.apache.spark.ml.{BaseRegressor, ComplexParamsReadable, ComplexParamsWritable}
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.ml.regression.RegressionModel

object VowpalWabbitRegressor extends ComplexParamsReadable[VowpalWabbitRegressor]

class VowpalWabbitRegressor(override val uid: String)
  extends BaseRegressor[Row, VowpalWabbitRegressor, VowpalWabbitRegressionModel]
    with VowpalWabbitBase
    with ComplexParamsWritable with BasicLogging {
  logClass(uid)

  override protected lazy val pyInternalWrapper = true

  def this() = this(Identifiable.randomUID("VowpalWabbitRegressor"))

  override def train(dataset: Dataset[_]): VowpalWabbitRegressionModel = {
    logTrain(uid)
    val model = new VowpalWabbitRegressionModel(uid)
      .setFeaturesCol(getFeaturesCol)
      .setAdditionalFeatures(getAdditionalFeatures)
      .setPredictionCol(getPredictionCol)

    trainInternal(dataset, model)
  }

  override def copy(extra: ParamMap): VowpalWabbitRegressor = defaultCopy(extra)
}

class VowpalWabbitRegressionModel(override val uid: String)
  extends RegressionModel[Row, VowpalWabbitRegressionModel]
    with VowpalWabbitBaseModel
    with ComplexParamsWritable with Wrappable with BasicLogging {
  logClass(uid)

  def this() = this(Identifiable.randomUID("VowpalWabbitRegressionModel"))

  override protected lazy val pyInternalWrapper = true

  protected override def transformImpl(dataset: Dataset[_]): DataFrame = {
    transformImplInternal(dataset)
      .withColumn($(predictionCol), col($(rawPredictionCol)))
  }

  override def predict(features: Row): Double = {
    logPredict(uid)
    throw new NotImplementedError("Not implement")
  }

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)
}

object VowpalWabbitRegressionModel extends ComplexParamsReadable[VowpalWabbitRegressionModel]
