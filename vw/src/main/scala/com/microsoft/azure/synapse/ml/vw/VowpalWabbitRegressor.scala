// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.vw

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.logging.SynapseMLLogging
import org.apache.spark.ml.param._
import org.apache.spark.ml.regression.RegressionModel
import org.apache.spark.ml.util._
import org.apache.spark.ml.{BaseRegressor, ComplexParamsReadable, ComplexParamsWritable}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DoubleType

/**
  * VowpalWabbit exposed as SparkML regressor.
  */
class VowpalWabbitRegressor(override val uid: String)
  extends BaseRegressor[Row, VowpalWabbitRegressor, VowpalWabbitRegressionModel]
    with VowpalWabbitBaseSpark
    with ComplexParamsWritable with SynapseMLLogging {
  logClass()

  override protected lazy val pyInternalWrapper = true

  def this() = this(Identifiable.randomUID("VowpalWabbitRegressor"))

  override def train(dataset: Dataset[_]): VowpalWabbitRegressionModel = {
    logFit({
      val model = new VowpalWabbitRegressionModel(uid)
        .setFeaturesCol(getFeaturesCol)
        .setAdditionalFeatures(getAdditionalFeatures)
        .setPredictionCol(getPredictionCol)

      trainInternal(dataset, model)
    }, dataset.columns.length)
  }

  override def copy(extra: ParamMap): VowpalWabbitRegressor = defaultCopy(extra)
}

object VowpalWabbitRegressor extends ComplexParamsReadable[VowpalWabbitRegressor]

class VowpalWabbitRegressionModel(override val uid: String)
  extends RegressionModel[Row, VowpalWabbitRegressionModel]
    with VowpalWabbitBaseModelSpark
    with ComplexParamsWritable with Wrappable with SynapseMLLogging {
  logClass()

  def this() = this(Identifiable.randomUID("VowpalWabbitRegressionModel"))

  override protected lazy val pyInternalWrapper = true

  protected override def transformImpl(dataset: Dataset[_]): DataFrame = {
    transformImplInternal(dataset)
      .withColumn($(rawPredictionCol),
        col(vowpalWabbitPredictionCol).getField("prediction").cast(DoubleType))
      .withColumn($(predictionCol), col($(rawPredictionCol)))
  }

  override def predict(features: Row): Double = {
    throw new NotImplementedError("Not implement")
  }

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)
}

object VowpalWabbitRegressionModel extends ComplexParamsReadable[VowpalWabbitRegressionModel]
