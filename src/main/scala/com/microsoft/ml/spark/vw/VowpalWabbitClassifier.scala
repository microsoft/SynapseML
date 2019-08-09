// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.vw

import com.microsoft.ml.spark.core.serialize.ConstructorReadable
import org.apache.spark.ml.ComplexParamsReadable
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.ml.classification.{ProbabilisticClassificationModel, ProbabilisticClassifier}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, udf}

import scala.math.exp

object VowpalWabbitClassifier extends DefaultParamsReadable[VowpalWabbitClassifier]

class VowpalWabbitClassifier(override val uid: String)
  extends ProbabilisticClassifier[Row, VowpalWabbitClassifier, VowpalWabbitClassificationModel]
  with VowpalWabbitBase
{
  def this() = this(Identifiable.randomUID("VowpalWabbitClassifier"))

  override protected def train(dataset: Dataset[_]): VowpalWabbitClassificationModel = {

    val binaryModel = trainInternal(dataset)

    new VowpalWabbitClassificationModel(uid)
      .setModel(binaryModel)
      .setFeaturesCol(getFeaturesCol)
      .setAdditionalFeatures(getAdditionalFeatures)
      .setPredictionCol(getPredictionCol)
      .setProbabilityCol(getProbabilityCol)
      .setRawPredictionCol(getRawPredictionCol)
  }

  override def copy(extra: ParamMap): VowpalWabbitClassifier = defaultCopy(extra)
}

// Preparation for multi-class learning, though it no fun as numClasses is spread around multiple reductions
class VowpalWabbitClassificationModel(override val uid: String)
  extends ProbabilisticClassificationModel[Row, VowpalWabbitClassificationModel]
    with VowpalWabbitBaseModel {

  def numClasses: Int = 2

  override def transform(dataset: Dataset[_]): DataFrame = {
    val df = transformImplInternal(dataset)

    // which mode one wants to use depends a bit on how this should be deployed
    // 1. if you stay in spark w/o link=logistic is probably more convenient as it also returns the raw prediction
    // 2. if you want to export the model *and* get probabilities at scoring term w/ link=logistic is preferable

    // convert raw prediction to probability (if needed)
    val probabilityUdf = if (vwArgs.getArgs.contains("--link logistic"))
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

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  protected override def predictRaw(features: Row): Vector = {
    throw new NotImplementedError("Not implemented")
  }

  protected override def raw2probabilityInPlace(rawPrediction: Vector): Vector= {
    throw new NotImplementedError("Not implemented")
  }
}

object VowpalWabbitClassificationModel extends ComplexParamsReadable[VowpalWabbitClassificationModel]
