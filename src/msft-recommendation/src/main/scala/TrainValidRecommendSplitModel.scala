// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.Model
import org.apache.spark.ml.param.{ArrayParam, ParamMap, TransformerParam}
import org.apache.spark.ml.recommendation.TrainValidRecommendSplitParams
import org.apache.spark.ml.util.{ComplexParamsReadable, ComplexParamsWritable, Identifiable}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}

@InternalWrapper
class TrainValidRecommendSplitModel(
                                     override val uid: String)
//                                     val bestModel: Model[_],
//                                     val validationMetrics: Array[Double])
  extends Model[TrainValidRecommendSplitModel] with TrainValidRecommendSplitParams
    with ComplexParamsWritable {

  def setValidationMetrics(value: Array[_]): this.type = set(validationMetrics, value)

  val validationMetrics = new ArrayParam(this, "validationMetrics", "Best Model")

  /** @group getParam */
  def getValidationMetrics: Array[_] = $(validationMetrics)

  def setBestModel(value: Model[_]): this.type = set(bestModel, value)

  val bestModel: TransformerParam =
    new TransformerParam(
      this,
      "bestModel", "The internal CNTK model used in the featurizer",
      { t => t.isInstanceOf[Model[_]] })

  /** @group getParam */
  def getBestModel: Model[_] = $(bestModel).asInstanceOf[Model[_]]

  def this() = this(Identifiable.randomUID("TrainValidRecommendSplitModel"))

  override def copy(extra: ParamMap): TrainValidRecommendSplitModel = {
    val copied = new TrainValidRecommendSplitModel(uid)
    copyValues(copied, extra).setParent(parent)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    $(bestModel).transform(dataset)
  }

  override def transformSchema(schema: StructType): StructType = {
    $(bestModel).transformSchema(schema)
  }
}

object TrainValidRecommendSplitModel extends ComplexParamsReadable[TrainValidRecommendSplitModel]
