// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.Model
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.recommendation.TrainValidRecommendSplitParams
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.reflect.runtime.universe.{TypeTag, typeTag}

class TrainValidRecommendSplitModel(
                                  override val uid: String,
                                  val bestModel: Model[_],
                                  val validationMetrics: Array[Double])
  extends Model[TrainValidRecommendSplitModel] with TrainValidRecommendSplitParams
    with ConstructorWritable[TrainValidRecommendSplitModel] {

  override def copy(extra: ParamMap): TrainValidRecommendSplitModel = {
    val copied = new TrainValidRecommendSplitModel(uid, bestModel, validationMetrics)
    copyValues(copied, extra).setParent(parent)
  }

  override val ttag: TypeTag[TrainValidRecommendSplitModel] = typeTag[TrainValidRecommendSplitModel]

  override def objectsToSave: List[AnyRef] =
    List(uid, bestModel, validationMetrics)

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    bestModel.transform(dataset)
  }

  override def transformSchema(schema: StructType): StructType = {
    bestModel.transformSchema(schema)
  }
}

object TrainValidRecommendSplitModel extends ConstructorReadable[TrainValidRecommendSplitModel]
