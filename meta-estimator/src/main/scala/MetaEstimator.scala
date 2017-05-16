// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azureml

import org.apache.spark.ml._
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset}

object MetaEstimator extends DefaultParamsReadable[MetaEstimator]

trait MetaEstimatorParams extends AMLParams {
  val baseEstimator = new PipelineStageParam(this, "baseEstimator", "Estimator that is fit")

  def getBaseEstimator: PipelineStage = $(baseEstimator)
}

class MetaEstimator(override val uid: String) extends Estimator[MetaModel] with MetaEstimatorParams {

  def this() = this(Identifiable.randomUID("MetaEstimator"))

  def setBaseEstimator(value: PipelineStage): this.type = set(baseEstimator, value)

  def mapBaseEstimator[T<:PipelineStage](mapping: T => PipelineStage): this.type = {
    setBaseEstimator(mapping(getBaseEstimator.asInstanceOf[T]))
  }

  override def fit(dataset: Dataset[_]): MetaModel = {
    val fitStage = getBaseEstimator match {
      case e: Estimator[_] => e.fit(dataset)
      case t: Transformer => t
      case _ => throw new IllegalArgumentException("base Estimator needs to be an instance" +
        " of a transformer or an estimator")
    }
    new MetaModel(uid, fitStage).setParent(this)
  }

  override def copy(extra: ParamMap): Estimator[MetaModel] = {
    val map = extractParamMap(extra)
    val newEstimator = map(baseEstimator).copy(extra)
    new MetaEstimator().setBaseEstimator(newEstimator)
  }

  override def transformSchema(schema: StructType): StructType = getBaseEstimator.transformSchema(schema)
}

class MetaModel(val uid: String, fitModel: Transformer) extends Model[MetaModel] with MLWritable
  with MetaEstimatorParams {

  override def copy(extra: ParamMap): MetaModel = {
    new MetaModel(uid, fitModel.copy(extra)).setParent(parent)
  }

  override def write: MLWriter = ???

  override def transform(dataset: Dataset[_]): DataFrame = fitModel.transform(dataset)

  override def transformSchema(schema: StructType): StructType = fitModel.transformSchema(schema)
}
