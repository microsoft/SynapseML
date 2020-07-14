// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.cb

import com.microsoft.ml.spark.core.contracts.{HasInputCols, HasOutputCol, Wrappable}
import com.microsoft.ml.spark.core.env.{InternalWrapper, StreamUtilities}
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable, Estimator, Model, PredictionModel, Predictor, Transformer}
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType

import org.apache.spark.sql.functions.array;

object ActionMerger extends ComplexParamsReadable[ActionMerger]

@InternalWrapper
class ActionMerger(override val uid: String) extends Transformer
  with HasInputCols with HasOutputCol  with Wrappable with ComplexParamsWritable {

  def this() = this(Identifiable.randomUID("ActionMerger"))

  override def copy(extra: ParamMap): ActionMerger = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = schema

  override def transform(dataset: Dataset[_]): DataFrame = {
    val inputCols = getInputCols
    dataset.withColumn(getOutputCol, array(inputCols.head, inputCols.tail: _*))
  }
}

