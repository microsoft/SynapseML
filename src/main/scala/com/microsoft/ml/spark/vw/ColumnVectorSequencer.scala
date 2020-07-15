// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.vw

import com.microsoft.ml.spark.core.contracts.{HasInputCols, HasOutputCol, Wrappable}
import com.microsoft.ml.spark.core.env.InternalWrapper
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable, Transformer}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.array
import org.apache.spark.sql.types.StructType;

object ColumnVectorSequencer extends ComplexParamsReadable[ColumnVectorSequencer]

@InternalWrapper
class ColumnVectorSequencer(override val uid: String) extends Transformer
  with HasInputCols with HasOutputCol with Wrappable with ComplexParamsWritable {

  def this() = this(Identifiable.randomUID("ColumnVectorSequencer"))

  override def copy(extra: ParamMap): ColumnVectorSequencer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = schema

  override def transform(dataset: Dataset[_]): DataFrame = {
    val inputCols = getInputCols
    dataset.withColumn(getOutputCol, array(inputCols.head, inputCols.tail: _*))
  }
}

