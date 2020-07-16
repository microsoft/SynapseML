// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.vw

import com.microsoft.ml.spark.core.contracts.{HasInputCols, HasOutputCol, Wrappable}
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable, Transformer}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.array
import org.apache.spark.sql.types.{ArrayType, StructType};

object ColumnVectorSequencer extends ComplexParamsReadable[ColumnVectorSequencer]

class ColumnVectorSequencer(override val uid: String) extends Transformer
  with HasInputCols with HasOutputCol with Wrappable with ComplexParamsWritable {

  def this() = this(Identifiable.randomUID("ColumnVectorSequencer"))

  override def copy(extra: ParamMap): ColumnVectorSequencer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    val firstDt = schema(getInputCols(0)).dataType
    getInputCols.tail.foreach(col => assert(schema(col).dataType == firstDt))
    schema.add(getOutputCol, ArrayType(firstDt))
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val inputCols = getInputCols
    dataset.withColumn(getOutputCol, array(inputCols.head, inputCols.tail: _*))
  }
}

