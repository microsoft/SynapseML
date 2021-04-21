// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.vw

import com.microsoft.ml.spark.codegen.Wrappable
import com.microsoft.ml.spark.core.contracts.{HasInputCols, HasOutputCol}
import com.microsoft.ml.spark.logging.BasicLogging
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable, Transformer}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.array
import org.apache.spark.sql.types.{ArrayType, StructType};

object VectorZipper extends ComplexParamsReadable[VectorZipper]

/**
  * Combine one or more input columns into a sequence in the output column.
  */
class VectorZipper(override val uid: String) extends Transformer
  with HasInputCols with HasOutputCol with Wrappable with ComplexParamsWritable with BasicLogging {
  logClass()

  def this() = this(Identifiable.randomUID("VectorZipper"))

  override def copy(extra: ParamMap): VectorZipper = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    val firstDt = schema(getInputCols(0)).dataType
    getInputCols.tail.foreach(col => assert(schema(col).dataType == firstDt))
    schema.add(getOutputCol, ArrayType(firstDt))
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform(dataset)
    val inputCols = getInputCols
    dataset.withColumn(getOutputCol, array(inputCols.head, inputCols.tail: _*))
  }
}

