// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.lime

import com.microsoft.ml.spark.codegen.Wrappable
import com.microsoft.ml.spark.core.contracts.{HasInputCol, HasOutputCol}
import com.microsoft.ml.spark.core.schema.ImageSchemaUtils
import com.microsoft.ml.spark.logging.BasicLogging
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{DoubleParam, ParamMap, Params}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{BinaryType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

object SuperpixelTransformer extends DefaultParamsReadable[SuperpixelTransformer]

trait HasCellSize extends Params {
  val cellSize = new DoubleParam(this, "cellSize", "Number that controls the size of the superpixels")

  def getCellSize: Double = $(cellSize)

  def setCellSize(v: Double): this.type = set(cellSize, v)
}

trait HasModifier extends Params {
  val modifier = new DoubleParam(this, "modifier", "Controls the trade-off spatial and color distance")

  def getModifier: Double = $(modifier)

  def setModifier(v: Double): this.type = set(modifier, v)
}

/** A transformer that decomposes an image into it's superpixels
  */
class SuperpixelTransformer(val uid: String) extends Transformer
  with HasInputCol with HasOutputCol
  with Wrappable with DefaultParamsWritable with HasCellSize with HasModifier with BasicLogging {
  logClass()

  def this() = this(Identifiable.randomUID("SuperpixelTransformer"))

  setDefault(cellSize->16.0, modifier->130.0, outputCol->s"${uid}_output")

  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform()
    val getSuperPixels = Superpixel.getSuperpixelUDF(
      dataset.schema(getInputCol).dataType, getCellSize, getModifier)

    dataset.toDF().withColumn(getOutputCol, getSuperPixels(col(getInputCol)))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema(getInputCol).dataType
    assert(ImageSchemaUtils.isImage(inputType) || inputType == BinaryType)
    schema.add(getOutputCol, SuperpixelData.Schema)
  }

}
