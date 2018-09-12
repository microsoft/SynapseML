// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import com.microsoft.ml.spark.schema.ImageSchema
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{DoubleParam, ParamMap}
import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}

object SuperpixelTransformer extends DefaultParamsReadable[SuperpixelTransformer]

/** A transformer that decomposes an image into it's superpixels
  */
class SuperpixelTransformer(val uid: String) extends Transformer
  with HasInputCol with HasOutputCol
  with MMLParams {
  def this() = this(Identifiable.randomUID("SuperpixelTransformer"))

  val cellSize = new DoubleParam(this, "cellSize", "Number that controls the size of the superpizels")

  def getCellSize: Double = $(cellSize)

  def setCellSize(v: Double): this.type = set(cellSize, v)

  val modifier = new DoubleParam(this, "modifier", "Number that controls ...")
  //TODO give this a reasonable mathematical thing

  def getModifier: Double = $(modifier)

  def setModifier(v: Double): this.type = set(modifier, v)

  setDefault(cellSize->16.0, modifier->130.0, outputCol->s"${uid}_output")

  override def transform(dataset: Dataset[_]): DataFrame = {
    dataset.toDF().withColumn(getOutputCol,
      Superpixel.getSuperpixelUDF(getCellSize, getModifier)(col(getInputCol)))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  /** Add the features column to the schema
    *
    * @param schema
    * @return schema with features column
    */
  override def transformSchema(schema: StructType): StructType = {
    assert(schema(getInputCol).dataType == ImageSchema.columnSchema)
    schema.add(getOutputCol, SuperpixelData.schema)
  }

}
