// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml._
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.microsoft.ml.spark.schema.ImageSchema

object ImageAugmenter extends DefaultParamsReadable[ImageAugmenter]

class ImageAugmenter(val uid: String) extends Transformer
  with HasInputCol with HasOutputCol with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("ImageAugmenter"))

  // if the problem is symmetric, the training dataset is supplemented with the "flipped" images
  // by default, we assume the left-right symmetry, but not up-down
  val flipLeftRight = BooleanParam(this, "flipLeftRight", "symmetric lr", false)

  def setFlipLeftRight(value: Boolean): this.type = set(flipLeftRight, value)

  def getFlipLeftRight: Boolean = $(flipLeftRight)

  val flipUpDown = BooleanParam(this, "flipUpDown", "symmetric ud", false)

  def setFlipUpDown(value: Boolean): this.type = set(flipUpDown, value)

  def getFlipUpDown: Boolean = $(flipUpDown)

  setDefault(flipLeftRight -> true, flipUpDown -> false)

  //flip images in a given column, keep the rest of the columns intact
  private def flipImages(dataset: Dataset[_], inCol: String, outCol: String, flipCode: Int): DataFrame = {
    val tr = new ImageTransformer()
      .flip(flipCode)
      .setInputCol(inCol)
      .setOutputCol(outCol)

    tr.transform(dataset)
  }

  def transform(dataset: Dataset[_]): DataFrame = {
    val df = dataset.toDF
    val dfID = df.withColumn(getOutputCol, new Column(getInputCol))

    val dfLR: Option[DataFrame] = if (getFlipLeftRight) {
      Some(flipImages(df, getInputCol, getOutputCol, Flip.flipLeftRight))
    } else {
      None
    }

    val dfUD: Option[DataFrame] = if (getFlipUpDown) {
      Some(flipImages(df, getInputCol, getOutputCol, Flip.flipUpDown))
    } else {
      None
    }

    List(dfLR, dfUD).flatten(x => x).foldLeft(dfID) { case (dfl, tdr) => dfl.union(tdr) }

  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  def transformSchema(schema: StructType): StructType = {
    schema.add(getOutputCol, ImageSchema.columnSchema)
  }

}
