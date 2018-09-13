// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.awt.image.BufferedImage
import java.awt.{Image => JImage}
import java.io.ByteArrayInputStream

import com.microsoft.ml.spark.schema.ImageSchema
import javax.imageio.ImageIO
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{IntParam, ParamMap}
import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{BinaryType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

object ResizeUtils {

  def resizeBufferedImage(width: Int, height: Int)(image: BufferedImage): BufferedImage = {
    val resizedImage = image.getScaledInstance(width, height, JImage.SCALE_DEFAULT)
    val bufferedImage = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB)
    val g = bufferedImage.createGraphics()
    g.drawImage(resizedImage, 0, 0, null)
    g.dispose()
    bufferedImage
  }

  def resizeSparkImage(width: Int, height: Int)(image: Row): Row = {
    val resizedImage = resizeBufferedImage(width,height)(ImageSchema.toBufferedImage(image))
    ImageSchema.toSparkImage(resizedImage)
  }

  def resizeBytes(width: Int, height: Int)(bytes: Array[Byte]): Row = {
    val resizedImage = resizeBufferedImage(width, height)(ImageIO.read(new ByteArrayInputStream(bytes)))
    ImageSchema.toSparkImage(resizedImage)
  }
}

object ResizeImageTransformer extends DefaultParamsReadable[ResizeImageTransformer]

@InternalWrapper
class ResizeImageTransformer(val uid: String) extends Transformer
  with HasInputCol with HasOutputCol with MMLParams {

  import ResizeUtils._

  def this() = this(Identifiable.randomUID("ResizeImageTransformer"))

  val width = new IntParam(this, "width", "the width of the image")

  val height = new IntParam(this, "height", "the width of the image")

  def getWidth: Int = $(width)

  def setWidth(v: Int): this.type = set(width, v)

  def getHeight: Int = $(height)

  def setHeight(v: Int): this.type = set(height, v)

  setDefault(inputCol -> "image", outputCol -> (uid + "_output"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    require(getWidth >= 0 && getHeight >= 0, "width and height should be nonnegative")
    val inputType = dataset.schema(getInputCol).dataType
    if ( inputType == ImageSchema.columnSchema){
      val resizeUDF = udf(resizeSparkImage(getWidth, getHeight) _, ImageSchema.columnSchema)
      dataset.toDF.withColumn(getOutputCol, resizeUDF(col(getInputCol)))
    } else if (inputType == BinaryType) {
      val resizeBytesUDF = udf(resizeBytes(getWidth, getHeight) _, ImageSchema.columnSchema)
      dataset.toDF.withColumn(getOutputCol, resizeBytesUDF(col(getInputCol)))
    }else{
      throw new IllegalArgumentException(
        s"Improper dataset schema: $inputType, need image type or byte array")
    }
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    schema.add($(outputCol), ImageSchema.columnSchema)
  }
}
