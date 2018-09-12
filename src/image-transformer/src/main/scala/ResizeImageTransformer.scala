// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.awt.color.ColorSpace
import java.awt.{Color, Image => JImage}
import java.io.ByteArrayInputStream

import com.microsoft.ml.spark.schema.ImageSchema
import javax.imageio.ImageIO
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.image.{ImageSchema => SparkImageSchema}
import org.apache.spark.ml.param.{IntParam, ParamMap}
import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{BinaryType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import java.awt.image.BufferedImage

object ResizeImageTransformer extends DefaultParamsReadable[ResizeImageTransformer]

@InternalWrapper
class ResizeImageTransformer(val uid: String) extends Transformer
  with HasInputCol with HasOutputCol with MMLParams {

  def this() = this(Identifiable.randomUID("ResizeImageTransformer"))

  val width = new IntParam(this, "width", "the width of the image")

  val height = new IntParam(this, "height", "the width of the image")

  def getWidth: Int = $(width)

  def setWidth(v: Int): this.type = set(width, v)

  def getHeight: Int = $(height)

  def setHeight(v: Int): this.type = set(height, v)

  setDefault(inputCol -> "image", outputCol -> (uid + "_output"))

  /** Returns the OCV type (int) of the passed-in image */
  private def getOCVType(img: BufferedImage): Int = {
    val isGray = img.getColorModel.getColorSpace.getType == ColorSpace.TYPE_GRAY
    val hasAlpha = img.getColorModel.hasAlpha
    if (isGray) {
      SparkImageSchema.ocvTypes("CV_8UC1")
    } else if (hasAlpha) {
      SparkImageSchema.ocvTypes("CV_8UC4")
    } else {
      SparkImageSchema.ocvTypes("CV_8UC3")
    }
  }

  /**
    * Takes a Java BufferedImage and returns a Row Image (spImage).
    *
    * @param image Java BufferedImage.
    * @return Row image in spark.ml.image format with channels in BGR(A) order.
    */
  private def toSparkImage(image: BufferedImage, origin: String = null): Row = {
    val nChannels = image.getColorModel.getNumComponents
    val height = image.getHeight
    val width = image.getWidth
    val isGray = image.getColorModel.getColorSpace.getType == ColorSpace.TYPE_GRAY
    val hasAlpha = image.getColorModel.hasAlpha
    val decoded = new Array[Byte](height * width * nChannels)

    // https://github.com/apache/spark/blob/7bd46d987156/mllib/
    // src/main/scala/org/apache/spark/ml/image/ImageSchema.scala#L134
    if (isGray) {
      var offset = 0
      val raster = image.getRaster
      for (h <- 0 until height) {
        for (w <- 0 until width) {
          decoded(offset) = raster.getSample(w, h, 0).toByte
          offset += 1
        }
      }
    } else {
      var offset = 0
      for (h <- 0 until height) {
        for (w <- 0 until width) {
          val color = new Color(image.getRGB(w, h), hasAlpha)
          decoded(offset) = color.getBlue.toByte
          decoded(offset + 1) = color.getGreen.toByte
          decoded(offset + 2) = color.getRed.toByte
          if (hasAlpha) {
            decoded(offset + 3) = color.getAlpha.toByte
          }
          offset += nChannels
        }
      }
    }
    Row(origin, height, width, getOCVType(image), decoded)
  }

  private def resizeBufferedImage(width: Int, height: Int)(image: BufferedImage): BufferedImage = {
    val resizedImage = image.getScaledInstance(width, height, JImage.SCALE_DEFAULT)
    val bufferedImage = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB)
    val g = bufferedImage.createGraphics()
    g.drawImage(resizedImage, 0, 0, null)
    g.dispose()
    bufferedImage
  }

  private def resizeSparkImage(width: Int, height: Int)(image: Row): Row = {
    val resizedImage = resizeBufferedImage(width,height)(ImageSchema.toBufferedImage(image))
    val sparkImage = toSparkImage(resizedImage)
    sparkImage
  }

  private def resizeBytes(width: Int, height: Int)(bytes: Array[Byte]): Row = {
    val resizedImage = resizeBufferedImage(width, height)(ImageIO.read(new ByteArrayInputStream(bytes)))
    val sparkImage = toSparkImage(resizedImage)
    sparkImage
  }

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
