// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.schema

import java.awt.color.ColorSpace
import java.awt.image.{BufferedImage, DataBufferByte, Raster}
import java.awt.{Color, Point}
import java.io.ByteArrayInputStream

import javax.imageio.ImageIO
import org.apache.spark.ml.image.{ImageSchema => SparkImageSchema}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

import scala.util.Try

case class ImageData(path: String,
                     height: Int,
                     width: Int,
                     `type`: Int,
                     bytes: Array[Byte])

object ImageData {
  def fromRow(row: Row): ImageData = {
    ImageData(
      row.getString(0),
      row.getInt(1),
      row.getInt(2),
      row.getInt(3),
      row.getAs[Array[Byte]](4)
    )
  }
}

object ImageSchema {

  /** Schema for the image column: Row(String, Int, Int, Int, Array[Byte]) */
  val columnSchema = StructType(
    StructField("path", StringType, true) ::
      StructField("height", IntegerType, true) ::
      StructField("width", IntegerType, true) ::
      // OpenCV type: CV_8U in most cases
      StructField("type", IntegerType, true) ::
      // OpenCV bytes: row-wise BGR in most cases
      StructField("bytes", BinaryType, true) :: Nil)

  // single column of images named "image"
  val schema = StructType(StructField("image", columnSchema, true) :: Nil)

  def getPath(row: Row): String      = row.getString(0)
  def getHeight(row: Row): Int        = row.getInt(1)
  def getWidth(row: Row): Int         = row.getInt(2)
  def getType(row: Row): Int          = row.getInt(3)
  def getBytes(row: Row): Array[Byte] = row.getAs[Array[Byte]](4)

  def toBufferedImage(row: Row): BufferedImage = {
    toBufferedImage(getBytes(row),getWidth(row), getHeight(row))
  }

  def toBufferedImage(bytes: Array[Byte], w: Int, h: Int): BufferedImage = {
    val img = new BufferedImage(w, h, BufferedImage.TYPE_3BYTE_BGR)
    img.setData(Raster.createRaster(
      img.getSampleModel,
      new DataBufferByte(bytes, bytes.length),
      new Point()))
    img
  }

  /** Check if the dataframe column contains images (i.e. has imageSchema)
    *
    * @param df
    * @param column
    * @return
    */
  def isImage(df: DataFrame, column: String): Boolean =
    df.schema(column).dataType == columnSchema

  /** Returns the OCV type (int) of the passed-in image */
  def getOCVType(img: BufferedImage): Int = {
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

  def safeRead(bytes: Array[Byte]): Option[BufferedImage] = {
    if (bytes == null) {return None}
    Try(Option(ImageIO.read(new ByteArrayInputStream(bytes))))
      .toOption.flatten
  }

  /**
    * Takes a Java BufferedImage and returns a Row Image (spImage).
    *
    * @param image Java BufferedImage.
    * @return Row image in spark.ml.image format with channels in BGR(A) order.
    */
  def toSparkImage(image: BufferedImage, origin: Option[String] = None): Row = {
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
    Row(origin.orNull, height, width, getOCVType(image), decoded)
  }

}
