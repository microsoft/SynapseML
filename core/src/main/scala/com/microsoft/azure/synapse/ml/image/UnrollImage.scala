// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.image

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.core.contracts.{HasInputCol, HasOutputCol}
import com.microsoft.azure.synapse.ml.core.schema.ImageSchemaUtils
import com.microsoft.azure.synapse.ml.io.image.ImageUtils
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.{Vectors, Vector => SVector}
import org.apache.spark.ml.param.{IntParam, ParamMap}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import java.awt.Color
import java.awt.color.ColorSpace
import java.awt.image.BufferedImage
import java.awt.{Image => JImage}
import scala.math.round

object UnrollImage extends DefaultParamsReadable[UnrollImage] {

  import org.apache.spark.ml.image.ImageSchema._

  private[ml] def unroll(row: Row): SVector = {
    val width = getWidth(row)
    val height = getHeight(row)
    val bytes = getData(row)
    val nChannels = getNChannels(row)

    val area = width * height
    require(area >= 0 && area < 1e8, "image has incorrect dimensions")
    require(bytes.length == width * height * nChannels, "image has incorrect number of bytes")

    val rearranged = Array.fill[Double](area * nChannels)(0.0)
    var count = 0
    for (c <- 0 until nChannels) {
      for (h <- 0 until height) {
        for (w <- 0 until width) {
          val index = h * width * nChannels + w * nChannels + c
          val b = bytes(index).toDouble

          //TODO: is there a better way to convert to unsigned byte?
          rearranged(count) = if (b > 0) b else b + 256.0
          count += 1
        }
      }
    }
    Vectors.dense(rearranged)
  }

  private[ml] def roll(values: SVector, originalImage: Row): Row = {
    roll(
      values.toArray.map(d => math.max(0, math.min(255, round(d))).toInt), //scalastyle:ignore magic.number
      originalImage.getString(0),
      originalImage.getInt(1),
      originalImage.getInt(2),
      originalImage.getInt(3),
      originalImage.getInt(4)
    )
  }

  private[ml] def roll(values: Array[Int], path: String,
                       height: Int, width: Int, nChannels: Int, mode: Int): Row = {
    val area = width * height
    require(area >= 0 && area < 1e8, "image has incorrect dimensions")
    require(values.length == width * height * 3, "image has incorrect number of bytes")

    val rearranged = Array.fill[Int](area * 3)(0)
    var count = 0
    for (c <- 0 until 3) {
      for (h <- 0 until height) {
        for (w <- 0 until width) {
          val index = h * width * 3 + w * 3 + c
          val b = values(count)
          rearranged(index) = if (b < 128) b else b - 256
          count += 1
        }
      }
    }

    Row(path, height, width, nChannels, mode, rearranged.map(_.toByte))
  }

  private[ml] def unrollBI(image: BufferedImage): SVector = {  //scalastyle:ignore cyclomatic.complexity
    val nChannels = image.getColorModel.getNumComponents
    val isGray = image.getColorModel.getColorSpace.getType == ColorSpace.TYPE_GRAY
    val hasAlpha = image.getColorModel.hasAlpha
    val height = image.getHeight
    val width = image.getWidth
    val unrolled = new Array[Double](height * width * nChannels)

    if (isGray) {
      var offset = 0
      val raster = image.getRaster
      for (h <- 0 until height) {
        for (w <- 0 until width) {
          unrolled(offset) = raster.getSample(w, h, 0).toDouble
          offset += 1
        }
      }
    } else {
      var offset = 0
      for (c <- 0 until nChannels) {
        for (h <- 0 until height) {
          for (w <- 0 until width) {
            val color = new Color(image.getRGB(w, h), hasAlpha)
            unrolled(offset) = c match {
              case 0 => color.getBlue.toDouble
              case 1 => color.getGreen.toDouble
              case 2 => color.getRed.toDouble
              case 3 => color.getAlpha.toDouble
            }
            offset += 1
          }
        }
      }
    }

    Vectors.dense(unrolled)
  }

  private[ml] def unrollBytes(bytes: Array[Byte],
                              width: Option[Int],
                              height: Option[Int],
                              nChannels: Option[Int]): Option[SVector] = {
    val biOpt = ImageUtils.safeRead(bytes)
    biOpt.map { bi =>
      (height, width) match {
        case (Some(h), Some(w)) => unrollBI(resizeBufferedImage(w, h, nChannels)(bi))
        case (None, None) => unrollBI(bi)
        case _ =>
          throw new IllegalArgumentException("Height and width must either both be specified or unspecified")
      }
    }
  }

  private[ml] def resizeBufferedImage(width: Int,
                                      height: Int,
                                      channels: Option[Int])(image: BufferedImage): BufferedImage = {
    val imgType = channels.map(ImageUtils.channelsToType).getOrElse(image.getType)

    if (image.getWidth == width && image.getHeight == height && image.getType == imgType) {
      image
    } else {
      val resizedImage = image.getScaledInstance(width, height, JImage.SCALE_DEFAULT)
      val bufferedImage = new BufferedImage(width, height, imgType)
      val g = bufferedImage.createGraphics()
      g.drawImage(resizedImage, 0, 0, null) //scalastyle:ignore null
      g.dispose()
      bufferedImage
    }
  }

}

/** Converts the representation of an m X n pixel image to an m * n vector of Doubles
  *
  * The input column name is assumed to be "image", the output column name is "<uid>_output"
  *
  * @param uid The id of the module
  */
class UnrollImage(val uid: String) extends Transformer
  with HasInputCol with HasOutputCol with Wrappable with DefaultParamsWritable with SynapseMLLogging {
  logClass(FeatureNames.Image)

  import UnrollImage._

  def this() = this(Identifiable.randomUID("UnrollImage"))

  setDefault(inputCol -> "image", outputCol -> (uid + "_output"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame]({
      val df = dataset.toDF
      assert(ImageSchemaUtils.isImage(df.schema(getInputCol)), "input column should have Image type")
      val unrollUDF = udf(unroll _)
      df.withColumn(getOutputCol, unrollUDF(df(getInputCol)))
    }, dataset.columns.length)
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    schema.add(getOutputCol, VectorType)
  }

}

object UnrollBinaryImage extends DefaultParamsReadable[UnrollBinaryImage]

/** Converts the representation of an m X n pixel image to an m * n vector of Doubles
  *
  * The input column name is assumed to be "image", the output column name is "<uid>_output"
  *
  * @param uid The id of the module
  */
class UnrollBinaryImage(val uid: String) extends Transformer
  with HasInputCol with HasOutputCol with Wrappable with DefaultParamsWritable with SynapseMLLogging {
  logClass(FeatureNames.Image)
  import UnrollImage._

  def this() = this(Identifiable.randomUID("UnrollImage"))

  val width = new IntParam(this, "width", "the width of the image")

  val height = new IntParam(this, "height", "the width of the image")

  val nChannels = new IntParam(this, "nChannels", "the number of channels of the target image")

  def getWidth: Int = $(width)

  def setWidth(v: Int): this.type = set(width, v)

  def getHeight: Int = $(height)

  def setHeight(v: Int): this.type = set(height, v)

  def getNChannels: Int = $(nChannels)

  def setNChannels(v: Int): this.type = set(nChannels, v)

  setDefault(inputCol -> "image", outputCol -> (uid + "_output"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame]({
      val df = dataset.toDF
      assert(df.schema(getInputCol).dataType == BinaryType, "input column should have Binary type")

      val unrollUDF = UDFUtils.oldUdf({ bytes: Array[Byte] =>
        unrollBytes(bytes, get(width), get(height), get(nChannels))
      }, VectorType)

      df.withColumn($(outputCol), unrollUDF(df($(inputCol))))
    }, dataset.columns.length)
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    schema.add($(outputCol), VectorType)
  }

}
