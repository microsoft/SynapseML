// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.awt.Color
import java.awt.color.ColorSpace
import java.awt.image.BufferedImage
import java.io.ByteArrayInputStream

import com.microsoft.ml.spark.schema.ImageSchema._
import javax.imageio.ImageIO
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.linalg.{DenseVector, Vector}
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.param.{IntParam, ParamMap}
import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.math.round

object UnrollImage extends DefaultParamsReadable[UnrollImage]{

  private[ml] def unroll(row: Row): DenseVector = {
    val width = getWidth(row)
    val height = getHeight(row)
    val bytes = getBytes(row)

    val area = width*height
    require(area >= 0 && area < 1e8, "image has incorrect dimensions" )
    require(bytes.length == width*height*3, "image has incorrect number of bytes" )

    val rearranged =  Array.fill[Double](area*3)(0.0)
    var count = 0
    for (c <- 0 until 3) {
      for (h <- 0 until height) {
        for (w <- 0 until width) {
          val index = h * width * 3 + w*3 + c
          val b = bytes(index).toDouble

          //TODO: is there a better way to convert to unsigned byte?
          rearranged(count) =  if (b>0) b else b + 256.0
          count += 1
        }
      }
    }
    new DenseVector(rearranged)
  }

  private[ml] def roll(values: Vector, originalImage: Row): Row = {
    roll(
      values.toArray.map(d => math.max(0, math.min(255, round(d))).toInt),
      originalImage.getString(0),
      originalImage.getInt(1),
      originalImage.getInt(2),
      originalImage.getInt(3))
  }

  private[ml] def roll(values: Array[Int], path: String, height: Int, width: Int, typeVal: Int): Row = {
    val area = width*height
    require(area >= 0 && area < 1e8, "image has incorrect dimensions" )
    require(values.length == width*height*3, "image has incorrect number of bytes" )

    val rearranged =  Array.fill[Int](area*3)(0)
    var count = 0
    for (c <- 0 until 3) {
      for (h <- 0 until height) {
        for (w <- 0 until width) {
          val index = h * width * 3 + w*3 + c
          val b = values(count)
          rearranged(index) = if (b < 128) b else b - 256
          count += 1
        }
      }
    }

    Row(path, height, width, typeVal, rearranged.map(_.toByte))
  }

  private[ml] def unrollBI(image: BufferedImage): DenseVector = {
    val nChannels = image.getColorModel.getNumComponents
    val isGray = image.getColorModel.getColorSpace.getType == ColorSpace.TYPE_GRAY
    val hasAlpha = image.getColorModel.hasAlpha
    val height = image.getHeight
    val width = image.getWidth
    val unrolled = new Array[Double](height * width * nChannels)

    var offset = 0
    for (c <- 0 until nChannels){
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

    new DenseVector(unrolled)
  }

  private[ml] def unrollBytes(bytes: Array[Byte], width: Option[Int], height: Option[Int]): Option[DenseVector] = {
    val biOpt = Option(ImageIO.read(new ByteArrayInputStream(bytes)))
    biOpt.map { bi =>
      (height, width) match {
        case (Some(h), Some(w)) => unrollBI(ResizeUtils.resizeBufferedImage(w, h)(bi))
        case (None, None) => unrollBI(bi)
        case _ =>
          throw new IllegalArgumentException("Height and width must either both be specified or unspecified")
      }
    }
  }

}

/** Converts the representation of an m X n pixel image to an m * n vector of Doubles
  *
  * The input column name is assumed to be "image", the output column name is "<uid>_output"
  *
  * @param uid The id of the module
  */
class UnrollImage(val uid: String) extends Transformer with HasInputCol with HasOutputCol with MMLParams{

  def this() = this(Identifiable.randomUID("UnrollImage"))

  import com.microsoft.ml.spark.UnrollImage._

  setDefault(inputCol -> "image", outputCol -> (uid + "_output"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    val df = dataset.toDF
    assert(isImage(df, $(inputCol)), "input column should have Image type")

    val func = unroll(_)
    val unrollUDF = udf(func)

    df.withColumn($(outputCol), unrollUDF(df($(inputCol))))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    schema.add($(outputCol), VectorType)
  }

}

object UnrollBinaryImage extends DefaultParamsReadable[UnrollBinaryImage]

/** Converts the representation of an m X n pixel image to an m * n vector of Doubles
  *
  * The input column name is assumed to be "image", the output column name is "<uid>_output"
  *
  * @param uid The id of the module
  */
class UnrollBinaryImage(val uid: String) extends Transformer with HasInputCol with HasOutputCol with MMLParams{

  def this() = this(Identifiable.randomUID("UnrollImage"))

  import com.microsoft.ml.spark.UnrollImage._

  val width = new IntParam(this, "width", "the width of the image")

  val height = new IntParam(this, "height", "the width of the image")

  def getWidth: Int = $(width)

  def setWidth(v: Int): this.type = set(width, v)

  def getHeight: Int = $(height)

  def setHeight(v: Int): this.type = set(height, v)

  setDefault(inputCol -> "image", outputCol -> (uid + "_output"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    val df = dataset.toDF
    assert(df.schema(getInputCol).dataType == BinaryType, "input column should have Binary type")

    val unrollUDF = udf({ bytes: Array[Byte] => unrollBytes(bytes, get(width), get(height))}, VectorType)

    df.withColumn($(outputCol), unrollUDF(df($(inputCol))))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    schema.add($(outputCol), VectorType)
  }

}
