// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.image

import java.awt.image.BufferedImage
import java.awt.{Image => JImage}
import com.microsoft.ml.spark.codegen.Wrappable
import com.microsoft.ml.spark.core.contracts.{HasInputCol, HasOutputCol}
import com.microsoft.ml.spark.core.schema.ImageSchemaUtils
import com.microsoft.ml.spark.io.image.ImageUtils
import com.microsoft.ml.spark.logging.BasicLogging
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.image.ImageSchema
import org.apache.spark.ml.param.{IntParam, ParamMap}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{BinaryType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

object ResizeUtils {

  def resizeBufferedImage(width: Int, height: Int, channels: Option[Int])(image: BufferedImage): BufferedImage = {
    val imgType = channels.map(ImageUtils.channelsToType).getOrElse(image.getType)

    if (image.getWidth == width &&
      image.getHeight == height &&
      image.getType == imgType
    ) {
      return image
    }

    val resizedImage = image.getScaledInstance(width, height, JImage.SCALE_DEFAULT)
    val bufferedImage = new BufferedImage(width, height, imgType)
    val g = bufferedImage.createGraphics()
    g.drawImage(resizedImage, 0, 0, null) //scalastyle:ignore null
    g.dispose()
    bufferedImage
  }

  def resizeSparkImage(width: Int, height: Int, channels: Option[Int])(image: Row): Row = {
    val resizedImage = resizeBufferedImage(width, height, channels)(ImageUtils.toBufferedImage(image))
    ImageUtils.toSparkImage(resizedImage).getStruct(0)
  }

  def resizeBytes(width: Int, height: Int, channels: Option[Int])(bytes: Array[Byte]): Option[Row] = {
    val biOpt = ImageUtils.safeRead(bytes)
    biOpt.map { bi =>
      val resizedImage = resizeBufferedImage(width, height, channels)(bi)
      ImageUtils.toSparkImage(resizedImage)
    }
  }
}

object ResizeImageTransformer extends DefaultParamsReadable[ResizeImageTransformer]

class ResizeImageTransformer(val uid: String) extends Transformer
  with HasInputCol with HasOutputCol with Wrappable with DefaultParamsWritable with BasicLogging {
  logClass()

  import ResizeUtils._

  def this() = this(Identifiable.randomUID("ResizeImageTransformer"))

  val width = new IntParam(this, "width", "the width of the image")

  val height = new IntParam(this, "height", "the width of the image")

  val nChannels= new IntParam(this, "nChannels", "the number of channels of the target image")

  def getWidth: Int = $(width)

  def setWidth(v: Int): this.type = set(width, v)

  def getHeight: Int = $(height)

  def setHeight(v: Int): this.type = set(height, v)

  def getNChannels: Int = $(nChannels)

  def setNChannels(v: Int): this.type = set(nChannels, v)

  setDefault(inputCol -> "image", outputCol -> (uid + "_output"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform(dataset)
    require(getWidth >= 0 && getHeight >= 0, "width and height should be nonnegative")
    val inputType = dataset.schema(getInputCol).dataType
    if (ImageSchemaUtils.isImage(inputType)) {
      val resizeUDF = UDFUtils.oldUdf(resizeSparkImage(getWidth, getHeight, get(nChannels)) _, ImageSchema.columnSchema)
      dataset.toDF.withColumn(getOutputCol, resizeUDF(col(getInputCol)))
    } else if (inputType == BinaryType) {
      val resizeBytesUDF = UDFUtils.oldUdf(resizeBytes(getWidth, getHeight, get(nChannels)) _, ImageSchema.columnSchema)
      dataset.toDF.withColumn(getOutputCol, resizeBytesUDF(col(getInputCol)))
    } else {
      throw new IllegalArgumentException(
        s"Improper dataset schema: $inputType, need image type or byte array")
    }
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    schema.add($(outputCol), ImageSchema.columnSchema)
  }
}
