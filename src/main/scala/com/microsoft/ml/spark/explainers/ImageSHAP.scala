// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.explainers

import com.microsoft.ml.spark.core.schema.{DatasetExtensions, ImageSchemaUtils}
import com.microsoft.ml.spark.io.image.ImageUtils
import com.microsoft.ml.spark.lime._
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.ComplexParamsReadable
import org.apache.spark.ml.image.ImageSchema
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param.shared.HasInputCol
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

import java.awt.image.BufferedImage

trait ImageSHAPParams
  extends KernelSHAPParams
    with HasCellSize
    with HasModifier
    with HasInputCol
    with HasSuperpixelCol {
  self: ImageSHAP =>

  def setInputCol(value: String): this.type = this.set(inputCol, value)

  setDefault(cellSize -> 16, modifier -> 130, superpixelCol -> "superpixels")
}

class ImageSHAP(override val uid: String)
  extends KernelSHAPBase(uid)
    with ImageSHAPParams
    with ImageExplainer {

  logClass()

  def this() = {
    this(Identifiable.randomUID("ImageSHAP"))
  }

  private def sample(bi: BufferedImage, spd: SuperpixelData, numSamplesOpt: Option[Int]): Seq[(ImageFormat, Vector)] = {
    val effectiveNumSamples = KernelSHAPBase.getEffectiveNumSamples(numSamplesOpt, spd.clusters.size)
    val sampler = new KernelSHAPImageSampler(bi, spd, effectiveNumSamples)
    (1 to effectiveNumSamples).map {
      _ =>
        val (outputImage, feature, _) = sampler.sample
        val (path, height, width, nChannels, mode, decoded) = ImageUtils.toSparkImageTuple(outputImage)
        val imageFormat = ImageFormat(path, height, width, nChannels, mode, decoded)
        (imageFormat, feature)
    }
  }

  private lazy val numSampleOpt = this.getNumSamplesOpt

  private lazy val imageSamplesUdf = {
    UDFUtils.oldUdf(
      {
        (image: Row, sp: Row) =>
          val bi = ImageUtils.toBufferedImage(image)
          val spd = SuperpixelData.fromRow(sp)
          sample(bi, spd, numSampleOpt)
      },
      getSampleSchema(ImageSchema.columnSchema)
    )
  }

  private lazy val binarySamplesUdf = {
    UDFUtils.oldUdf(
      {
        (data: Array[Byte], sp: Row) =>
          val biOpt = ImageUtils.safeRead(data)
          val spd = SuperpixelData.fromRow(sp)
          biOpt.map {
            bi =>
              sample(bi, spd, numSampleOpt)
          }.getOrElse(Seq.empty)
      },
      getSampleSchema(ImageSchema.columnSchema)
    )
  }

  override protected def createSamples(df: DataFrame, idCol: String, coalitionCol: String): DataFrame = {
    val samplingUdf = df.schema(getInputCol).dataType match {
      case BinaryType =>
        this.binarySamplesUdf
      case t if ImageSchemaUtils.isImage(t) =>
        this.imageSamplesUdf
    }

    val samplesCol = DatasetExtensions.findUnusedColumnName("samples", df)

    df.withColumn(samplesCol, explode(samplingUdf(col(getInputCol), col(getSuperpixelCol))))
      .select(
        col(idCol),
        col(samplesCol).getField(coalitionField).alias(coalitionCol),
        col(samplesCol).getField(sampleField).alias(getInputCol)
      )
  }

  override protected def validateSchema(schema: StructType): Unit = {
    super.validateSchema(schema)

    require(
      ImageSchemaUtils.isImage(schema(getInputCol).dataType) || schema(getInputCol).dataType == BinaryType,
      s"Field $getInputCol is expected to be image type or binary type, " +
        s"but got ${schema(getInputCol).dataType} instead."
    )
  }

  override def transformSchema(schema: StructType): StructType = {
    this.validateSchema(schema)
    schema
      .add(getSuperpixelCol, SuperpixelData.Schema)
      .add(getOutputCol, VectorType)
  }
}

object ImageSHAP extends ComplexParamsReadable[ImageSHAP]
