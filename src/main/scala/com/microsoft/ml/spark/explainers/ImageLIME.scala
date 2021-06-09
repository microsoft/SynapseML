// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.explainers

import breeze.stats.distributions.RandBasis
import com.microsoft.ml.spark.core.schema.ImageSchemaUtils
import com.microsoft.ml.spark.io.image.ImageUtils
import com.microsoft.ml.spark.lime._
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.ComplexParamsReadable
import org.apache.spark.ml.image.ImageSchema
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.{Vector => SV}
import org.apache.spark.ml.param.Param
import org.apache.spark.ml.param.shared.HasInputCol
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

import java.awt.image.BufferedImage

trait ImageLIMEParams extends LIMEParams with HasCellSize with HasModifier with HasSamplingFraction with HasInputCol {
  self: ImageLIME =>

  val superpixelCol = new Param[String](
    this,
    "superpixelCol",
    "The column holding the superpixel decompositions"
  )

  def getSuperpixelCol: String = $(superpixelCol)

  def setSuperpixelCol(v: String): this.type = set(superpixelCol, v)

  def setInputCol(value: String): this.type = this.set(inputCol, value)

  setDefault(numSamples -> 900, cellSize -> 16, modifier -> 130, regularization -> 0.0, samplingFraction -> 0.7,
    superpixelCol -> "superpixels")
}

class ImageLIME(override val uid: String)
  extends LIMEBase(uid) with ImageLIMEParams {
  logClass()

  def this() = {
    this(Identifiable.randomUID("ImageLIME"))
  }

  override protected def preprocess(df: DataFrame): DataFrame = {
    // Dataframe with new column containing superpixels (Array[Cluster]) for each row (image to explain)
    new SuperpixelTransformer()
      .setCellSize(getCellSize)
      .setModifier(getModifier)
      .setInputCol(getInputCol)
      .setOutputCol(getSuperpixelCol)
      .transform(df)
  }

  private def sample(numSamples: Int, samplingFraction: Double)(bi: BufferedImage, spd: SuperpixelData)
    : Seq[(ImageFormat, SV, Double)] = {
    implicit val randBasis: RandBasis = RandBasis.mt0
    val sampler = new LIMEImageSampler(bi, samplingFraction, spd)
    (1 to numSamples).map {
      _ =>
        val (outputImage, feature, distance) = sampler.sample
        val (path, height, width, nChannels, mode, decoded) = ImageUtils.toSparkImageTuple(outputImage)
        val imageFormat = ImageFormat(path, height, width, nChannels, mode, decoded)
        (imageFormat, feature, distance)
    }
  }

  private lazy val samplingFunc = sample(this.getNumSamples, this.getSamplingFraction) _

  private lazy val imageSamplesUdf = {
    UDFUtils.oldUdf(
      {
        (image: Row, sp: Row) =>
          val bi = ImageUtils.toBufferedImage(image)
          val spd = SuperpixelData.fromRow(sp)
          samplingFunc(bi, spd)
      },
      getSampleSchema
    )
  }

  private lazy val binarySamplesUdf = {
    UDFUtils.oldUdf(
      {
        (data: Array[Byte], sp: Row) =>
          val biOpt = ImageUtils.safeRead(data)
          val spd = SuperpixelData.fromRow(sp)
          biOpt.map {
            bi => samplingFunc(bi, spd)
          }.getOrElse(Seq.empty)
      },
      getSampleSchema
    )
  }

  override protected def createSamples(df: DataFrame,
                                       idCol: String,
                                       stateCol: String,
                                       distanceCol: String
                                      ): DataFrame = {

    val samplingUdf = df.schema(getInputCol).dataType match {
      case BinaryType =>
        this.binarySamplesUdf
      case t if ImageSchemaUtils.isImage(t) =>
        this.imageSamplesUdf
    }

    df.withColumn("samples", explode(samplingUdf(col(getInputCol), col(getSuperpixelCol))))
      .select(
        col(idCol),
        col("samples.distance").alias(distanceCol),
        col("samples.state").alias(stateCol),
        col("samples.sample").alias(getInputCol)
      )
  }

  private def getSampleSchema: DataType = {
    ArrayType(
      StructType(Seq(
        StructField("sample", ImageSchema.columnSchema),
        StructField("state", VectorType),
        StructField("distance", DoubleType)
      ))
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

object ImageLIME extends ComplexParamsReadable[ImageLIME]
