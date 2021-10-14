// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.lime

import com.microsoft.ml.spark.codegen.Wrappable
import com.microsoft.ml.spark.core.contracts.{HasInputCol, HasOutputCol}
import com.microsoft.ml.spark.core.schema.ImageSchemaUtils
import com.microsoft.ml.spark.logging.BasicLogging
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{DoubleParam, FloatParam, IntParam, ParamMap, ParamValidators, Params}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{BinaryType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.bytedeco.opencv.global.{opencv_ximgproc => ximgproc}

object SuperpixelTransformer extends DefaultParamsReadable[SuperpixelTransformer]

trait SLICParams extends Params {
  @deprecated("Use regionSize instead.", since = "1.0.0-rc3")
  val cellSize = new DoubleParam(this, "cellSize", "Dummy parameter. For backward compatibility only.")

  @deprecated("Use getRegionSize instead.", since = "1.0.0-rc3")
  def getCellSize: Double = $(cellSize)

  @deprecated("Use setRegionSize instead.", since = "1.0.0-rc3")
  def setCellSize(v: Double): this.type = set(cellSize, v)

  @deprecated("This param is no longer supported.", since = "1.0.0-rc3")
  val modifier = new DoubleParam(this, "modifier", "Dummy parameter. For backward compatibility only.")

  @deprecated("This param is no longer supported.", since = "1.0.0-rc3")
  def getModifier: Double = $(modifier)

  @deprecated("This param is no longer supported.", since = "1.0.0-rc3")
  def setModifier(v: Double): this.type = set(modifier, v)

  val regionSize = new IntParam(
    this,
    "regionSize",
    "Chooses an average superpixel size measured in pixels.",
    ParamValidators.gt(0)
  )

  def getRegionSize: Int = $(regionSize)

  def setRegionSize(v: Int): this.type = set(regionSize, v)

  val slicType = new IntParam(
    this,
    "slicType",
    "Type of SLIC optimization: LIC = 100: baseline; SLICO = 101: Zero parameter SLIC; MSLIC = 102: Manifold SLIC",
    ParamValidators.inArray(Array(ximgproc.SLIC, ximgproc.SLICO, ximgproc.MSLIC))
  )

  def getSLICType: Int = $(slicType)

  def setSLICType(value: Int): this.type = set(slicType, value)

  val ruler = new FloatParam(
    this,
    "ruler",
    "Chooses the enforcement of superpixel smoothness factor of superpixel. Higher value makes " +
      "the cluster boundary smoother.",
    ParamValidators.gt(0)
  )

  def getRuler: Float = $(ruler)

  def setRuler(value: Float): this.type = set(ruler, value)

  val iterations = new IntParam(
    this,
    "iterations",
    "Number of iterations to calculate the superpixel segmentation on a given image with " +
      "the initialized parameters in the SuperpixelSLIC object. Higher number improves the result",
    ParamValidators.gt(0)
  )

  def getIterations: Int = $(iterations)
  def setIterations(value: Int): this.type = set(iterations, value)

  //noinspection ScalaStyle
  val minElementSize = new IntParam(
    this,
    "minElementSize",
    "When specified, enforce label connectivity. The minimum element size in percents " +
      "that should be absorbed into a bigger superpixel. Valid value should be in 0-100 range, " +
      "25 means that less than a quarter sized superpixel should be absorbed.",
    ParamValidators.inRange(0, 100)
  )

  def getMinElementSize: Option[Int] = this.get(minElementSize)
  def setMinElementSize(value: Int): this.type = set(minElementSize, value)

  setDefault(regionSize -> 10, ruler -> 10f, slicType -> ximgproc.SLIC, iterations -> 10)
}

/** A transformer that decomposes an image into it's superpixels
  */
class SuperpixelTransformer(val uid: String) extends Transformer
  with HasInputCol with HasOutputCol
  with Wrappable with DefaultParamsWritable with SLICParams with BasicLogging {
  logClass()

  def this() = this(Identifiable.randomUID("SuperpixelTransformer"))

  setDefault(outputCol->s"${uid}_output")

  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame]({
      val getSuperPixels = Superpixel.getSuperpixelUDF(
        dataset.schema(getInputCol).dataType, getSLICType, getRegionSize, getRuler, getIterations, getMinElementSize
      )

      dataset.toDF().withColumn(getOutputCol, getSuperPixels(col(getInputCol)))
    })
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema(getInputCol).dataType
    assert(ImageSchemaUtils.isImage(inputType) || inputType == BinaryType)
    schema.add(getOutputCol, SuperpixelData.Schema)
  }
}
