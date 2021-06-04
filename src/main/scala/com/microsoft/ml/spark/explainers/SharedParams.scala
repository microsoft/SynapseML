// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.explainers

import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.{Vector => SV}
import org.apache.spark.ml.param._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

trait HasMetricsCol extends Params {
  val metricsCol = new Param[String](
    this,
    "metricsCol",
    "Column name for fitting metrics"
  )

  def getMetricsCol: String = $(metricsCol)

  def setMetricsCol(v: String): this.type = this.set(metricsCol, v)
}

trait HasModel extends Params {
  val model = new TransformerParam(this, "model", "The model to be interpreted.")

  def getModel: Transformer = $(model)

  def setModel(v: Transformer): this.type = set(model, v)
}

trait HasNumSamples extends Params {
  final val numSamples: IntParam = new IntParam(
    this,
    "numSamples",
    "Number of samples to generate.",
    ParamValidators.gt(0)
  )

  final def getNumSamples: Int = $(numSamples)

  final def getNumSamplesOpt: Option[Int] = this.get(numSamples)

  final def setNumSamples(value: Int): this.type = this.set(numSamples, value)
}

trait HasSamplingFraction extends Params {
  val samplingFraction = new DoubleParam(
    this,
    "samplingFraction",
    "The fraction of superpixels (for image) or tokens (for text) to keep on",
    ParamValidators.inRange(0, 1)
  )

  def getSamplingFraction: Double = $(samplingFraction)

  def setSamplingFraction(d: Double): this.type = set(samplingFraction, d)
}

trait HasExplainTarget extends Params {
  final val targetCol: Param[String] = new Param[String](
    this,
    "targetCol",
    "The column name of the prediction target to explain (i.e. the response variable). " +
      "This is usually set to \"prediction\" for regression models and " +
      "\"probability\" for probabilistic classification models. Default value: probability")
  setDefault(targetCol, "probability")

  final def getTargetCol: String = $(targetCol)
  final def setTargetCol(value: String): this.type = this.set(targetCol, value)

  final val targetClass: IntParam = new IntParam(
    this,
    "targetClass",
    "The index of the classes for multinomial classification models. Default: 0." +
      "For regression models this parameter is ignored."
  )

  setDefault(targetClass, 0)
  final def getTargetClass: Int = $(targetClass)
  final def setTargetClass(value: Int): this.type = this.set(targetClass, value)

  final val targetClassCol: Param[String] = new Param[String](
    this,
    "targetClassCol",
    "The name of the column that specifies the index of the class for multinomial classification models."
  )

  final def getTargetClassCol: String = $(targetClassCol)
  final def setTargetClassCol(value: String): this.type = this.set(targetClassCol, value)

  def getExplainTarget(schema: StructType): Column = {
    val explainTarget = schema(getTargetCol).dataType match {
      case _: NumericType =>
        col(getTargetCol)
      case VectorType =>
        val classCol = this.get(targetClassCol).map(col).getOrElse(lit(getTargetClass))
        val vectorAccessor = UDFUtils.oldUdf((v: SV, index: Int) => v(index), DoubleType)
        vectorAccessor(col(getTargetCol), classCol)
      case ArrayType(_: NumericType, _) | MapType(_, _: NumericType, _) =>
        val classIndex = this.get(targetClassCol).getOrElse(getTargetClass.toString)
        expr(s"$getTargetCol[cast($classIndex as int)]")
      case other =>
        throw new IllegalArgumentException(
          s"Only numeric types, vector type, array of numeric types and map types with numeric value type " +
            s"are supported as target column. The current type is $other."
        )
    }

    explainTarget
  }
}
