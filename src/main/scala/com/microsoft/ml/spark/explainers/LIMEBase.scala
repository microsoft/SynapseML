package com.microsoft.ml.spark.explainers

import org.apache.spark.ml.param.shared.{HasInputCol, HasInputCols}
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.DataFrame

trait LIMEParams extends HasNumSamples with HasInputCol {
  self: LocalExplainer =>

  val regularization = new DoubleParam(
    this,
    "regularization",
    "Regularization param for the lasso. Default value: 0.",
    ParamValidators.gt(0)
  )

  val kernelWidth = new DoubleParam(
    this,
    "kernelWidth",
    "Kernel width. Default value: sqrt (number of features) * 0.75",
    ParamValidators.gt(0)
  )

  def getRegularization: Double = $(regularization)

  def setRegularization(v: Double): this.type = set(regularization, v)

  def getKernelWidth: Double = $(kernelWidth)

  def setKernelWidth(v: Double): this.type = set(kernelWidth, v)

  setDefault(numSamples -> 1000, regularization -> 0.0)
}

abstract class LIMEBase(override val uid: String) extends LocalExplainer with LIMEParams {
  override def explain(instances: DataFrame): DataFrame = {
    ???
  }

  override def copy(extra: ParamMap): Params = this.defaultCopy(extra)
}

class TabularLIME(override val uid: String) extends LIMEBase(uid) with HasInputCols {

  def this() = {
    this(Identifiable.randomUID("tablime"))
  }

  val categoricalFeatures = new StringArrayParam(
    this,
    "categoricalFeatures",
    "Name of features that should be treated as categorical variables."
  )

  def getCategoricalFeatures: Array[String] = $(categoricalFeatures)

  def setCategoricalFeatures(values: Array[String]): this.type = this.set(categoricalFeatures, values)

  def setInputCols(values: Array[String]): this.type = this.set(inputCols, values)

  setDefault(categoricalFeatures -> Array.empty)
}

class VectorLIME(override val uid: String) extends LIMEBase(uid) with HasInputCol {
  def this() = {
    this(Identifiable.randomUID("veclime"))
  }

  def setInputCol(value: String): this.type = this.set(inputCol, value)
}