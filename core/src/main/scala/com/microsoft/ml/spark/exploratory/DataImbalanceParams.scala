package com.microsoft.ml.spark.exploratory

import org.apache.spark.ml.param.{Param, Params, StringArrayParam}

trait DataImbalanceParams extends Params {
  val sensitiveCols = new StringArrayParam(
    this,
    "sensitiveCols",
    "Sensitive columns to use."
  )

  def getSensitiveCols: Array[String] = $(sensitiveCols)

  def setSensitiveCols(values: Array[String]): this.type = set(sensitiveCols, values)

  val labelCol = new Param[String](
    this,
    "labelCol",
    "Label column to use."
  )

  def getLabelCol: String = $(labelCol)

  def setLabelCol(value: String): this.type = set(labelCol, value)

  val verbose = new Param[Boolean](
    this,
    "verbose",
    "Whether to show intermediate measures and calculations, such as Positive Rate."
  )

  def getVerbose: Boolean = $(verbose)

  def setVerbose(value: Boolean): this.type = set(verbose, value)

  setDefault(verbose -> false)
}
