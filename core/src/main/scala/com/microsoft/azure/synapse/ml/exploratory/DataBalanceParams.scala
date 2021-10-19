package com.microsoft.azure.synapse.ml.exploratory

import org.apache.spark.ml.param.{BooleanParam, Param, Params, StringArrayParam}
import org.apache.spark.sql.types._

trait DataBalanceParams extends Params {
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

  val verbose = new BooleanParam(
    this,
    "verbose",
    "Whether to show intermediate measures and calculations, such as Positive Rate."
  )

  def getVerbose: Boolean = $(verbose)

  def setVerbose(value: Boolean): this.type = set(verbose, value)

  setDefault(
    verbose -> false
  )

  def validateSchema(schema: StructType): Unit = {
    getSensitiveCols.foreach {
      c =>
        schema(c).dataType match {
          case ByteType | ShortType | IntegerType | LongType | StringType =>
          case _ => throw new Exception(s"The sensitive column named $c does not contain integral or string values.")
        }
    }
  }
}
