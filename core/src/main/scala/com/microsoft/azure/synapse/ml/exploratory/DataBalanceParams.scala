// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.exploratory

import org.apache.spark.ml.param.shared.HasOutputCol
import org.apache.spark.ml.param.{BooleanParam, Params, StringArrayParam}
import org.apache.spark.sql.types._

trait DataBalanceParams extends Params with HasOutputCol {
  val sensitiveCols = new StringArrayParam(
    this,
    "sensitiveCols",
    "Sensitive columns to use."
  )

  def getSensitiveCols: Array[String] = $(sensitiveCols)

  def setSensitiveCols(values: Array[String]): this.type = set(sensitiveCols, values)

  val verbose = new BooleanParam(
    this,
    "verbose",
    "Whether to show intermediate measures and calculations, such as Positive Rate."
  )

  def getVerbose: Boolean = $(verbose)

  def setVerbose(value: Boolean): this.type = set(verbose, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

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
