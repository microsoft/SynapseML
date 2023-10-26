// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.stages

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset}

object DropColumns extends DefaultParamsReadable[DropColumns]

/** <code>DropColumns</code> takes a dataframe and a list of columns to drop as input and returns
  * a dataframe comprised of only those columns not listed in the input list.
  *
  */

class DropColumns(val uid: String) extends Transformer with Wrappable with DefaultParamsWritable with SynapseMLLogging {
  logClass(FeatureNames.Core)

  def this() = this(Identifiable.randomUID("DropColumns"))

  val cols: StringArrayParam = new StringArrayParam(this, "cols", "Comma separated list of column names")

  /** @group getParam */
  final def getCols: Array[String] = $(cols)

  /** @group setParam */
  def setCols(value: Array[String]): this.type = set(cols, value)

  def setCol(value: String): this.type = set(cols, Array(value))

  /** @param dataset - The input dataset, to be transformed
    * @return The DataFrame that results from column selection
    */
  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame]({
      dataset.toDF().drop(getCols: _*)
    }, dataset.columns.length)
  }

  def transformSchema(schema: StructType): StructType = {
    val droppedCols = getCols.toSet
    StructType(schema.fields.filter(f => !droppedCols(f.name)))
  }

  def copy(extra: ParamMap): DropColumns = defaultCopy(extra)
}
