// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.stages

import com.microsoft.ml.spark.codegen.Wrappable
import com.microsoft.ml.spark.core.contracts.{HasInputCol, HasOutputCol}
import com.microsoft.ml.spark.logging.BasicLogging
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

object RenameColumn extends DefaultParamsReadable[RenameColumn]

/** <code>RenameColumn</code> takes a dataframe with an input and an output column name
  * and returns a dataframe comprised of the original columns with the input column renamed
  * as the output column name.
  */
class RenameColumn(val uid: String) extends Transformer with Wrappable with DefaultParamsWritable
  with HasInputCol with HasOutputCol with BasicLogging {
  logClass()

  def this() = this(Identifiable.randomUID("RenameColumn"))

  /** @param dataset - The input dataset, to be transformed
    * @return The DataFrame that results from renaming the input column
    */
  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame]({
      transformSchema(dataset.schema, logging = true)
      dataset.toDF().withColumnRenamed(getInputCol, getOutputCol)
    })
  }

  def validateAndTransformSchema(schema: StructType): StructType = {
    val col = schema(getInputCol)
    schema.add(StructField(getOutputCol, col.dataType, col.nullable, col.metadata))
  }

  def transformSchema(schema: StructType): StructType = validateAndTransformSchema(schema)

  def copy(extra: ParamMap): RenameColumn = defaultCopy(extra)

}
