// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.stages

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.core.contracts.{HasInputCol, HasOutputCol}
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset}

object Explode extends DefaultParamsReadable[Explode]

class Explode(val uid: String) extends Transformer
  with HasInputCol with HasOutputCol with Wrappable with DefaultParamsWritable with SynapseMLLogging {
  logClass(FeatureNames.Core)

  def this() = this(Identifiable.randomUID("Explode"))

  setDefault(outputCol->(this.uid + "_output"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame]({
      transformSchema(dataset.schema)
      dataset.toDF().withColumn(getOutputCol, explode(col(getInputCol)))
    }, dataset.columns.length)
  }

  def transformSchema(schema: StructType): StructType = {
    val innerType = schema(getInputCol).dataType match {
      case ArrayType(it, _) => it
      case _ => throw new IllegalArgumentException("Explode only accepts array columns")
    }
   schema.add(getOutputCol, innerType)
  }

  def copy(extra: ParamMap): Explode = defaultCopy(extra)

}
