// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.stages

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{BooleanParam, ParamMap}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}

class Cacher(val uid: String) extends Transformer with Wrappable with DefaultParamsWritable with SynapseMLLogging {
  logClass(FeatureNames.Core)

  val disable = new BooleanParam(this,
    "disable", "Whether or disable caching (so that you can turn it off during evaluation)")

  def getDisable: Boolean = $(disable)

  def setDisable(value: Boolean): this.type = set(disable, value)

  setDefault(disable->false)

  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame]({
      if (!getDisable) {
        dataset.toDF.cache()
      } else {
        dataset.toDF
      }
    }, dataset.columns.length)
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = schema

  def this() = this(Identifiable.randomUID("Cacher"))
}

object Cacher extends DefaultParamsReadable[Cacher]
