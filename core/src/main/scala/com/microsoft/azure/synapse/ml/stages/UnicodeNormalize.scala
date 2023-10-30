// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.stages

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.core.contracts.{HasInputCol, HasOutputCol}
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import org.apache.spark.ml.param.{BooleanParam, Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable, Transformer}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

import java.text.Normalizer

object UnicodeNormalize extends ComplexParamsReadable[UnicodeNormalize]

/** <code>UnicodeNormalize</code> takes a dataframe and normalizes the unicode representation.
  */
class UnicodeNormalize(val uid: String) extends Transformer
  with HasInputCol with HasOutputCol with Wrappable with ComplexParamsWritable with SynapseMLLogging {
  logClass(FeatureNames.Core)

  def this() = this(Identifiable.randomUID("UnicodeNormalize"))

  val form = new Param[String](this, "form", "Unicode normalization form: NFC, NFD, NFKC, NFKD")

  /** @group getParam */
  def getForm: String = get(form).getOrElse("NFKD")

  /** @group setParam */
  def setForm(value: String): this.type = {
    // check input value
    Normalizer.Form.valueOf(getForm)

    set("form", value)
  }

  val lower = new BooleanParam(this, "lower", "Lowercase text")

  /** @group getParam */
  def getLower: Boolean = get(lower).getOrElse(true)

  /** @group setParam */
  def setLower(value: Boolean): this.type = set("lower", value)

  /** @param dataset - The input dataset, to be transformed
    * @return The DataFrame that results from column selection
    */
  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame]({
      val inputIndex = dataset.columns.indexOf(getInputCol)

      require(inputIndex != -1, s"Input column $getInputCol does not exist")

      val normalizeFunc = (value: String) =>
        if (value == null) null  //scalastyle:ignore null
        else Normalizer.normalize(value, Normalizer.Form.valueOf(getForm))

      val f = if (getLower)
        (value: String) => Option(value).map(s => normalizeFunc(s.toLowerCase)).orNull
      else
        normalizeFunc

      val textMapper = udf(f)

      dataset.withColumn(getOutputCol, textMapper(dataset(getInputCol)).as(getOutputCol))
    }, dataset.columns.length)
  }

  def transformSchema(schema: StructType): StructType = {
    schema.add(StructField(getOutputCol, StringType))
  }

  def copy(extra: ParamMap): UnicodeNormalize = defaultCopy(extra)

}
