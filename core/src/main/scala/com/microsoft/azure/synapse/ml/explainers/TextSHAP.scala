// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.explainers

import com.microsoft.azure.synapse.ml.core.schema.DatasetExtensions
import com.microsoft.azure.synapse.ml.logging.FeatureNames
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.ComplexParamsReadable
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.param.shared.HasInputCol
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types._

trait TextSHAPParams extends KernelSHAPParams with HasInputCol with HasTokensCol {
  self: TextSHAP =>

  def setInputCol(value: String): this.type = this.set(inputCol, value)

  setDefault(tokensCol -> "tokens")
}

class TextSHAP(override val uid: String)
  extends KernelSHAPBase(uid)
    with TextSHAPParams
    with TextExplainer {

  logClass(FeatureNames.Explainers)

  def this() = {
    this(Identifiable.randomUID("TextSHAP"))
  }

  override protected def createSamples(df: DataFrame,
                                       idCol: String,
                                       coalitionCol: String,
                                       weightCol: String,
                                       targetClassesCol: String): DataFrame = {
    val numSamplesOpt = this.getNumSamplesOpt

    val infWeightVal = this.getInfWeight
    val samplesUdf = UDFUtils.oldUdf(
      {
        (tokens: Seq[String]) =>
          val effectiveNumSamples = KernelSHAPBase.getEffectiveNumSamples(numSamplesOpt, tokens.size)
          val sampler = new KernelSHAPTextSampler(tokens, effectiveNumSamples, infWeightVal)
          (1 to effectiveNumSamples).map {
            _ =>
              val (sampleTokens, features, weight) = sampler.sample
              val sampleText = sampleTokens.mkString(" ")
              (sampleText, features, weight)
          }
      },
      getSampleSchema(StringType)
    )

    val samplesCol = DatasetExtensions.findUnusedColumnName("samples", df)

    df.withColumn(samplesCol, explode(samplesUdf(col(getTokensCol))))
      .select(
        col(idCol),
        col(samplesCol).getField(sampleField).alias(getInputCol),
        col(samplesCol).getField(coalitionField).alias(coalitionCol),
        col(samplesCol).getField(weightField).alias(weightCol),
        col(targetClassesCol)
      )
  }

  override protected def validateSchema(schema: StructType): Unit = {
    super.validateSchema(schema)

    require(
      schema(getInputCol).dataType == StringType,
      s"Field $getInputCol is expected to be string type, but got ${schema(getInputCol).dataType} instead."
    )
  }

  override def transformSchema(schema: StructType): StructType = {
    this.validateSchema(schema)
    schema
      .add(getTokensCol, ArrayType(StringType))
      .add(getOutputCol, VectorType)
  }
}

object TextSHAP extends ComplexParamsReadable[TextSHAP]
