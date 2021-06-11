// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.explainers

import breeze.stats.distributions.RandBasis
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.ComplexParamsReadable
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.param.shared.HasInputCol
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types._

trait TextLIMEParams extends LIMEParams with HasSamplingFraction with HasInputCol with HasTokensCol {
  self: TextLIME =>

  def setInputCol(value: String): this.type = this.set(inputCol, value)

  setDefault(numSamples -> 1000, regularization -> 0.0, samplingFraction -> 0.7, tokensCol -> "tokens")
}

class TextLIME(override val uid: String)
  extends LIMEBase(uid)
    with TextLIMEParams
    with TextExplainer {

  logClass()

  def this() = {
    this(Identifiable.randomUID("TextLIME"))
  }

  override protected def createSamples(df: DataFrame,
                                       idCol: String,
                                       stateCol: String,
                                       distanceCol: String
                                      ): DataFrame = {
    val (numSamples, samplingFraction) = (this.getNumSamples, this.getSamplingFraction)

    val samplesUdf = UDFUtils.oldUdf(
      {
        (tokens: Seq[String]) =>
          implicit val randBasis: RandBasis = RandBasis.mt0
          val sampler = new LIMETextSampler(tokens, samplingFraction)
          (1 to numSamples).map {
            _ =>
              val (sampleTokens, features, distance) = sampler.sample
              val sampleText = sampleTokens.mkString(" ")
              (sampleText, features, distance)
          }
      },
      getSampleSchema(StringType)
    )

    df.withColumn("samples", explode(samplesUdf(col(getTokensCol))))
      .select(
        col(idCol),
        col("samples.distance").alias(distanceCol),
        col("samples.state").alias(stateCol),
        col("samples.sample").alias(getInputCol)
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

object TextLIME extends ComplexParamsReadable[TextLIME]
