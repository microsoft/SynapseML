// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.explainers

import breeze.stats.distributions.RandBasis
import com.microsoft.azure.synapse.ml.core.schema.DatasetExtensions
import com.microsoft.azure.synapse.ml.logging.FeatureNames
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.ComplexParamsReadable
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param.shared.HasInputCol
import org.apache.spark.ml.stat.Summarizer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row}

class VectorLIME(override val uid: String)
  extends LIMEBase(uid) with HasInputCol with HasBackgroundData {

  logClass(FeatureNames.Explainers)

  def this() = {
    this(Identifiable.randomUID("VectorLIME"))
  }

  def setInputCol(value: String): this.type = this.set(inputCol, value)

  private implicit val randBasis: RandBasis = RandBasis.mt0

  override protected def createSamples(df: DataFrame,
                                       idCol: String,
                                       stateCol: String,
                                       distanceCol: String,
                                       targetClassesCol: String): DataFrame = {
    val numSamples = this.getNumSamples

    val featureStats = this.createFeatureStats(this.getBackgroundData)

    val samplesUdf = UDFUtils.oldUdf(
      {
        vector: Vector =>
          implicit val randBasis: RandBasis = RandBasis.mt0
          val sampler = new LIMEVectorSampler(vector, featureStats)
          (1 to numSamples).map(_ => sampler.sample)
      },
      getSampleSchema(VectorType)
    )

    val samplesCol = DatasetExtensions.findUnusedColumnName("samples", df)

    df.withColumn(samplesCol, explode(samplesUdf(col(getInputCol))))
      .select(
        col(idCol),
        col(samplesCol).getField(distanceField).alias(distanceCol),
        col(samplesCol).getField(stateField).alias(stateCol),
        col(samplesCol).getField(sampleField).alias(getInputCol),
        col(targetClassesCol)
      )
  }

  private def createFeatureStats(df: DataFrame): Seq[ContinuousFeatureStats] = {
    val Row(std: Vector) = df
      .select(Summarizer.metrics("std").summary(col($(inputCol))).as("summary"))
      .select("summary.std")
      .first()

    std.toArray.map(ContinuousFeatureStats)
  }

  override protected def validateSchema(schema: StructType): Unit = {
    super.validateSchema(schema)

    require(
      schema(getInputCol).dataType == VectorType,
      s"Field $getInputCol is expected to be vector type, but got ${schema(getInputCol).dataType} instead."
    )

    if (this.get(backgroundData).isDefined) {
      val dataType = getBackgroundData.schema(getInputCol).dataType

      require(
        dataType == VectorType,
        s"Field $getInputCol from background dataset must be Vector type, but got $dataType instead."
      )
    }
  }
}

object VectorLIME extends ComplexParamsReadable[VectorLIME]
