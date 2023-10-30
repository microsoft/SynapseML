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
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types._

class VectorSHAP(override val uid: String)
  extends KernelSHAPBase(uid)
    with HasInputCol
    with HasBackgroundData {

  logClass(FeatureNames.Explainers)

  def this() = {
    this(Identifiable.randomUID("VectorSHAP"))
  }

  def setInputCol(value: String): this.type = this.set(inputCol, value)

  override protected def createSamples(df: DataFrame,
                                       idCol: String,
                                       coalitionCol: String,
                                       weightCol: String,
                                       targetClassesCol: String): DataFrame = {
    val instanceCol = DatasetExtensions.findUnusedColumnName("instance", df)
    val backgroundCol = DatasetExtensions.findUnusedColumnName("background", df)

    val instances = df.select(col(idCol), col(targetClassesCol), col(getInputCol).alias(instanceCol))
    val background = this.getBackgroundData
      .select(col(getInputCol).alias(backgroundCol))

    val numSampleOpt = this.getNumSamplesOpt
    val infWeightVal = this.getInfWeight
    val samplesUdf = UDFUtils.oldUdf(
      {
        (instance: Vector, background: Vector) =>
          val effectiveNumSamples = KernelSHAPBase.getEffectiveNumSamples(numSampleOpt, instance.size)
          val sampler = new KernelSHAPVectorSampler(instance, background, effectiveNumSamples, infWeightVal)
          (1 to effectiveNumSamples) map {
            _ =>
              implicit val randBasis: RandBasis = RandBasis.mt0
              sampler.sample
          }
      },
      getSampleSchema(VectorType)
    )

    val samplesCol = DatasetExtensions.findUnusedColumnName("samples", df)

    instances.crossJoin(background)
      .withColumn(samplesCol, explode(samplesUdf(col(instanceCol), col(backgroundCol))))
      .select(
        col(idCol),
        col(samplesCol).getField(sampleField).alias(getInputCol),
        col(samplesCol).getField(coalitionField).alias(coalitionCol),
        col(samplesCol).getField(weightField).alias(weightCol),
        col(targetClassesCol)
      )
  }

  protected override def validateSchema(schema: StructType): Unit = {
    super.validateSchema(schema)

    require(
      schema(getInputCol).dataType == VectorType,
      s"Field $getInputCol from input must be Vector type, but got ${schema(getInputCol).dataType} instead."
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

object VectorSHAP extends ComplexParamsReadable[VectorSHAP]
