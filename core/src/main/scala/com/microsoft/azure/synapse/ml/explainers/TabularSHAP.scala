// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.explainers

import breeze.stats.distributions.RandBasis
import com.microsoft.azure.synapse.ml.core.schema.DatasetExtensions
import com.microsoft.azure.synapse.ml.logging.FeatureNames
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.ComplexParamsReadable
import org.apache.spark.ml.param.shared.HasInputCols
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

class TabularSHAP(override val uid: String)
  extends KernelSHAPBase(uid)
    with HasInputCols
    with HasBackgroundData {

  logClass(FeatureNames.Explainers)

  def this() = {
    this(Identifiable.randomUID("TabularSHAP"))
  }

  def setInputCols(values: Array[String]): this.type = this.set(inputCols, values)

  def setInputCols(values: Seq[String]): this.type =  setInputCols(values.toArray)

  override protected def createSamples(df: DataFrame,
                                       idCol: String,
                                       coalitionCol: String,
                                       weightCol: String,
                                       targetClassesCol: String): DataFrame = {
    val instanceCol = DatasetExtensions.findUnusedColumnName("instance", df)
    val backgroundCol = DatasetExtensions.findUnusedColumnName("background", df)

    val instances = df.select(col(idCol), col(targetClassesCol), struct(getInputCols.map(col): _*).alias(instanceCol))
    val background = this.getBackgroundData
      .select(struct(getInputCols.map(col): _*).alias(backgroundCol))

    val featureSize = this.getInputCols.length

    val effectiveNumSamples = KernelSHAPBase.getEffectiveNumSamples(this.getNumSamplesOpt, featureSize)

    val sampleType = StructType(this.getInputCols.map {
      feature =>
        df.schema.fields.find(_.name == feature).getOrElse {
          throw new Exception(s"Column $feature not found in schema ${df.schema.simpleString}")
        }
    })

    val infWeightVal = this.getInfWeight
    val samplesUdf = UDFUtils.oldUdf(
      {
        (instance: Row, background: Row) =>
          val sampler = new KernelSHAPTabularSampler(instance, background, effectiveNumSamples, infWeightVal)
          (1 to effectiveNumSamples) map {
            _ =>
              implicit val randBasis: RandBasis = RandBasis.mt0
              sampler.sample
          }
      },
      getSampleSchema(sampleType)
    )

    val samplesCol = DatasetExtensions.findUnusedColumnName("samples", df)

    instances.crossJoin(background)
      .withColumn(samplesCol, explode(samplesUdf(col(instanceCol), col(backgroundCol))))
      .select(
        col(idCol),
        expr(s"$samplesCol.$sampleField.*"),
        col(samplesCol).getField(coalitionField).alias(coalitionCol),
        col(samplesCol).getField(weightField).alias(weightCol),
        col(targetClassesCol)
      )
  }

  override def validateSchema(schema: StructType): Unit = {
    super.validateSchema(schema)

    if (this.get(backgroundData).isDefined) {
      this.getInputCols.foreach {
        col =>
          val inputField: StructField = schema(col)
          val backgroundField = this.getBackgroundData.schema(col)

          require(
            DataType.equalsStructurally(inputField.dataType, backgroundField.dataType, ignoreNullability = true),
            s"Field $col has type ${inputField.dataType} from input instance, but type ${backgroundField.dataType} " +
              s"from background dataset."
          )
      }
    }
  }
}

object TabularSHAP extends ComplexParamsReadable[TabularSHAP]
