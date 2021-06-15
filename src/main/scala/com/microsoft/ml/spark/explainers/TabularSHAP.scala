// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.explainers

import breeze.stats.distributions.RandBasis
import com.microsoft.ml.spark.core.schema.DatasetExtensions
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.ComplexParamsReadable
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.param.shared.HasInputCols
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class TabularSHAP(override val uid: String)
  extends KernelSHAPBase(uid)
    with HasInputCols
    with HasBackgroundData {

  logClass()

  def this() = {
    this(Identifiable.randomUID("TabularSHAP"))
  }

  def setInputCols(values: Array[String]): this.type = this.set(inputCols, values)

  override protected def createSamples(df: DataFrame, idCol: String, coalitionCol: String): DataFrame = {
    val instanceCol = DatasetExtensions.findUnusedColumnName("instance", df)
    val backgroundCol = DatasetExtensions.findUnusedColumnName("background", df)

    val instances = df.select(col(idCol), struct(getInputCols.map(col): _*).alias(instanceCol))
    val background = this.get(backgroundData).getOrElse(df)
      .select(struct(getInputCols.map(col): _*).alias(backgroundCol))

    val featureSize = this.getInputCols.length

    val effectiveNumSamples = KernelSHAPBase.getEffectiveNumSamples(this.getNumSamplesOpt, featureSize)

    val sampleType = StructType(this.getInputCols.map {
      feature =>
        df.schema.fields.find(_.name == feature).getOrElse {
          throw new Exception(s"Column $feature not found in schema ${df.schema.simpleString}")
        }
    })

    val samplesUdf = UDFUtils.oldUdf(
      {
        (instance: Row, background: Row) =>
          val sampler = new KernelSHAPTabularSampler(instance, background, effectiveNumSamples)
          (1 to effectiveNumSamples) map {
            _ =>
              implicit val randBasis: RandBasis = RandBasis.mt0
              sampler.sample
          } map {
            case (sample, state, _) => (sample, state)
          }
      },
      getSampleSchema(sampleType)
    )

    val samplesCol = DatasetExtensions.findUnusedColumnName("samples", df)

    instances.crossJoin(background)
      .withColumn(samplesCol, explode(samplesUdf(col(instanceCol), col(backgroundCol))))
      .select(
        col(idCol),
        col(samplesCol).getField(coalitionField).alias(coalitionCol),
        col(samplesCol).getField(sampleField).getField("*")
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
