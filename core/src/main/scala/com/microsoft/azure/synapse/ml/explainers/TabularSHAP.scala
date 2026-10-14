// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.explainers

import com.microsoft.azure.synapse.ml.core.schema.DatasetExtensions
import com.microsoft.azure.synapse.ml.logging.FeatureNames
import org.apache.spark.ml.ComplexParamsReadable
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.param.shared.HasInputCols
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.functions.{col, struct}
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
    val pairs = instances.crossJoin(background)
    val outputSchema = StructType(
      Seq(df.schema(idCol)) ++
        sampleType.fields.map(_.copy(nullable = true)) ++
        Seq(
          StructField(coalitionCol, VectorType),
          StructField(weightCol, DoubleType),
          df.schema(targetClassesCol)
        )
    )
    implicit val outputEncoder: ExpressionEncoder[Row] = ExpressionEncoder(outputSchema)

    pairs.mapPartitions {
      rows =>
        TabularSHAP.expandPairs(rows, effectiveNumSamples) {
          (instance, background) =>
            new KernelSHAPTabularSampler(instance, background, effectiveNumSamples, infWeightVal)
        }
    }
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

object TabularSHAP extends ComplexParamsReadable[TabularSHAP] {
  private[explainers] def sampleIterator[T](numSamples: Int)(sample: => T): Iterator[T] = {
    Iterator.fill(numSamples)(()).map(_ => sample)
  }

  /** Expands (id, targetClasses, instance, background) pair rows into sample rows one at a time. */
  private[explainers] def expandPairs(pairs: Iterator[Row], numSamples: Int)
                                     (newSampler: (Row, Row) => KernelSHAPSampler[Row]): Iterator[Row] = {
    pairs.flatMap {
      row =>
        val id = row.get(0)
        val targetClasses = row.get(1)
        val sampler = newSampler(row.getAs[Row](2), row.getAs[Row](3))

        sampleIterator(numSamples)(sampler.sample).map {
          case (sample, coalition, weight) =>
            Row.fromSeq(Seq(id) ++ sample.toSeq ++ Seq(coalition, weight, targetClasses))
        }
    }
  }
}
