// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.exploratory

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.core.schema.DatasetExtensions
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable, Transformer}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/** This transformer computes a set of aggregated balance measures that represents how balanced
  * the given dataframe is along the given sensitive features.
  *
  * The output is a dataframe that contains one column:
  *   - A struct containing measure names and their values showing higher notions of inequality.
  *     The following measures are computed:
  *     - Atkinson Index - https://en.wikipedia.org/wiki/Atkinson_index
  *     - Theil Index (L and T) - https://en.wikipedia.org/wiki/Theil_index
  *
  * The output dataframe contains one row.
  *
  * @param uid The unique ID.
  */
@org.apache.spark.annotation.Experimental
class AggregateBalanceMeasure(override val uid: String)
  extends Transformer
    with DataBalanceParams
    with ComplexParamsWritable
    with Wrappable
    with SynapseMLLogging {

  logClass(FeatureNames.Core)

  def this() = this(Identifiable.randomUID("AggregateBalanceMeasure"))

  val epsilon = new DoubleParam(
    this,
    "epsilon",
    "Epsilon value for Atkinson Index. Inverse of alpha (1 - alpha)."
  )

  def getEpsilon: Double = $(epsilon)

  def setEpsilon(value: Double): this.type = set(epsilon, value)

  val errorTolerance = new DoubleParam(
    this,
    "errorTolerance",
    "Error tolerance value for Atkinson Index."
  )

  def getErrorTolerance: Double = $(errorTolerance)

  def setErrorTolerance(value: Double): this.type = set(errorTolerance, value)

  setDefault(
    outputCol -> "AggregateBalanceMeasure",
    epsilon -> 1d,
    errorTolerance -> 1e-12
  )

  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame]({
      validateSchema(dataset.schema)

      val df = dataset.cache
      val numRows = df.count.toDouble

      val featureCountCol = DatasetExtensions.findUnusedColumnName("featureCount", df.schema)
      val rowCountCol = DatasetExtensions.findUnusedColumnName("rowCount", df.schema)
      val featureProbCol = DatasetExtensions.findUnusedColumnName("featureProb", df.schema)

      val featureStats = df
        .groupBy(getSensitiveCols map col: _*)
        .agg(count("*").cast(DoubleType).alias(featureCountCol))
        .withColumn(rowCountCol, lit(numRows))
        .withColumn(featureProbCol, col(featureCountCol) / col(rowCountCol)) // P(sensitive)

      //noinspection ScalaStyle
      if (getVerbose)
        featureStats.cache.show(numRows = 20, truncate = false)  //scalastyle:ignore magic.number

      df.unpersist
      calculateAggregateMeasures(featureStats, featureProbCol)
    }, dataset.columns.length)
  }

  private def calculateAggregateMeasures(featureStats: DataFrame, featureProbCol: String): DataFrame = {
    val Row(numFeatures: Double, meanFeatures: Double) =
      featureStats.agg(count("*").cast(DoubleType), mean(featureProbCol).cast(DoubleType)).head

    val metricsCols = AggregateMetrics(
      featureProbCol, numFeatures, meanFeatures, getEpsilon, getErrorTolerance).toColumnMap.values.toSeq
    val aggDf = featureStats.agg(metricsCols.head, metricsCols.tail: _*)

    if (getVerbose)
      aggDf.cache.show(truncate = false)

    val measureTuples = AggregateMetrics.METRICS.map(col)
    aggDf.withColumn(getOutputCol, struct(measureTuples: _*)).select(getOutputCol)
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    validateSchema(schema)

    StructType(
      StructField(getOutputCol,
        StructType(AggregateMetrics.METRICS.map(StructField(_, DoubleType, nullable = true))), nullable = false) ::
        Nil
    )
  }
}

object AggregateBalanceMeasure extends ComplexParamsReadable[AggregateBalanceMeasure]

//noinspection SpellCheckingInspection
private[exploratory] object AggregateMetrics {
  val ATKINSONINDEX = "atkinson_index"
  val THEILLINDEX = "theil_l_index"
  val THEILTINDEX = "theil_t_index"

  val METRICS: Seq[String] = Seq(ATKINSONINDEX, THEILLINDEX, THEILTINDEX)
}

//noinspection SpellCheckingInspection
private[exploratory] case class AggregateMetrics(featureProbCol: String,
                                                 numFeatures: Double,
                                                 meanFeatures: Double,
                                                 epsilon: Double,
                                                 errorTolerance: Double) {

  import AggregateMetrics._

  private val normFeatureProbCol = col(featureProbCol) / meanFeatures

  def toColumnMap: Map[String, Column] = Map(
    ATKINSONINDEX -> atkinsonIndex.alias(ATKINSONINDEX),
    THEILLINDEX -> theilLIndex.alias(THEILLINDEX),
    THEILTINDEX -> theilTIndex.alias(THEILTINDEX)
  )

  def atkinsonIndex: Column = {
    val alpha = 1d - epsilon
    val productExpression = exp(sum(log(normFeatureProbCol)))
    val powerMeanExpression = sum(pow(normFeatureProbCol, alpha)) / numFeatures
    when(
      abs(lit(alpha)) < errorTolerance,
      lit(1d) - pow(productExpression, 1d / numFeatures)
    ).otherwise(
      lit(1d) - pow(powerMeanExpression, 1d / alpha)
    )
  }

  def theilLIndex: Column = {
    val negativeSumLog = sum(log(normFeatureProbCol) * -1d)
    negativeSumLog / numFeatures
  }

  def theilTIndex: Column = {
    val sumLog = sum(normFeatureProbCol * log(normFeatureProbCol))
    sumLog / numFeatures
  }
}
