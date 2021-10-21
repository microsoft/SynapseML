// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.exploratory.imbalance

import breeze.stats.distributions.ChiSquared
import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.core.schema.DatasetExtensions
import com.microsoft.azure.synapse.ml.logging.BasicLogging
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable, Transformer}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.language.postfixOps

/** This transformer computes data imbalance measures based on a reference distribution.
  *
  * The output is a dataframe that contains two columns:
  *   - The sensitive feature name.
  *   - A struct containing measure names and their values showing differences between
  *   the observed and reference distributions.
  *
  * The output dataframe contains a row per sensitive feature.
  *
  * This feature is experimental. It is subject to change or removal in future releases.
  *
  * @param uid The unique ID.
  */
class DistributionMeasures(override val uid: String)
  extends Transformer
    with ComplexParamsWritable
    with DataBalanceParams
    with Wrappable
    with BasicLogging {

  logClass()

  def this() = this(Identifiable.randomUID("DistributionMeasures"))

  val featureNameCol = new Param[String](
    this,
    "featureNameCol",
    "Output column name for feature names."
  )

  def getFeatureNameCol: String = $(featureNameCol)

  def setFeatureNameCol(value: String): this.type = set(featureNameCol, value)

  val distributionMeasuresCol = new Param[String](
    this,
    "distributionMeasuresCol",
    "Output column name for distribution measures."
  )

  def getDistributionMeasuresCol: String = $(distributionMeasuresCol)

  def setDistributionMeasuresCol(value: String): this.type = set(distributionMeasuresCol, value)

  setDefault(
    featureNameCol -> "FeatureName",
    distributionMeasuresCol -> "DistributionMeasures"
  )

  private val uniformDistribution: Int => String => Double = {
    n: Int => {
      _: String =>
        1d / n
    }
  }

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
        featureStats.cache.show(numRows = 20, truncate = false)

      // TODO (for v2): Introduce a referenceDistribution function param for user to override the uniform distribution
      val referenceDistribution = uniformDistribution

      df.unpersist
      calculateDistributionMeasures(featureStats, featureProbCol, featureCountCol, numRows, referenceDistribution)
    })
  }

  private def calculateDistributionMeasures(featureStats: DataFrame,
                                            obsFeatureProbCol: String,
                                            obsFeatureCountCol: String,
                                            numRows: Double,
                                            referenceDistribution: Int => String => Double): DataFrame = {
    val distributionMeasures = getSensitiveCols.map {
      sensitiveCol =>
        val observed = featureStats
          .groupBy(sensitiveCol)
          .agg(sum(obsFeatureProbCol).alias(obsFeatureProbCol), sum(obsFeatureCountCol).alias(obsFeatureCountCol))

        val numFeatures = observed.count.toInt
        val refDistFunc = udf(referenceDistribution(numFeatures))
        val refFeatureProbCol = DatasetExtensions.findUnusedColumnName("refFeatureProb", featureStats.schema)
        val refFeatureCountCol = DatasetExtensions.findUnusedColumnName("refFeatureCount", featureStats.schema)

        val observedWithRef = observed
          .withColumn(refFeatureProbCol, refDistFunc(col(sensitiveCol)))
          .withColumn(refFeatureCountCol, refDistFunc(col(sensitiveCol)) * lit(numRows))
          .cache

        val metrics =
          DistributionMetrics(numFeatures, obsFeatureProbCol, obsFeatureCountCol, refFeatureProbCol, refFeatureCountCol)
        val metricsCols = metrics.toColumnMap.values.toSeq

        observedWithRef.agg(metricsCols.head, metricsCols.tail: _*).withColumn(getFeatureNameCol, lit(sensitiveCol))
    }.reduce(_ union _)

    if (getVerbose)
      distributionMeasures.cache.show(truncate = false)

    val measureTuples = DistributionMetrics.METRICS.map(col)
    distributionMeasures
      .withColumn(getDistributionMeasuresCol, struct(measureTuples: _*))
      .select(col(getFeatureNameCol), col(getDistributionMeasuresCol))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    validateSchema(schema)

    StructType(
      StructField(getFeatureNameCol, StringType, nullable = false) ::
        StructField(getDistributionMeasuresCol,
          StructType(DistributionMetrics.METRICS.map(StructField(_, DoubleType, nullable = true))), nullable = false) ::
        Nil
    )
  }
}

object DistributionMeasures extends ComplexParamsReadable[DistributionMeasures]

//noinspection SpellCheckingInspection
private[imbalance] object DistributionMetrics {
  val KLDIVERGENCE = "kl_divergence"
  val JSDISTANCE = "js_dist"
  val INFNORMDISTANCE = "inf_norm_dist"
  val TOTALVARIATIONDISTANCE = "total_variation_dist"
  val WASSERSTEINDISTANCE = "wasserstein_dist"
  val CHISQUAREDTESTSTATISTIC = "chi_sq_stat"
  val CHISQUAREDPVALUE = "chi_sq_p_value"

  val METRICS = Seq(KLDIVERGENCE, JSDISTANCE, INFNORMDISTANCE, TOTALVARIATIONDISTANCE, WASSERSTEINDISTANCE,
    CHISQUAREDTESTSTATISTIC, CHISQUAREDPVALUE)
}

//noinspection SpellCheckingInspection
private[imbalance] case class DistributionMetrics(numFeatures: Int,
                               obsFeatureProbCol: String,
                               obsFeatureCountCol: String,
                               refFeatureProbCol: String,
                               refFeatureCountCol: String) {

  import DistributionMetrics._

  val absDiffObsRef: Column = abs(col(obsFeatureProbCol) - col(refFeatureProbCol))

  def toColumnMap: Map[String, Column] = Map(
    KLDIVERGENCE -> klDivergence.alias(KLDIVERGENCE),
    JSDISTANCE -> jsDistance.alias(JSDISTANCE),
    INFNORMDISTANCE -> infNormDistance.alias(INFNORMDISTANCE),
    TOTALVARIATIONDISTANCE -> totalVariationDistance.alias(TOTALVARIATIONDISTANCE),
    WASSERSTEINDISTANCE -> wassersteinDistance.alias(WASSERSTEINDISTANCE),
    CHISQUAREDTESTSTATISTIC -> chiSquaredTestStatistic.alias(CHISQUAREDTESTSTATISTIC),
    CHISQUAREDPVALUE -> chiSquaredPValue.alias(CHISQUAREDPVALUE)
  )

  def klDivergence: Column = entropy(col(obsFeatureProbCol), Some(col(refFeatureProbCol)))

  def jsDistance: Column = {
    val averageObsRef = (col(obsFeatureProbCol) + col(refFeatureProbCol)) / 2d
    val entropyObsAvg = entropy(col(obsFeatureProbCol), Some(averageObsRef))
    val entropyRefAvg = entropy(col(refFeatureProbCol), Some(averageObsRef))
    sqrt((entropyRefAvg + entropyObsAvg) / 2d)
  }

  def infNormDistance: Column = max(absDiffObsRef)

  def totalVariationDistance: Column = sum(absDiffObsRef) * 0.5d

  // Calculates the 1st Wasserstein Distance (p = 1)
  def wassersteinDistance: Column = {
    // Typically, we sort the two distributions before finding their difference
    // Because we know the reference distribution consists of the same value, we can skip this step
    mean(abs(col(obsFeatureProbCol) - col(refFeatureProbCol)))
  }

  // Calculates Pearson's chi-squared statistic
  def chiSquaredTestStatistic: Column =
    sum(pow(col(obsFeatureCountCol) - col(refFeatureCountCol), 2) / col(refFeatureCountCol))

  // Calculates left-tailed p-value from degrees of freedom and chi-squared test statistic
  def chiSquaredPValue: Column = {
    val degOfFreedom = numFeatures - 1
    val scoreCol = chiSquaredTestStatistic
    val chiSqPValueUdf = udf({
      score: Double =>
        1d - ChiSquared(degOfFreedom).cdf(score)
    })
    chiSqPValueUdf(scoreCol)
  }

  private def entropy(distA: Column, distB: Option[Column] = None): Column = {
    if (distB.isDefined) {
      sum(distA * log(distA / distB.get))
    } else {
      sum(distA * log(distA)) * -1d
    }
  }
}
