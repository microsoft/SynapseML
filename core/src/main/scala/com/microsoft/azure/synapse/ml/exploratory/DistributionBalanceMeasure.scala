// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.exploratory

import breeze.stats.distributions.{ChiSquared, RandBasis}
import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.core.schema.DatasetExtensions
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import com.microsoft.azure.synapse.ml.param.ArrayMapParam
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable, Transformer}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.util
import scala.collection.JavaConverters._
import scala.language.postfixOps

/** This transformer computes data balance measures based on a reference distribution.
  * For now, we only support a uniform reference distribution.
  *
  * The output is a dataframe that contains two columns:
  *   - The sensitive feature name.
  *   - A struct containing measure names and their values showing differences between
  *     the observed and reference distributions. The following measures are computed:
  *     - Kullback-Leibler Divergence - https://en.wikipedia.org/wiki/Kullback%E2%80%93Leibler_divergence
  *     - Jensen-Shannon Distance - https://en.wikipedia.org/wiki/Jensen%E2%80%93Shannon_divergence
  *     - Wasserstein Distance - https://en.wikipedia.org/wiki/Wasserstein_metric
  *     - Infinity Norm Distance - https://en.wikipedia.org/wiki/Chebyshev_distance
  *     - Total Variation Distance - https://en.wikipedia.org/wiki/Total_variation_distance_of_probability_measures
  *     - Chi-Squared Test - https://en.wikipedia.org/wiki/Chi-squared_test
  *
  * The output dataframe contains a row per sensitive feature.
  *
  * @param uid The unique ID.
  */
@org.apache.spark.annotation.Experimental
class DistributionBalanceMeasure(override val uid: String)
  extends Transformer
    with DataBalanceParams
    with ComplexParamsWritable
    with Wrappable
    with SynapseMLLogging {

  logClass(FeatureNames.Core)

  def this() = this(Identifiable.randomUID("DistributionBalanceMeasure"))

  val featureNameCol = new Param[String](
    this,
    "featureNameCol",
    "Output column name for feature names."
  )

  def getFeatureNameCol: String = $(featureNameCol)

  def setFeatureNameCol(value: String): this.type = set(featureNameCol, value)

  val referenceDistribution = new ArrayMapParam(
    this,
    "referenceDistribution",
    "An ordered list of reference distributions that correspond to each of the sensitive columns."
  )

  val emptyReferenceDistribution: Array[Map[String, Double]] = Array.empty

  def getReferenceDistribution: Array[Map[String, Double]] =
    if (isDefined(referenceDistribution))
      $(referenceDistribution).map(_.mapValues(_.asInstanceOf[Double]).map(identity))
    else emptyReferenceDistribution

  def setReferenceDistribution(value: Array[Map[String, Double]]): this.type =
    set(referenceDistribution, value.map(_.mapValues(_.asInstanceOf[Any])))

  def setReferenceDistribution(value: util.ArrayList[util.HashMap[String, Double]]): this.type = {
    val arrayMap = value.asScala.toArray.map(_.asScala.toMap.mapValues(_.asInstanceOf[Any]))
    set(referenceDistribution, arrayMap)
  }

  setDefault(
    featureNameCol -> "FeatureName",
    outputCol -> "DistributionBalanceMeasure"
  )

  private val uniformDistribution: Int => String => Double = {
    n: Int => {
      _: String =>
        1d / n
    }
  }

  private val customDistribution: Map[String, Double] => String => Double = {
    dist: Map[String, Double] => {
      // NOTE: If the custom distribution doesn't have the col value, return a default probability of 0
      // This assumes that the reference distribution does not contain the col value at all
      s: String =>
        dist.getOrElse(s, 0d)
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
        featureStats.cache.show(numRows = 20, truncate = false)  //scalastyle:ignore magic.number

      df.unpersist
      calculateDistributionMeasures(featureStats, featureProbCol, featureCountCol, numRows)
    }, dataset.columns.length)
  }

  private def calculateDistributionMeasures(featureStats: DataFrame,
                                            obsFeatureProbCol: String,
                                            obsFeatureCountCol: String,
                                            numRows: Double): DataFrame = {
    val distributionMeasures = getSensitiveCols.zipWithIndex.map {
      case (sensitiveCol, i) =>
        val observed = featureStats
          .groupBy(sensitiveCol)
          .agg(sum(obsFeatureProbCol).alias(obsFeatureProbCol), sum(obsFeatureCountCol).alias(obsFeatureCountCol))

        val numFeatures = observed.count.toInt
        val refFeatureProbCol = DatasetExtensions.findUnusedColumnName("refFeatureProb", featureStats.schema)
        val refFeatureCountCol = DatasetExtensions.findUnusedColumnName("refFeatureCount", featureStats.schema)

        val refDist: String => Double =
          if (!isDefined(referenceDistribution) || getReferenceDistribution(i).isEmpty) uniformDistribution(numFeatures)
          else customDistribution(getReferenceDistribution(i))
        val refDistFunc = udf(refDist)

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
      .withColumn(getOutputCol, struct(measureTuples: _*))
      .select(col(getFeatureNameCol), col(getOutputCol))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    validateSchema(schema)

    StructType(
      StructField(getFeatureNameCol, StringType, nullable = false) ::
        StructField(getOutputCol,
          StructType(DistributionMetrics.METRICS.map(StructField(_, DoubleType, nullable = true))), nullable = false) ::
        Nil
    )
  }

  override def validateSchema(schema: StructType): Unit = {
    super.validateSchema(schema)

    if (isDefined(referenceDistribution) && getReferenceDistribution.length != getSensitiveCols.length) {
      throw new Exception("The reference distribution must have the same length and order as the sensitive columns: "
        + getSensitiveCols.mkString(", "))
    }
  }
}

object DistributionBalanceMeasure extends ComplexParamsReadable[DistributionBalanceMeasure]

//noinspection SpellCheckingInspection
private[exploratory] object DistributionMetrics {
  val KLDIVERGENCE = "kl_divergence"
  val JSDISTANCE = "js_dist"
  val INFNORMDISTANCE = "inf_norm_dist"
  val TOTALVARIATIONDISTANCE = "total_variation_dist"
  val WASSERSTEINDISTANCE = "wasserstein_dist"
  val CHISQUAREDTESTSTATISTIC = "chi_sq_stat"
  val CHISQUAREDPVALUE = "chi_sq_p_value"

  val METRICS: Seq[String] = Seq(
    KLDIVERGENCE,
    JSDISTANCE,
    INFNORMDISTANCE,
    TOTALVARIATIONDISTANCE,
    WASSERSTEINDISTANCE,
    CHISQUAREDTESTSTATISTIC,
    CHISQUAREDPVALUE)
}

//noinspection SpellCheckingInspection
private[exploratory] case class DistributionMetrics(numFeatures: Int,
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
  def chiSquaredTestStatistic: Column = sum(
    // If expected is zero and observed is not zero, the test assumes observed is impossible so Chi^2 value becomes +inf
    when(col(refFeatureCountCol) === 0 && col(obsFeatureCountCol) =!= 0, lit(Double.PositiveInfinity))
      .otherwise(pow(col(obsFeatureCountCol) - col(refFeatureCountCol), 2) / col(refFeatureCountCol)))

  // Calculates left-tailed p-value from degrees of freedom and chi-squared test statistic
  def chiSquaredPValue: Column = {
    implicit val rand: RandBasis = RandBasis.mt0
    val degOfFreedom = numFeatures - 1
    val scoreCol = chiSquaredTestStatistic
    val chiSqPValueUdf = udf(
      (score: Double) => score match {
        // limit of CDF as x approaches +inf is 1 (https://en.wikipedia.org/wiki/Cumulative_distribution_function)
        case Double.PositiveInfinity => 1d
        case _ => 1 - ChiSquared(degOfFreedom).cdf(score)
      }
    )
    chiSqPValueUdf(scoreCol)
  }

  private def entropy(distA: Column, distB: Option[Column] = None): Column = {
    if (distB.isDefined) {
      // Using same cases as scipy (https://docs.scipy.org/doc/scipy/reference/generated/scipy.special.rel_entr.html)
      val entropies = when(distA === 0d && distB.get >= 0d, lit(0d))
        .when(distA > 0d && distB.get > 0d, distA * log(distA / distB.get))
        .otherwise(lit(Double.PositiveInfinity))
      sum(entropies)
    } else {
      sum(distA * log(distA)) * -1d
    }
  }
}
