package com.microsoft.ml.spark.exploratory

import breeze.stats.distributions.ChiSquared
import com.microsoft.ml.spark.codegen.Wrappable
import com.microsoft.ml.spark.core.schema.DatasetExtensions
import com.microsoft.ml.spark.logging.BasicLogging
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{ComplexParamsWritable, Transformer}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.language.postfixOps

class DistributionMeasures(override val uid: String)
  extends Transformer
    with ComplexParamsWritable
    with DataImbalanceParams
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

  val referenceDistribution = new DataFrameParam(
    this,
    "referenceDistribution",
    "The reference distribution to use. If not set, the uniform distribution is used."
  )

  def getReferenceDistribution: DataFrame = $(referenceDistribution)

  def setReferenceDistribution(value: DataFrame): this.type = set(referenceDistribution, value)

  setDefault(
    featureNameCol -> "FeatureName",
    distributionMeasuresCol -> "DistributionMeasures"
  )

  override def transform(dataset: Dataset[_]): DataFrame = {
    validateSchema(dataset.schema)

    val df = dataset.cache
    val numRows = df.count.toDouble

    val featureCountCol = DatasetExtensions.findUnusedColumnName("featureCount", df.schema)
    val dfCountCol = DatasetExtensions.findUnusedColumnName("dfCount", df.schema)
    val featureProbCol = DatasetExtensions.findUnusedColumnName("featureProb", df.schema)

    val featureCountsAndProbabilities = df
      .groupBy(getSensitiveCols map col: _*)
      .agg(count("*").cast(DoubleType).alias(featureCountCol))
      .withColumn(dfCountCol, lit(numRows))
      // P(sensitive)
      .withColumn(featureProbCol, col(featureCountCol) / col(dfCountCol))

    if (getVerbose)
      featureCountsAndProbabilities.cache.show(numRows = 20, truncate = false)

    calculateDistributionMeasures(featureCountsAndProbabilities, featureProbCol, featureCountCol, numRows)
  }

  private def calculateDistributionMeasures(featureCountsAndProbabilities: DataFrame,
                                            obsFeatureProbCol: String,
                                            obsFeatureCountCol: String,
                                            numRows: Double): DataFrame = {
    val distributionMeasures = getSensitiveCols.map {
      sensitiveCol =>
        val observations = featureCountsAndProbabilities
          .groupBy(sensitiveCol)
          .agg(sum(obsFeatureProbCol).alias(obsFeatureProbCol), sum(obsFeatureCountCol).alias(obsFeatureCountCol))
          .cache

        // For calculating p-value
        val numFeatures = observations.count.toDouble
        val obsFeatureCounts = observations.select(obsFeatureCountCol).collect().map(_.getDouble(0))

        // Only use uniform distribution if user didn't setReferenceDistribution
        val (refFeatureProb, refFeatureCount) = (1d / numFeatures, numRows / numFeatures)

        val metricsMap = DistributionMetrics(
          obsFeatureProbCol, obsFeatureCountCol, refFeatureProb, refFeatureCount, numFeatures, obsFeatureCounts
        ).toColumnMap
        val metricsCols = metricsMap.values.toSeq

        observations.agg(metricsCols.head, metricsCols.tail: _*).withColumn(getFeatureNameCol, lit(sensitiveCol))
    }.reduce(_ union _)

    if (getVerbose)
      distributionMeasures.cache.show(truncate = false)

    val measureTuples = distributionMeasures.schema.fieldNames.filterNot(_ == getFeatureNameCol).flatMap {
      metricName =>
        lit(metricName) :: col(metricName) :: Nil
    }.toSeq

    distributionMeasures
      .withColumn(getDistributionMeasuresCol, map(measureTuples: _*))
      .select(col(getFeatureNameCol), col(getDistributionMeasuresCol))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    validateSchema(schema)

    StructType(
      StructField(getFeatureNameCol, StringType, nullable = false) ::
        StructField(
          getDistributionMeasuresCol, MapType(StringType, DoubleType, valueContainsNull = true), nullable = false) ::
        Nil
    )
  }
}

case class DistributionMetrics(obsFeatureProbCol: String,
                               obsFeatureCountCol: String,
                               refFeatureProb: Double,
                               refFeatureCount: Double,
                               numFeatures: Double,
                               obsFeatureCounts: Array[Double]) {
  val absDiffObsRef: Column = abs(col(obsFeatureProbCol) - lit(refFeatureProb))

  def toColumnMap: Map[String, Column] = Map(
    "kl_divergence" -> klDivergence.alias("kl_divergence"),
    "js_dist" -> jsDistance.alias("js_dist"),
    "inf_norm_dist" -> infNormDistance.alias("inf_norm_dist"),
    "total_variation_dist" -> totalVariationDistance.alias("total_variation_dist"),
    "wasserstein_dist" -> wassersteinDistance.alias("wasserstein_dist"),
    "chi_sq_stat" -> chiSqTestStatistic.alias("chi_sq_stat"),
    "chi_sq_p_value" -> chiSqPValue.alias("chi_sq_p_value")
  )

  def klDivergence: Column = entropy(col(obsFeatureProbCol), Some(lit(refFeatureProb)))

  def jsDistance: Column = {
    val averageObsRef = (col(obsFeatureProbCol) + lit(refFeatureProb)) / 2d
    val entropyObsAvg = entropy(col(obsFeatureProbCol), Some(averageObsRef))
    val entropyRefAvg = entropy(lit(refFeatureProb), Some(averageObsRef))
    sqrt((entropyRefAvg + entropyObsAvg) / 2d)
  }

  def infNormDistance: Column = max(absDiffObsRef)

  def totalVariationDistance: Column = sum(absDiffObsRef) * 0.5d

  // Calculates the 1st Wasserstein Distance (p = 1)
  def wassersteinDistance: Column = {
    // Typically, we sort the two distributions before finding their difference
    // Because we know the reference distribution consists of the same value, we can skip this step
    mean(abs(col(obsFeatureProbCol) - lit(refFeatureProb)))
  }

  // Calculates Pearson's chi-squared statistic
  def chiSqTestStatistic: Column =
    sum(pow(col(obsFeatureCountCol) - lit(refFeatureCount), 2) / refFeatureCount)

  // Calculates left-tailed p-value from degrees of freedom and chi-squared test statistic
  def chiSqPValue: Column = {
    val degOfFreedom = numFeatures - 1
    val score = obsFeatureCounts.map(o => scala.math.pow(o - refFeatureCount, 2) / refFeatureCount).sum
    lit(1 - ChiSquared(degOfFreedom).cdf(score))
  }

  private def entropy(distA: Column, distB: Option[Column] = None): Column = {
    if (distB.isDefined) {
      sum(distA * log(distA / distB.get))
    } else {
      sum(distA * log(distA)) * -1d
    }
  }
}
