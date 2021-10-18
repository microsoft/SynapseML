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

  setDefault(
    featureNameCol -> "FeatureName",
    distributionMeasuresCol -> "DistributionMeasures"
  )

  val uniformDistribution: Int => String => Double = {
    n: Int => {
      value: String =>
        1d / n
    }
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    validateSchema(dataset.schema)

    val df = dataset.cache
    val numRows = df.count.toDouble

    val featureCountCol = DatasetExtensions.findUnusedColumnName("featureCount", df.schema)
    val dfCountCol = DatasetExtensions.findUnusedColumnName("dfCount", df.schema)
    val featureProbCol = DatasetExtensions.findUnusedColumnName("featureProb", df.schema)

    val featureStats = df
      .groupBy(getSensitiveCols map col: _*)
      .agg(count("*").cast(DoubleType).alias(featureCountCol))
      .withColumn(dfCountCol, lit(numRows))
      // P(sensitive)
      .withColumn(featureProbCol, col(featureCountCol) / col(dfCountCol))

    if (getVerbose)
      featureStats.cache.show(numRows = 20, truncate = false)

    // TODO: Introduce a referenceDistribution function param for user to override the uniform distribution
    val referenceDistribution = uniformDistribution

    calculateDistributionMeasures(featureStats, featureProbCol, featureCountCol, numRows, referenceDistribution)
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

        val metrics = DistributionMetrics(obsFeatureProbCol, obsFeatureCountCol, refFeatureProbCol, refFeatureCountCol)
        val metricsCols = metrics.toColumnMap.values.toSeq

        // Special case: chi-squared p-value is calculated using breeze.stats.distributions.ChiSquared
        val (obsFeatureCounts, refFeatureCounts) = observedWithRef.select(obsFeatureCountCol, refFeatureCountCol).collect
          .map(r => (r.getDouble(0), r.getDouble(1))).unzip

        observedWithRef
          .agg(metricsCols.head, metricsCols.tail: _*)
          .withColumn(metrics.chiSqPValueCol, metrics.chiSqPValue(numFeatures, obsFeatureCounts, refFeatureCounts))
          .withColumn(getFeatureNameCol, lit(sensitiveCol))
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
                               refFeatureProbCol: String,
                               refFeatureCountCol: String) {
  val absDiffObsRef: Column = abs(col(obsFeatureProbCol) - col(refFeatureProbCol))

  def toColumnMap: Map[String, Column] = Map(
    "kl_divergence" -> klDivergence.alias("kl_divergence"),
    "js_dist" -> jsDistance.alias("js_dist"),
    "inf_norm_dist" -> infNormDistance.alias("inf_norm_dist"),
    "total_variation_dist" -> totalVariationDistance.alias("total_variation_dist"),
    "wasserstein_dist" -> wassersteinDistance.alias("wasserstein_dist"),
    "chi_sq_stat" -> chiSqTestStatistic.alias("chi_sq_stat")
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
  def chiSqTestStatistic: Column =
    sum(pow(col(obsFeatureCountCol) - col(refFeatureCountCol), 2) / col(refFeatureCountCol))

  val chiSqPValueCol = "chi_sq_p_value"

  // Calculates left-tailed p-value from degrees of freedom and chi-squared test statistic
  def chiSqPValue(numFeatures: Int, obsFeatureCounts: Array[Double], refFeatureCounts: Array[Double]): Column = {
    val degOfFreedom = numFeatures - 1
    val score = (obsFeatureCounts, refFeatureCounts).zipped.map((a, b) => scala.math.pow(a - b, 2) / b).sum
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
