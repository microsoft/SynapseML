package com.microsoft.ml.spark.exploratory

import com.microsoft.ml.spark.codegen.Wrappable
import com.microsoft.ml.spark.core.schema.DatasetExtensions
import com.microsoft.ml.spark.logging.BasicLogging
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{ComplexParamsWritable, Transformer}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class DistributionMeasures(override val uid: String)
  extends Transformer
    with ComplexParamsWritable
    with DataImbalanceParams
    with Wrappable
    with BasicLogging {

  override protected lazy val pyInternalWrapper = true

  logClass()

  def this() = this(Identifiable.randomUID("DistributionMeasures"))

  val featureNameCol = "FeatureName"

  val distributionMeasuresCol = new Param[String](
    this,
    "distributionMeasuresCol",
    "Output column name for distribution measures."
  )

  def getDistributionMeasuresCol: String = $(distributionMeasuresCol)

  def setDistributionMeasuresCol(value: String): this.type = set(distributionMeasuresCol, value)

  setDefault(distributionMeasuresCol -> "DistributionMeasures")

  override def transform(dataset: Dataset[_]): DataFrame = {
    validateSchema(dataset.schema)

    val df = dataset.cache
    val numRows = df.count.toDouble

    val countSensitiveCol = DatasetExtensions.findUnusedColumnName("countSensitive", df.schema)
    val countAllCol = DatasetExtensions.findUnusedColumnName("countAll", df.schema)
    val countSensitiveProbCol = DatasetExtensions.findUnusedColumnName("countSensitiveProb", df.schema)

    val benefits = df
      .groupBy(getSensitiveCols map col: _*)
      .agg(count("*").cast(DoubleType).alias(countSensitiveCol))
      .withColumn(countAllCol, lit(numRows))
      .withColumn(countSensitiveProbCol, col(countSensitiveCol) / col(countAllCol))

    if (getVerbose)
      benefits.cache.show(numRows = 20, truncate = false)

    calculateDistributionMeasures(benefits, countSensitiveProbCol, numRows)
  }

  private def calculateDistributionMeasures(benefitsDf: DataFrame, benefitCol: String, numRows: Double): DataFrame = {
    val distributionMeasures = getSensitiveCols.map {
      sensitiveCol =>
        val observations = benefitsDf.groupBy(sensitiveCol).agg(sum(benefitCol).alias(benefitCol))
        val numObservations = observations.count.toDouble
        // We assume uniform distribution, but this should be abstracted into a function.
        val expectedProbability = 1d / numObservations
        val expectedNumObservations = numRows / numObservations

        val metricsMap =
          DistributionMetrics(benefitCol, numObservations, expectedProbability, expectedNumObservations).toMap
        val metricsCols = metricsMap.values.toSeq

        observations.agg(metricsCols.head, metricsCols.tail: _*).withColumn(featureNameCol, lit(sensitiveCol))
    }.reduce(_ union _)

    if (getVerbose)
      distributionMeasures.cache.show(truncate = false)

    // We instantiate a dummy class to access the metric names (keys) from .toMap
    val measureTuples = DistributionMetrics("", 0d, 0d, 0d).toMap.flatMap {
      case (metricName, _) =>
        lit(metricName) :: col(metricName) :: Nil
    }.toSeq

    distributionMeasures
      .withColumn(getDistributionMeasuresCol, map(measureTuples: _*))
      .select(col(featureNameCol), col(getDistributionMeasuresCol))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    validateSchema(schema)

    StructType(
      StructField(featureNameCol, StringType, nullable = false) ::
        StructField(
          getDistributionMeasuresCol, MapType(StringType, DoubleType, valueContainsNull = true), nullable = false) ::
        Nil
    )
  }

  private def validateSchema(schema: StructType): Unit = {
  }
}

case class DistributionMetrics(observedCol: String,
                               numObservations: Double,
                               referenceProbability: Double,
                               referenceNumObservations: Double) {
  val absDiffObsRef: Column = abs(col(observedCol) - lit(referenceProbability))

  def toMap: Map[String, Column] = Map(
    "kl_divergence" -> klDivergence.alias("kl_divergence"),
    "js_dist" -> jsDistance.alias("js_dist"),
    "inf_norm_dist" -> infNormDistance.alias("inf_norm_dist"),
    "total_variation_dist" -> totalVariationDistance.alias("total_variation_dist"),
    //    "wasserstein_dist" -> wassersteinDistance.alias("wasserstein_dist"),
    "chi_sq_stat" -> chiSqTestStatistic.alias("chi_sq_stat"),
    //    "chi_sq_p_value" -> chiSqPValue.alias("chi_sq_p_value")
  )

  def klDivergence: Column = entropy(col(observedCol), lit(referenceProbability))

  def jsDistance: Column = {
    val averageObsRef = (col(observedCol) + lit(referenceProbability)) / 2d
    val entropyObsAvg = entropy(col(observedCol), averageObsRef)
    val entropyRefAvg = entropy(lit(referenceProbability), averageObsRef)
    sqrt((entropyRefAvg + entropyObsAvg) / 2d)
  }

  def infNormDistance: Column = max(absDiffObsRef)

  // Trying this results in taking square root of a negative number. Are we supposed to abs() it?
  // https://www.wikiwand.com/en/Pinsker%27s_inequality
  def totalVariationDistance: Column = sum(absDiffObsRef) * 0.5d

  // Need to implement
  def wassersteinDistance: Column = lit(0d)

  def chiSqTestStatistic: Column = pow(lit(numObservations - referenceNumObservations), 2) / referenceNumObservations

  // Need to implement
  def chiSqPValue: Column = lit(0d)

  private def entropy(distA: Column, distB: Column = null): Column = {
    if (distB != null) {
      sum(distA * log(distA / distB))
    } else {
      sum(distA * log(distA)) * -1d
    }
  }
}
