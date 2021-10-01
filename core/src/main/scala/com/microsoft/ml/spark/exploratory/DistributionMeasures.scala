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

import scala.language.postfixOps

class DistributionMeasures(override val uid: String)
  extends Transformer
    with ComplexParamsWritable
    with DataImbalanceParams
    with Wrappable
    with BasicLogging {

  override protected lazy val pyInternalWrapper = true

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

  val distribution = new Param[String](
    this,
    "distribution",
    "Distribution to use for calculating the reference distribution. Default: uniform"
  )

  def getDistribution: String = $(distribution)

  def setDistribution(value: String): this.type = set(distribution, value)

  setDefault(
    featureNameCol -> "FeatureName",
    distributionMeasuresCol -> "DistributionMeasures",
    distribution -> "uniform"
  )

  override def transform(dataset: Dataset[_]): DataFrame = {
    validateSchema(dataset.schema)

    val df = dataset.cache
    val numRows = df.count.toDouble

    val countSensitiveCol = DatasetExtensions.findUnusedColumnName("countSensitive", df.schema)
    val countAllCol = DatasetExtensions.findUnusedColumnName("countAll", df.schema)
    val probSensitiveCol = DatasetExtensions.findUnusedColumnName("probSensitive", df.schema)

    val benefits = df
      .groupBy(getSensitiveCols map col: _*)
      .agg(count("*").cast(DoubleType).alias(countSensitiveCol))
      .withColumn(countAllCol, lit(numRows))
      // P(sensitive)
      .withColumn(probSensitiveCol, col(countSensitiveCol) / col(countAllCol))

    if (getVerbose)
      benefits.cache.show(numRows = 20, truncate = false)

    calculateDistributionMeasures(benefits, probSensitiveCol, numRows)
  }

  private def calculateDistributionMeasures(benefitsDf: DataFrame, benefitCol: String, numRows: Double): DataFrame = {
    val distributionMeasures = getSensitiveCols.map {
      sensitiveCol =>
        val observations = benefitsDf.groupBy(sensitiveCol).agg(sum(benefitCol).alias(benefitCol))
        val numObservations = observations.count.toDouble

        val (expectedProbability, expectedNumObservations) = getDistribution toLowerCase match {
          case "uniform" | _ => (1d / numObservations, numRows / numObservations)
        }

        val metricsMap =
          DistributionMetrics(benefitCol, numObservations, expectedProbability, expectedNumObservations).toMap
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

  private def validateSchema(schema: StructType): Unit = {
    getSensitiveCols.foreach {
      c =>
        schema(c).dataType match {
          case ByteType | ShortType | IntegerType | LongType | StringType =>
          case _ => throw new Exception(s"The sensitive column named $c does not contain integral or string values.")
        }
    }
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

  def klDivergence: Column = entropy(col(observedCol), Some(lit(referenceProbability)))

  def jsDistance: Column = {
    val averageObsRef = (col(observedCol) + lit(referenceProbability)) / 2d
    val entropyObsAvg = entropy(col(observedCol), Some(averageObsRef))
    val entropyRefAvg = entropy(lit(referenceProbability), Some(averageObsRef))
    sqrt((entropyRefAvg + entropyObsAvg) / 2d)
  }

  def infNormDistance: Column = max(absDiffObsRef)

  def totalVariationDistance: Column = sum(absDiffObsRef) * 0.5d

  // Need to implement
  def wassersteinDistance: Column = lit(0d)

  def chiSqTestStatistic: Column = pow(lit(numObservations - referenceNumObservations), 2) / referenceNumObservations

  // Need to implement
  def chiSqPValue: Column = lit(0d)

  private def entropy(distA: Column, distB: Option[Column] = None): Column = {
    if (distB.isDefined) {
      sum(distA * log(distA / distB.get))
    } else {
      sum(distA * log(distA)) * -1d
    }
  }
}
