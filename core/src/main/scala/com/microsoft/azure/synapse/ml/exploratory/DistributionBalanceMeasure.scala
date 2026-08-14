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
  * A uniform reference distribution is used by default, and custom reference distributions are supported.
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

  private def decodeReferenceProbability(category: String, probability: Any, distributionIndex: Int): Double = {
    Option(probability) match {
      case Some(value: java.lang.Number) => value.doubleValue()
      case None => throw new IllegalArgumentException(
        s"Reference probability for category '$category' in distribution $distributionIndex cannot be null.")
      case Some(value) => throw new IllegalArgumentException(
        s"Reference probability for category '$category' in distribution $distributionIndex must be numeric, " +
          s"but found ${value.getClass.getName}.")
    }
  }

  private def normalizeReferenceDistributionValue(value: Array[Map[String, Any]]): Boolean = {
    val distributions = Option(value).getOrElse {
      throw new IllegalArgumentException("Reference distributions cannot be null.")
    }
    distributions.indices.foreach { distributionIndex =>
      val distribution = Option(distributions(distributionIndex)).getOrElse {
        throw new IllegalArgumentException(
          s"Reference distribution $distributionIndex cannot be null; use an empty map for a uniform distribution.")
      }
      distributions(distributionIndex) = distribution.map { case (category, probability) =>
        category -> (decodeReferenceProbability(category, probability, distributionIndex): Any)
      }
    }
    true
  }

  val referenceDistribution = new ArrayMapParam(
    this,
    "referenceDistribution",
    "An ordered list of reference distributions that correspond to each of the sensitive columns. " +
      "An empty map selects the uniform distribution. Each non-empty distribution must sum to 1. " +
      "Positive-probability reference-only categories are included, and omitted observed categories have " +
      "reference probability 0. Keys must be non-null strings that resolve to distinct values matching the sensitive " +
      "column's string or integral type.",
    normalizeReferenceDistributionValue _
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

  private val referenceDistributionTolerance = 1e-8

  private def parseReferenceKey(category: String, dataType: DataType, sensitiveCol: String): Any = {
    if (Option(category).isEmpty) {
      throw new IllegalArgumentException(
        s"Reference distribution keys for sensitive column '$sensitiveCol' cannot be null.")
    }

    try {
      dataType match {
        case ByteType => java.lang.Byte.valueOf(category)
        case ShortType => java.lang.Short.valueOf(category)
        case IntegerType => java.lang.Integer.valueOf(category)
        case LongType => java.lang.Long.valueOf(category)
        case StringType => category
        case _ => throw new IllegalArgumentException(
          s"Unsupported sensitive column type ${dataType.simpleString} for '$sensitiveCol'.")
      }
    } catch {
      case _: NumberFormatException => throw new IllegalArgumentException(
        s"Reference distribution key '$category' cannot be converted to ${dataType.simpleString} " +
          s"for sensitive column '$sensitiveCol'.")
    }
  }

  private def validateCustomReferenceDistribution(sensitiveCol: String,
                                                  dataType: DataType,
                                                  distribution: Map[String, Double]): Unit = {
    distribution.foreach { case (category, probability) =>
      if (!java.lang.Double.isFinite(probability) || probability < 0d || probability > 1d) {
        throw new IllegalArgumentException(
          s"Reference probability for category '$category' in sensitive column '$sensitiveCol' " +
            s"must be finite and between 0 and 1, but found $probability.")
      }
    }

    val probabilitySum = distribution.values.sum
    if (math.abs(probabilitySum - 1d) > referenceDistributionTolerance) {
      throw new IllegalArgumentException(
        s"Reference distribution for sensitive column '$sensitiveCol' must sum to 1, but found $probabilitySum.")
    }

    val typedCategories = distribution.keys.toSeq.map(parseReferenceKey(_, dataType, sensitiveCol))
    if (typedCategories.distinct.length != typedCategories.length) {
      throw new IllegalArgumentException(
        s"Reference distribution keys for sensitive column '$sensitiveCol' must identify distinct " +
          s"${dataType.simpleString} categories.")
    }
  }

  private def createReferenceSupport(observed: DataFrame,
                                     sensitiveCol: String,
                                     obsFeatureProbCol: String,
                                     obsFeatureCountCol: String,
                                     refFeatureProbCol: String,
                                     refFeatureCountCol: String,
                                     numRows: Double,
                                     distribution: Map[String, Double]): DataFrame = {
    val sensitiveType = observed.schema(sensitiveCol).dataType
    val referenceRows = distribution.toSeq.collect {
      case (category, probability) if probability > 0d =>
        Row(parseReferenceKey(category, sensitiveType, sensitiveCol), probability)
    }
    val referenceSchema = StructType(Seq(
      StructField(sensitiveCol, sensitiveType, nullable = false),
      StructField(refFeatureProbCol, DoubleType, nullable = false)
    ))
    val reference = observed.sparkSession.createDataFrame(referenceRows.asJava, referenceSchema)
      .withColumn(obsFeatureProbCol, lit(0d))
      .withColumn(obsFeatureCountCol, lit(0d))

    observed
      .withColumn(refFeatureProbCol, lit(0d))
      .unionByName(reference)
      .groupBy(col(sensitiveCol))
      .agg(
        sum(obsFeatureProbCol).alias(obsFeatureProbCol),
        sum(obsFeatureCountCol).alias(obsFeatureCountCol),
        sum(refFeatureProbCol).alias(refFeatureProbCol)
      )
      .withColumn(refFeatureCountCol, col(refFeatureProbCol) * lit(numRows))
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame]({
      validateSchema(dataset.schema)

      val df = dataset.toDF()
      val numRows = df.count.toDouble
      if (numRows == 0d) {
        throw new IllegalArgumentException("DistributionBalanceMeasure requires at least one input row.")
      }

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
        featureStats.show(numRows = 20, truncate = false)  //scalastyle:ignore magic.number

      calculateDistributionMeasures(featureStats, featureProbCol, featureCountCol, numRows)
    }, dataset.columns.length)
  }

  private def calculateDistributionMeasures(featureStats: DataFrame,
                                            obsFeatureProbCol: String,
                                            obsFeatureCountCol: String,
                                            numRows: Double): DataFrame = {
    val configuredReferences =
      if (isDefined(referenceDistribution)) Some(getReferenceDistribution) else None
    val distributionMeasures = getSensitiveCols.zipWithIndex.map {
      case (sensitiveCol, i) =>
        val observed = featureStats
          .groupBy(sensitiveCol)
          .agg(sum(obsFeatureProbCol).alias(obsFeatureProbCol), sum(obsFeatureCountCol).alias(obsFeatureCountCol))

        val refFeatureProbCol = DatasetExtensions.findUnusedColumnName("refFeatureProb", featureStats.schema)
        val refFeatureCountCol = DatasetExtensions.findUnusedColumnName("refFeatureCount", featureStats.schema)
        val reference = configuredReferences.map(_(i)).getOrElse(Map.empty[String, Double])

        val observedWithRef = if (reference.isEmpty) {
          val observedFeatureCount = observed.count.toDouble
          val uniformProbability = 1d / observedFeatureCount
          observed
            .withColumn(refFeatureProbCol, lit(uniformProbability))
            .withColumn(refFeatureCountCol, lit(uniformProbability * numRows))
        } else {
          createReferenceSupport(
            observed,
            sensitiveCol,
            obsFeatureProbCol,
            obsFeatureCountCol,
            refFeatureProbCol,
            refFeatureCountCol,
            numRows,
            reference
          )
        }

        val metrics =
          DistributionMetrics(obsFeatureProbCol, obsFeatureCountCol, refFeatureProbCol, refFeatureCountCol)
        val metricsCols = metrics.toColumnMap.values.toSeq

        observedWithRef.agg(metricsCols.head, metricsCols.tail: _*).withColumn(getFeatureNameCol, lit(sensitiveCol))
    }.reduce(_ union _)

    if (getVerbose)
      distributionMeasures.show(truncate = false)

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
    if (!isDefined(sensitiveCols) || getSensitiveCols.isEmpty) {
      throw new IllegalArgumentException("DistributionBalanceMeasure requires at least one sensitive column.")
    }

    super.validateSchema(schema)

    if (isDefined(referenceDistribution)) {
      val distributions = getReferenceDistribution
      if (distributions.length != getSensitiveCols.length) {
        throw new Exception("The reference distribution must have the same length and order as the sensitive columns: "
          + getSensitiveCols.mkString(", "))
      }

      getSensitiveCols.zip(distributions).foreach { case (sensitiveCol, distribution) =>
        if (distribution.nonEmpty) {
          validateCustomReferenceDistribution(sensitiveCol, schema(sensitiveCol).dataType, distribution)
        }
      }
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

  def apply(numFeatures: Int,
            obsFeatureProbCol: String,
            obsFeatureCountCol: String,
            refFeatureProbCol: String,
            refFeatureCountCol: String): DistributionMetrics = {
    require(numFeatures > 0, "Distribution metrics require at least one feature.")
    DistributionMetrics(obsFeatureProbCol, obsFeatureCountCol, refFeatureProbCol, refFeatureCountCol)
  }
}

//noinspection SpellCheckingInspection
private[exploratory] case class DistributionMetrics(obsFeatureProbCol: String,
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
    // Keep KL divergence in natural-log units while normalizing only JS divergence to base 2.
    val jsDivergenceBase2 = (entropyRefAvg + entropyObsAvg) / (2d * math.log(2d))
    // Floating-point aggregation can make a theoretically non-negative divergence slightly negative.
    val nonNegativeJsDivergence = when(jsDivergenceBase2 < 0d, lit(0d)).otherwise(jsDivergenceBase2)
    sqrt(nonNegativeJsDivergence)
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
    val scoreCol = chiSquaredTestStatistic
    val chiSqPValueUdf = udf(
      (score: Double, numFeatures: Long) => {
        val degOfFreedom = numFeatures - 1
        if (degOfFreedom == 0) {
          1d
        } else {
          implicit val rand: RandBasis = RandBasis.mt0
          score match {
            // limit of CDF as x approaches +inf is 1 (https://en.wikipedia.org/wiki/Cumulative_distribution_function)
            case Double.PositiveInfinity => 1d
            case _ => 1 - ChiSquared(degOfFreedom.toDouble).cdf(score)
          }
        }
      }
    )
    chiSqPValueUdf(scoreCol, count(lit(1)))
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
