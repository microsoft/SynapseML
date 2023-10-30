// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.exploratory

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.core.schema.DatasetExtensions
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.HasLabelCol
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable, Transformer}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}

/** This transformer computes a set of balance measures from the given dataframe and sensitive features.
  *
  * The output is a dataframe that contains four columns:
  *   - The sensitive feature name.
  *   - A feature value within the sensitive feature.
  *   - Another feature value within the sensitive feature.
  *   - A struct containing measure names and their values showing parities between the two feature values.
  *     The following measures are computed:
  *     - Demographic Parity - https://en.wikipedia.org/wiki/Fairness_(machine_learning)
  *     - Pointwise Mutual Information - https://en.wikipedia.org/wiki/Pointwise_mutual_information
  *     - Sorensen-Dice Coefficient - https://en.wikipedia.org/wiki/S%C3%B8rensen%E2%80%93Dice_coefficient
  *     - Jaccard Index - https://en.wikipedia.org/wiki/Jaccard_index
  *     - Kendall Rank Correlation - https://en.wikipedia.org/wiki/Kendall_rank_correlation_coefficient
  *     - Log-Likelihood Ratio - https://en.wikipedia.org/wiki/Likelihood_function#Likelihood_ratio
  *     - t-test - https://en.wikipedia.org/wiki/Student's_t-test
  *
  * The output dataframe contains a row per combination of feature values for each sensitive feature.
  *
  * @param uid The unique ID.
  */
@org.apache.spark.annotation.Experimental
class FeatureBalanceMeasure(override val uid: String)
  extends Transformer
    with DataBalanceParams
    with HasLabelCol
    with ComplexParamsWritable
    with Wrappable
    with SynapseMLLogging {

  logClass(FeatureNames.Core)

  def this() = this(Identifiable.randomUID("FeatureBalanceMeasure"))

  val featureNameCol = new Param[String](
    this,
    "featureNameCol",
    "Output column name for feature names."
  )

  def getFeatureNameCol: String = $(featureNameCol)

  def setFeatureNameCol(value: String): this.type = set(featureNameCol, value)

  val classACol = new Param[String](
    this,
    "classACol",
    "Output column name for the first feature value to compare."
  )

  def getClassACol: String = $(classACol)

  def setClassACol(value: String): this.type = set(classACol, value)

  val classBCol = new Param[String](
    this,
    "classBCol",
    "Output column name for the second feature value to compare."
  )

  def getClassBCol: String = $(classBCol)

  def setClassBCol(value: String): this.type = set(classBCol, value)

  def setLabelCol(value: String): this.type = set(labelCol, value)

  setDefault(
    featureNameCol -> "FeatureName",
    classACol -> "ClassA",
    classBCol -> "ClassB",
    outputCol -> "FeatureBalanceMeasure"
  )

  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame]({
      validateSchema(dataset.schema)

      val df = dataset
        // Convert label into binary
        // TODO (for v2): support regression scenarios
        .withColumn(getLabelCol, when(col(getLabelCol).cast(LongType) > lit(0L), lit(1L)).otherwise(lit(0L)))
        .cache

      val Row(numRows: Double, numTrueLabels: Double) =
        df.agg(count("*").cast(DoubleType), sum(getLabelCol).cast(DoubleType)).head

      val positiveFeatureCountCol = DatasetExtensions.findUnusedColumnName("positiveFeatureCount", df.schema)
      val featureCountCol = DatasetExtensions.findUnusedColumnName("featureCount", df.schema)
      val positiveCountCol = DatasetExtensions.findUnusedColumnName("positiveCount", df.schema)
      val rowCountCol = DatasetExtensions.findUnusedColumnName("rowCount", df.schema)
      val featureValueCol = "FeatureValue"

      val featureCounts = getSensitiveCols.map {
        sensitiveCol =>
          df
            .groupBy(sensitiveCol)
            .agg(
              sum(getLabelCol).cast(DoubleType).alias(positiveFeatureCountCol),
              count("*").cast(DoubleType).alias(featureCountCol)
            )
            .withColumn(positiveCountCol, lit(numTrueLabels))
            .withColumn(rowCountCol, lit(numRows))
            .withColumn(getFeatureNameCol, lit(sensitiveCol))
            .withColumn(featureValueCol, col(sensitiveCol))
      }.reduce(_ union _)

      val metrics =
        AssociationMetrics(positiveFeatureCountCol, featureCountCol, positiveCountCol, rowCountCol).toColumnMap

      val associationMetricsDf = metrics.foldLeft(featureCounts) {
        case (dfAcc, (metricName, metricFunc)) => dfAcc.withColumn(metricName, metricFunc)
      }

      df.unpersist
      calculateParity(associationMetricsDf, featureValueCol)
    }, dataset.columns.length)
  }

  private def calculateParity(associationMetricsDf: DataFrame, featureValueCol: String): DataFrame = {
    val combinations = associationMetricsDf.alias("A")
      .crossJoin(associationMetricsDf.alias("B"))
      .filter(
        col(s"A.$getFeatureNameCol") === col(s"B.$getFeatureNameCol")
          && col(s"A.$featureValueCol") > col(s"B.$featureValueCol")
      )

    // We handle the case that if A == B, then the gap is 0.0
    // If not handled, A == B == 0.0 for some measures equals Double.NegativeInfinity - Double.NegativeInfinity = NaN
    val gapFunc = (colA: Column, colB: Column) => when(colA === colB, lit(0d)).otherwise(colA - colB)

    val gapTuples = AssociationMetrics.METRICS.map(m => gapFunc(col(s"A.$m"), col(s"B.$m")).alias(m))
    val measureTuples = if (getVerbose) Seq(
      col(s"A.${AssociationMetrics.DP}").alias("prA"),
      col(s"B.${AssociationMetrics.DP}").alias("prB")
    ) else Seq.empty
    combinations.withColumn(getOutputCol, struct(gapTuples ++ measureTuples: _*))
      .select(
        col(s"A.$getFeatureNameCol").alias(getFeatureNameCol),
        col(s"A.$featureValueCol").alias(getClassACol),
        col(s"B.$featureValueCol").alias(getClassBCol),
        col(getOutputCol)
      )
  }

  override def validateSchema(schema: StructType): Unit = {
    super.validateSchema(schema)
    schema(getLabelCol).dataType match {
      case ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType =>
      case _ => throw new Exception(s"The label column named $getLabelCol does not contain numeric values.")
    }
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    validateSchema(schema)

    StructType(
      StructField(getFeatureNameCol, StringType, nullable = false) ::
        StructField(getClassACol, StringType, nullable = true) ::
        StructField(getClassBCol, StringType, nullable = true) ::
        StructField(getOutputCol,
          StructType(AssociationMetrics.METRICS.map(StructField(_, DoubleType, nullable = true))), nullable = false) ::
        Nil
    )
  }
}

object FeatureBalanceMeasure extends ComplexParamsReadable[FeatureBalanceMeasure]

//noinspection SpellCheckingInspection
private[exploratory] object AssociationMetrics {
  val DP = "dp"
  val SDC = "sdc"
  val JI = "ji"
  val LLR = "llr"
  val PMI = "pmi"
  val NPMIY = "n_pmi_y"
  val NPMIXY = "n_pmi_xy"
  val SPMI = "s_pmi"
  val KRC = "krc"
  val TTEST = "t_test"

  val METRICS = Seq(DP, SDC, JI, LLR, PMI, NPMIY, NPMIXY, SPMI, KRC, TTEST)
}

//noinspection SpellCheckingInspection
private[exploratory] case class AssociationMetrics(positiveFeatureCountCol: String,
                                                   featureCountCol: String,
                                                   positiveCountCol: String,
                                                   totalCountCol: String) {

  import AssociationMetrics._

  val pPositive: Column = col(positiveCountCol) / col(totalCountCol)
  val pFeature: Column = col(featureCountCol) / col(totalCountCol)
  val pPositiveFeature: Column = col(positiveFeatureCountCol) / col(totalCountCol)

  def toColumnMap: Map[String, Column] = Map(
    DP -> dp,
    SDC -> sdc,
    JI -> ji,
    LLR -> llr,
    PMI -> pmi,
    NPMIY -> nPmiY,
    NPMIXY -> nPmiXY,
    SPMI -> sPmi,
    KRC -> krc,
    TTEST -> tTest
  )

  // Demographic Parity
  def dp: Column = pPositiveFeature / pFeature

  // Sorensen-Dice Coefficient
  def sdc: Column = pPositiveFeature / (pFeature + pPositive)

  // Jaccard Index
  def ji: Column = pPositiveFeature / (pFeature + pPositive - pPositiveFeature)

  // Log-Likelihood Ratio
  def llr: Column = log(pPositiveFeature / pPositive)

  // Pointwise Mutual Information
  // If dp == 0.0, then we don't calculate its log, but rather assume that ln(0.0) = -inf
  def pmi: Column = when(dp === lit(0d), lit(Double.NegativeInfinity)).otherwise(log(dp))

  // Normalized Pointwise Mutual Information, p(y) normalization
  // If pmi == -inf and positiveCol == 0.0, then we don't calculate pmi / ln(0.0) because -inf / -inf = NaN
  def nPmiY: Column = when(pPositive === lit(0d), lit(0d)).otherwise(pmi / log(pPositive))

  // Normalized Pointwise Mutual Information, p(x,y) normalization
  def nPmiXY: Column = when(pPositiveFeature === lit(0d), lit(0d)).otherwise(pmi / log(pPositiveFeature))

  // Squared Pointwise Mutual Information
  def sPmi: Column = when(pFeature * pPositive === lit(0d), lit(0d))
    .otherwise(log(pow(pPositiveFeature, 2) / (pFeature * pPositive)))

  // Kendall Rank Correlation
  def krc: Column = {
    val a = pow(totalCountCol, 2) * (lit(1) - lit(2) * pFeature - lit(2) * pPositive +
      lit(2) * pPositiveFeature + lit(2) * pFeature * pPositive)
    //noinspection ScalaStyle
    val b = col(totalCountCol) * (lit(2) * pFeature + lit(2) * pPositive - lit(4) * pPositiveFeature - lit(1))
    val c = pow(totalCountCol, 2) * sqrt((pFeature - pow(pFeature, 2)) * (pPositive - pow(pPositive, 2)))
    (a + b) / c
  }

  // t-test
  def tTest: Column = (pPositiveFeature - (pFeature * pPositive)) / sqrt(pFeature * pPositive)
}
