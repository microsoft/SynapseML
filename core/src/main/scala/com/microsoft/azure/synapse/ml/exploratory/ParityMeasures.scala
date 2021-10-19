package com.microsoft.azure.synapse.ml.exploratory

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.core.schema.DatasetExtensions
import com.microsoft.azure.synapse.ml.logging.BasicLogging
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable, Transformer}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}

// This feature is experimental. It is subject to change or removal in future releases.
class ParityMeasures(override val uid: String)
  extends Transformer
    with ComplexParamsWritable
    with DataImbalanceParams
    with Wrappable
    with BasicLogging {

  logClass()

  def this() = this(Identifiable.randomUID("ParityMeasures"))

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

  val parityMeasuresCol = new Param[String](
    this,
    "parityMeasuresCol",
    "Output column name for parity measures."
  )

  def getParityMeasuresCol: String = $(parityMeasuresCol)

  def setParityMeasuresCol(value: String): this.type = set(parityMeasuresCol, value)

  setDefault(
    featureNameCol -> "FeatureName",
    classACol -> "ClassA",
    classBCol -> "ClassB",
    parityMeasuresCol -> "ParityMeasures"
  )

  override def transform(dataset: Dataset[_]): DataFrame = {
    validateSchema(dataset.schema)

    val df = dataset
      // Convert label into binary
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

    calculateParityMeasures(associationMetricsDf, metrics, featureValueCol)
  }

  private def calculateParityMeasures(associationMetricsDf: DataFrame,
                                      metrics: Map[String, Column],
                                      featureValueCol: String): DataFrame = {
    val combinations = associationMetricsDf.alias("A")
      .crossJoin(associationMetricsDf.alias("B"))
      .filter(
        col(s"A.$getFeatureNameCol") === col(s"B.$getFeatureNameCol")
          && col(s"A.$featureValueCol") > col(s"B.$featureValueCol")
      )

    // We handle the case that if A == B, then the gap is 0.0
    // If not handled, A == B == 0.0 for some measures equals Double.NegativeInfinity - Double.NegativeInfinity = NaN
    val gapFunc = (colA: Column, colB: Column) => when(colA === colB, lit(0d)).otherwise(colA - colB)

    val gapTuples = metrics.flatMap {
      case (metricName, _) =>
        lit(metricName) :: gapFunc(col(s"A.$metricName"), col(s"B.$metricName")) :: Nil
    }.toSeq

    val measureTuples = if (getVerbose) Seq(lit("prA"), col("A.dp"), lit("prB"), col("B.dp")) else Seq.empty

    combinations.withColumn(getParityMeasuresCol, map(gapTuples ++ measureTuples: _*))
      .select(
        col(s"A.$getFeatureNameCol").alias(getFeatureNameCol),
        col(s"A.$featureValueCol").alias(getClassACol),
        col(s"B.$featureValueCol").alias(getClassBCol),
        col(getParityMeasuresCol)
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
        StructField(
          getParityMeasuresCol, MapType(StringType, DoubleType, valueContainsNull = true), nullable = false) ::
        Nil
    )
  }
}

object ParityMeasures extends ComplexParamsReadable[ParityMeasures]

private[exploratory] case class AssociationMetrics(positiveFeatureCountCol: String,
                                                   featureCountCol: String,
                                                   positiveCountCol: String,
                                                   totalCountCol: String) {
  val pPositive: Column = col(positiveCountCol) / col(totalCountCol)
  val pFeature: Column = col(featureCountCol) / col(totalCountCol)
  val pPositiveFeature: Column = col(positiveFeatureCountCol) / col(featureCountCol)
  val pPositiveGivenFeature: Column = pPositiveFeature / pFeature
  val pFeatureGivenPositive: Column = pPositiveFeature / pPositive

  def toColumnMap: Map[String, Column] = Map(
    "dp" -> dp,
    "sdc" -> sdc,
    "ji" -> ji,
    "llr" -> llr,
    "pmi" -> pmi,
    "n_pmi_y" -> nPmiY,
    "n_pmi_xy" -> nPmiXY,
    "s_pmi" -> sPmi,
    "krc" -> krc,
    "t_test" -> tTest
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
    val b = col(totalCountCol) * (lit(2) * pFeature + lit(2) * pPositive - lit(4) * pPositiveFeature - lit(1))
    val c = pow(totalCountCol, 2) * sqrt((pFeature - pow(pFeature, 2)) * (pPositive - pow(pPositive, 2)))
    (a + b) / c
  }

  // t-test
  def tTest: Column = (pPositiveFeature - (pFeature * pPositive)) / sqrt(pFeature * pPositive)
}
