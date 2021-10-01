package com.microsoft.ml.spark.exploratory

import com.microsoft.ml.spark.codegen.Wrappable
import com.microsoft.ml.spark.core.schema.DatasetExtensions
import com.microsoft.ml.spark.logging.BasicLogging
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{ComplexParamsWritable, Transformer}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}

class AssociationGaps(override val uid: String)
  extends Transformer
    with ComplexParamsWritable
    with DataImbalanceParams
    with Wrappable
    with BasicLogging {

  override protected lazy val pyInternalWrapper = true

  logClass()

  def this() = this(Identifiable.randomUID("AssociationGaps"))

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

  val associationGapsCol = new Param[String](
    this,
    "associationGapsCol",
    "Output column name for association gaps."
  )

  def getAssociationGapsCol: String = $(associationGapsCol)

  def setAssociationGapsCol(value: String): this.type = set(associationGapsCol, value)

  setDefault(
    featureNameCol -> "FeatureName",
    classACol -> "ClassA",
    classBCol -> "ClassB",
    associationGapsCol -> "AssociationGaps"
  )

  override def transform(dataset: Dataset[_]): DataFrame = {
    validateSchema(dataset.schema)

    val df = dataset
      // Convert label into binary
      .withColumn(getLabelCol, when(col(getLabelCol).cast(LongType) > lit(0L), lit(1L)).otherwise(lit(0L)))
      .cache

    val Row(numRows: Double, numTrueLabels: Double) =
      df.agg(count("*").cast(DoubleType), sum(getLabelCol).cast(DoubleType)).head

    val countSensitivePositiveCol = DatasetExtensions.findUnusedColumnName("countSensitivePositive", df.schema)
    val countSensitiveCol = DatasetExtensions.findUnusedColumnName("countSensitive", df.schema)
    val countPositiveCol = DatasetExtensions.findUnusedColumnName("countPositive", df.schema)
    val countAllCol = DatasetExtensions.findUnusedColumnName("countAll", df.schema)
    val sensitiveValueCol = "SensitiveValue"

    val counts = getSensitiveCols.map {
      sensitiveCol =>
        df
          .groupBy(sensitiveCol)
          .agg(
            sum(getLabelCol).cast(DoubleType).alias(countSensitivePositiveCol),
            count("*").cast(DoubleType).alias(countSensitiveCol)
          )
          .withColumn(countPositiveCol, lit(numTrueLabels))
          .withColumn(countAllCol, lit(numRows))
          .withColumn(getFeatureNameCol, lit(sensitiveCol))
          .withColumn(sensitiveValueCol, col(sensitiveCol))
    }.reduce(_ union _)

    val metrics = AssociationMetrics(countSensitivePositiveCol, countSensitiveCol, countPositiveCol, countAllCol).toMap

    val associationMetricsDf = metrics.foldLeft(counts) {
      case (dfAcc, (metricName, metricFunc)) => dfAcc.withColumn(metricName, metricFunc)
    }

    calculateAssociationGaps(associationMetricsDf, metrics, sensitiveValueCol)
  }

  private def calculateAssociationGaps(associationMetricsDf: DataFrame,
                                       metrics: Map[String, Column],
                                       sensitiveValueCol: String): DataFrame = {
    val combinations = associationMetricsDf.alias("A")
      .crossJoin(associationMetricsDf.alias("B"))
      .filter(
        col(s"A.$getFeatureNameCol") === col(s"B.$getFeatureNameCol")
          && col(s"A.$sensitiveValueCol") > col(s"B.$sensitiveValueCol")
      )

    // We handle the case that if A == B, then the gap is 0.0
    // If not handled, A == B == 0.0 for some measures equals Double.NegativeInfinity - Double.NegativeInfinity = NaN
    val gapFunc = (colA: Column, colB: Column) => when(colA === colB, lit(0d)).otherwise(colA - colB)

    val gapTuples = metrics.flatMap {
      case (metricName, _) =>
        lit(metricName) :: gapFunc(col(s"A.$metricName"), col(s"B.$metricName")) :: Nil
    }.toSeq

    val measureTuples = if (getVerbose) Seq(lit("prA"), col("A.dp"), lit("prB"), col("B.dp")) else Seq.empty

    combinations.withColumn(getAssociationGapsCol, map(gapTuples ++ measureTuples: _*))
      .select(
        col(s"A.$getFeatureNameCol").alias(getFeatureNameCol),
        col(s"A.$sensitiveValueCol").alias(getClassACol),
        col(s"B.$sensitiveValueCol").alias(getClassBCol),
        col(getAssociationGapsCol)
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
          getAssociationGapsCol, MapType(StringType, DoubleType, valueContainsNull = true), nullable = false) ::
        Nil
    )
  }
}

case class AssociationMetrics(sensitivePositiveCountCol: String,
                              sensitiveCountCol: String,
                              positiveCountCol: String,
                              totalCountCol: String) {
  val pPositive: Column = col(positiveCountCol) / col(totalCountCol)
  val pSensitive: Column = col(sensitiveCountCol) / col(totalCountCol)
  val pSensitivePositive: Column = col(sensitivePositiveCountCol) / col(sensitiveCountCol)
  val pPositiveGivenSensitive: Column = pSensitivePositive / pSensitive
  val pSensitiveGivenPositive: Column = pSensitivePositive / pPositive

  def toMap: Map[String, Column] = Map(
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
  def dp: Column = pSensitivePositive / pSensitive

  // Sorensen-Dice Coefficient
  def sdc: Column = pSensitivePositive / (pSensitive + pPositive)

  // Jaccard Index
  def ji: Column = pSensitivePositive / (pSensitive + pPositive - pSensitivePositive)

  // Log-Likelihood Ratio
  def llr: Column = log(pSensitivePositive / pPositive)

  // Pointwise Mutual Information
  // If dp == 0.0, then we don't calculate its log, but rather assume that ln(0.0) = -inf
  def pmi: Column = when(dp === lit(0d), lit(Double.NegativeInfinity)).otherwise(log(dp))

  // Normalized Pointwise Mutual Information, p(y) normalization
  // If pmi == -inf and positiveCol == 0.0, then we don't calculate pmi / ln(0.0) because -inf / -inf = NaN
  def nPmiY: Column = when(pPositive === lit(0d), lit(0d)).otherwise(pmi / log(pPositive))

  // Normalized Pointwise Mutual Information, p(x,y) normalization
  def nPmiXY: Column = when(pSensitivePositive === lit(0d), lit(0d)).otherwise(pmi / log(pSensitivePositive))

  // Squared Pointwise Mutual Information
  def sPmi: Column = when(pSensitive * pPositive === lit(0d), lit(0d))
    .otherwise(log(pow(pSensitivePositive, 2) / (pSensitive * pPositive)))

  // Kendall Rank Correlation
  def krc: Column = {
    val a = pow(totalCountCol, 2) * (lit(1) - lit(2) * pSensitive - lit(2) * pPositive +
      lit(2) * pSensitivePositive + lit(2) * pSensitive * pPositive)
    val b = col(totalCountCol) * (lit(2) * pSensitive + lit(2) * pPositive - lit(4) * pSensitivePositive - lit(1))
    val c = pow(totalCountCol, 2) * sqrt((pSensitive - pow(pSensitive, 2)) * (pPositive - pow(pPositive, 2)))
    (a + b) / c
  }

  // t-test
  def tTest: Column = (pSensitivePositive - (pSensitive * pPositive)) / sqrt(pSensitive * pPositive)
}
