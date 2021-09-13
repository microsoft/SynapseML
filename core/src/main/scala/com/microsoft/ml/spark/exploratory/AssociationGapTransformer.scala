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

class AssociationGapTransformer(override val uid: String)
  extends Transformer
    with ComplexParamsWritable
    with DataImbalanceParams
    with Wrappable
    with BasicLogging {

  override protected lazy val pyInternalWrapper = true

  logClass()

  def this() = this(Identifiable.randomUID("AssociationGapTransformer"))

  val featureNameCol = "FeatureName"
  val classACol = "ClassA"
  val classBCol = "ClassB"

  val associationGapsCol = new Param[String](
    this,
    "associationGapsCol",
    "Output column name for association gaps."
  )

  def getAssociationGapsCol: String = $(associationGapsCol)

  def setAssociationGapsCol(value: String): this.type = set(associationGapsCol, value)

  setDefault(associationGapsCol -> "AssociationGaps")

  override def transform(dataset: Dataset[_]): DataFrame = {
    validateSchema(dataset.schema)

    val df = dataset
      // Convert label into binary
      .withColumn(getLabelCol, when(col(getLabelCol).cast(LongType) > lit(0L), lit(1L)).otherwise(lit(0L)))
      .cache

    val Row(numRows: Double, numTrueLabels: Double) =
      df.agg(count("*").cast(DoubleType), sum(getLabelCol).cast(DoubleType)).head

    val countSensitivePositiveCol = DatasetExtensions.findUnusedColumnName("countSensitivePositive", dataset.schema)
    val countSensitiveCol = DatasetExtensions.findUnusedColumnName("countSensitive", dataset.schema)
    val countPositiveCol = DatasetExtensions.findUnusedColumnName("countPositive", dataset.schema)
    val countAllCol = DatasetExtensions.findUnusedColumnName("countAll", dataset.schema)
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
          .withColumn(featureNameCol, lit(sensitiveCol))
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
        col(s"A.$featureNameCol") === col(s"B.$featureNameCol")
          && col(s"A.$sensitiveValueCol") > col(s"B.$sensitiveValueCol")
      )

    // We handle the case that if A == B, then the gap is 0.0
    // If not handled, then A == B == 0.0 results in Double.NegativeInfinity - Double.NegativeInfinity = NaN
    val gapFunc = (colA: Column, colB: Column) => when(colA === colB, lit(0d)).otherwise(colA - colB)

    val gapTuples = metrics.flatMap {
      case (metricName, _) =>
        lit(metricName) :: gapFunc(col(s"A.$metricName"), col(s"B.$metricName")) :: Nil
    }.toSeq

    val measureTuples = if (getVerbose) Seq(lit("prA"), col("A.dp"), lit("prB"), col("B.dp")) else Seq.empty

    combinations.withColumn(getAssociationGapsCol, map(gapTuples ++ measureTuples: _*))
      .select(
        col(s"A.$featureNameCol").alias(featureNameCol),
        col(s"A.$sensitiveValueCol").alias(classACol),
        col(s"B.$sensitiveValueCol").alias(classBCol),
        col(getAssociationGapsCol)
      )
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    validateSchema(schema)

    StructType(
      StructField(featureNameCol, StringType, nullable = false) ::
        StructField(classACol, StringType, nullable = true) ::
        StructField(classBCol, StringType, nullable = true) ::
        StructField(
          getAssociationGapsCol, MapType(StringType, DoubleType, valueContainsNull = true), nullable = false) ::
        Nil
    )
  }

  private def validateSchema(schema: StructType): Unit = {
    val labelCol = schema(getLabelCol)
    if (!labelCol.dataType.isInstanceOf[NumericType]) {
      throw new Exception(s"The label column named $getLabelCol does not contain numeric values.")
    }
  }
}

case class AssociationMetrics(sensitivePositiveCountCol: String,
                              sensitiveCountCol: String,
                              positiveCountCol: String,
                              totalCountCol: String) {

  def toMap: Map[String, Column] = Map("dp" -> dp, "pmi" -> pmi)

  // We handle the case ln(0.0) = -inf
  def pmi: Column = when(dp === lit(0d), lit(Double.NegativeInfinity)).otherwise(log(dp))

  def dp: Column = col(sensitivePositiveCountCol) / col(sensitiveCountCol)
}
