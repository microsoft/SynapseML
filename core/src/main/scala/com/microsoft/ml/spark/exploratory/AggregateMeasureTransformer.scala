package com.microsoft.ml.spark.exploratory

import com.microsoft.ml.spark.codegen.Wrappable
import com.microsoft.ml.spark.core.schema.DatasetExtensions
import com.microsoft.ml.spark.logging.BasicLogging
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{ComplexParamsWritable, Transformer}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, functions}

class AggregateMeasureTransformer(override val uid: String)
  extends Transformer
    with ComplexParamsWritable
    with DataImbalanceParams
    with Wrappable
    with BasicLogging {

  override protected lazy val pyInternalWrapper = true

  logClass()

  def this() = this(Identifiable.randomUID("AggregateMeasureTransformer"))

  val aggregateMeasuresCol = new Param[String](
    this,
    "aggregateMeasuresCol",
    "Output column name for aggregate measures."
  )

  def getAggregateMeasuresCol: String = $(aggregateMeasuresCol)

  def setAggregateMeasuresCol(value: String): this.type = set(aggregateMeasuresCol, value)

  val alpha = new Param[Double](
    this,
    "alpha",
    "Alpha value for Atkinson Index."
  )

  def getAlpha: Double = $(alpha)

  def setAlpha(value: Double): this.type = set(alpha, value)

  val errorTolerance = new Param[Double](
    this,
    "errorTolerance",
    "Error tolerance value for Atkinson Index."
  )

  def getErrorTolerance: Double = $(errorTolerance)

  def setErrorTolerance(value: Double): this.type = set(errorTolerance, value)

  setDefault(aggregateMeasuresCol -> "AggregateMeasures", alpha -> 0d, errorTolerance -> 1e-12)

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
    val countSensitiveProbCol = DatasetExtensions.findUnusedColumnName("countSensitiveProb", dataset.schema)
    val countSensitiveProbNormCol = DatasetExtensions.findUnusedColumnName("countSensitiveProbNorm", dataset.schema)

    val counts = df
      .groupBy(getSensitiveCols map col: _*)
      .agg(
//        sum(getLabelCol).cast(DoubleType).alias(countSensitivePositiveCol),
        count("*").cast(DoubleType).alias(countSensitiveCol)
      )
//      .withColumn(countPositiveCol, lit(numTrueLabels))
      .withColumn(countAllCol, lit(numRows))
      .withColumn(countSensitiveProbCol, col(countSensitiveCol) / col(countAllCol))

    counts.show()

    val Row(numBenefits: Double, meanBenefits: Double) =
      counts.agg(count("*").cast(DoubleType), mean(countSensitiveProbCol).cast(DoubleType)).head

    val benefits = counts
      .withColumn(countSensitiveProbNormCol, col(countSensitiveProbCol) / lit(meanBenefits))
      .agg(
        exp(sum(functions.log(countSensitiveProbNormCol))).alias("Product"),
        (sum(pow(countSensitiveProbNormCol, getAlpha)) / numBenefits).alias("PowerMean"),
        sum(lit(-1d) * functions.log(countSensitiveProbNormCol)).alias("NegativeSumLog"),
        sum(col(countSensitiveProbNormCol) * functions.log(countSensitiveProbNormCol)).alias("SumLog")
      )

    calculateAggregateMeasures(benefits, numBenefits)
  }

  private def calculateAggregateMeasures(benefitsDf: DataFrame, numBenefits: Double): DataFrame = {
    val metrics = AggregateMetrics(
      "Product", "PowerMean", "NegativeSumLog", "SumLog", numBenefits, getAlpha, getErrorTolerance).toMap

    val aggregateMetricsDf = metrics.foldLeft(benefitsDf) {
      case (dfAcc, (metricName, metricFunc)) => dfAcc.withColumn(metricName, metricFunc)
    }

    if (getVerbose)
      aggregateMetricsDf.cache().show()

    val measureTuples = metrics.flatMap {
      case (metricName, _) =>
        lit(metricName) :: col(metricName) :: Nil
    }.toSeq

    aggregateMetricsDf.withColumn(getAggregateMeasuresCol, map(measureTuples: _*))
      .select(
        col(getAggregateMeasuresCol)
      )
  }

  private def validateSchema(schema: StructType): Unit = {
    val labelCol = schema(getLabelCol)
    if (!labelCol.dataType.isInstanceOf[NumericType]) {
      throw new Exception(s"The label column named $getLabelCol does not contain numeric values.")
    }
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    validateSchema(schema)

    StructType(
        StructField(
          getAggregateMeasuresCol, MapType(StringType, DoubleType, valueContainsNull = true), nullable = false) ::
        Nil
    )
  }
}

case class AggregateMetrics(productCol: String,
                            powerMeanCol: String,
                            negativeSumLogCol: String,
                            sumLogCol: String,
                            numBenefits: Double,
                            alpha: Double,
                            errorTolerance: Double) {

  def toMap: Map[String, Column] = Map(
    "atkinson_index" -> atkinsonIndex,
    "thiel_l_index" -> thielLIndex,
    "thiel_t_index" -> thielTIndex
  )

  def atkinsonIndex: Column = when(
    abs(lit(alpha)) < errorTolerance,
    lit(1d) - pow(productCol, 1d / numBenefits))
    .otherwise(
      lit(1d) - pow(powerMeanCol, 1d / alpha)
    )

  def thielLIndex: Column = col(negativeSumLogCol) / numBenefits

  def thielTIndex: Column = col(sumLogCol) / numBenefits
}
