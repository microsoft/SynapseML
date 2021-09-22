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

  val epsilon = new Param[Double](
    this,
    "epsilon",
    "Epsilon value for Atkinson Index. Inverse of alpha (1 - alpha)."
  )

  def getEpsilon: Double = $(epsilon)

  def setEpsilon(value: Double): this.type = set(epsilon, value)

  val errorTolerance = new Param[Double](
    this,
    "errorTolerance",
    "Error tolerance value for Atkinson Index."
  )

  def getErrorTolerance: Double = $(errorTolerance)

  def setErrorTolerance(value: Double): this.type = set(errorTolerance, value)

  setDefault(aggregateMeasuresCol -> "AggregateMeasures", epsilon -> 1d, errorTolerance -> 1e-12)

  override def transform(dataset: Dataset[_]): DataFrame = {
    validateSchema(dataset.schema)

    val df = dataset.cache
    val numRows = df.count.toDouble

    val countSensitiveCol = DatasetExtensions.findUnusedColumnName("countSensitive", df.schema)
    val countAllCol = DatasetExtensions.findUnusedColumnName("countAll", df.schema)
    val countSensitiveProbCol = DatasetExtensions.findUnusedColumnName("countSensitiveProb", df.schema)

    val benefits = df
      .groupBy(getSensitiveCols map col: _*)
      .agg(
        count("*").cast(DoubleType).alias(countSensitiveCol)
      )
      .withColumn(countAllCol, lit(numRows))
      .withColumn(countSensitiveProbCol, col(countSensitiveCol) / col(countAllCol))

    if (getVerbose)
      benefits.cache.show(numRows = 20, truncate = false)

    calculateAggregateMeasures(benefits, countSensitiveProbCol)
  }

  private def calculateAggregateMeasures(benefitsDf: DataFrame, benefitCol: String): DataFrame = {
    val Row(numBenefits: Double, meanBenefits: Double) =
      benefitsDf.agg(count("*").cast(DoubleType), mean(benefitCol).cast(DoubleType)).head

    val metricsMap = AggregateMetrics(benefitCol, numBenefits, meanBenefits, getEpsilon, getErrorTolerance).toMap
    val metricsCols = metricsMap.values.toSeq

    val aggDf = benefitsDf.agg(metricsCols.head, metricsCols.tail: _*)

    if (getVerbose)
      aggDf.cache.show(truncate = false)

    val measureTuples = metricsMap.flatMap {
      case (metricName, _) =>
        lit(metricName) :: col(metricName) :: Nil
    }.toSeq

    aggDf.withColumn(getAggregateMeasuresCol, map(measureTuples: _*)).select(getAggregateMeasuresCol)
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

  private def validateSchema(schema: StructType): Unit = {
  }
}

case class AggregateMetrics(benefitCol: String,
                            numBenefits: Double,
                            meanBenefits: Double,
                            epsilon: Double,
                            errorTolerance: Double) {
  private val normBenefit = col(benefitCol) / meanBenefits

  def toMap: Map[String, Column] = Map(
    "atkinson_index" -> atkinsonIndex.alias("atkinson_index"),
    "thiel_l_index" -> thielLIndex.alias("thiel_l_index"),
    "thiel_t_index" -> thielTIndex.alias("thiel_t_index")
  )

  def atkinsonIndex: Column = {
    val alpha = 1d - epsilon
    val productExpression = exp(sum(log(normBenefit)))
    val powerMeanExpression = sum(pow(normBenefit, alpha)) / numBenefits
    when(
      abs(lit(alpha)) < errorTolerance,
      lit(1d) - pow(productExpression, 1d / numBenefits)
    ).otherwise(
      lit(1d) - pow(powerMeanExpression, 1d / alpha)
    )
  }

  def thielLIndex: Column = {
    val negativeSumLog = sum(log(normBenefit) * -1d)
    negativeSumLog / numBenefits
  }

  def thielTIndex: Column = {
    val sumLog = sum(normBenefit * log(normBenefit))
    sumLog / numBenefits
  }
}
