package com.microsoft.azure.synapse.ml.exploratory

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.core.schema.DatasetExtensions
import com.microsoft.azure.synapse.ml.logging.BasicLogging
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable, Transformer}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// This feature is experimental. It is subject to change or removal in future releases.
class AggregateMeasures(override val uid: String)
  extends Transformer
    with ComplexParamsWritable
    with DataBalanceParams
    with Wrappable
    with BasicLogging {

  logClass()

  def this() = this(Identifiable.randomUID("AggregateMeasures"))

  val aggregateMeasuresCol = new Param[String](
    this,
    "aggregateMeasuresCol",
    "Output column name for aggregate measures."
  )

  def getAggregateMeasuresCol: String = $(aggregateMeasuresCol)

  def setAggregateMeasuresCol(value: String): this.type = set(aggregateMeasuresCol, value)

  val epsilon = new DoubleParam(
    this,
    "epsilon",
    "Epsilon value for Atkinson Index. Inverse of alpha (1 - alpha)."
  )

  def getEpsilon: Double = $(epsilon)

  def setEpsilon(value: Double): this.type = set(epsilon, value)

  val errorTolerance = new DoubleParam(
    this,
    "errorTolerance",
    "Error tolerance value for Atkinson Index."
  )

  def getErrorTolerance: Double = $(errorTolerance)

  def setErrorTolerance(value: Double): this.type = set(errorTolerance, value)

  setDefault(
    aggregateMeasuresCol -> "AggregateMeasures",
    epsilon -> 1d,
    errorTolerance -> 1e-12
  )

  override def transform(dataset: Dataset[_]): DataFrame = {
    validateSchema(dataset.schema)

    val df = dataset.cache
    val numRows = df.count.toDouble

    val featureCountCol = DatasetExtensions.findUnusedColumnName("featureCount", df.schema)
    val rowCountCol = DatasetExtensions.findUnusedColumnName("rowCount", df.schema)
    val featureProbCol = DatasetExtensions.findUnusedColumnName("featureProb", df.schema)

    val featureStats = df
      .groupBy(getSensitiveCols map col: _*)
      .agg(count("*").cast(DoubleType).alias(featureCountCol))
      .withColumn(rowCountCol, lit(numRows))
      // P(sensitive)
      .withColumn(featureProbCol, col(featureCountCol) / col(rowCountCol))

    if (getVerbose)
      featureStats.cache.show(numRows = 20, truncate = false)

    calculateAggregateMeasures(featureStats, featureProbCol)
  }

  private def calculateAggregateMeasures(featureStats: DataFrame, featureProbCol: String): DataFrame = {
    val Row(numFeatures: Double, meanFeatures: Double) =
      featureStats.agg(count("*").cast(DoubleType), mean(featureProbCol).cast(DoubleType)).head

    val metricsMap = AggregateMetrics(
      featureProbCol, numFeatures, meanFeatures, getEpsilon, getErrorTolerance).toColumnMap
    val metricsCols = metricsMap.values.toSeq

    val aggDf = featureStats.agg(metricsCols.head, metricsCols.tail: _*)

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
}

object AggregateMeasures extends ComplexParamsReadable[AggregateMeasures]

case class AggregateMetrics(featureProbCol: String,
                            numFeatures: Double,
                            meanFeatures: Double,
                            epsilon: Double,
                            errorTolerance: Double) {
  private val normFeatureProbCol = col(featureProbCol) / meanFeatures

  def toColumnMap: Map[String, Column] = Map(
    "atkinson_index" -> atkinsonIndex.alias("atkinson_index"),
    "theil_l_index" -> theilLIndex.alias("theil_l_index"),
    "theil_t_index" -> theilTIndex.alias("theil_t_index")
  )

  def atkinsonIndex: Column = {
    val alpha = 1d - epsilon
    val productExpression = exp(sum(log(normFeatureProbCol)))
    val powerMeanExpression = sum(pow(normFeatureProbCol, alpha)) / numFeatures
    when(
      abs(lit(alpha)) < errorTolerance,
      lit(1d) - pow(productExpression, 1d / numFeatures)
    ).otherwise(
      lit(1d) - pow(powerMeanExpression, 1d / alpha)
    )
  }

  def theilLIndex: Column = {
    val negativeSumLog = sum(log(normFeatureProbCol) * -1d)
    negativeSumLog / numFeatures
  }

  def theilTIndex: Column = {
    val sumLog = sum(normFeatureProbCol * log(normFeatureProbCol))
    sumLog / numFeatures
  }
}
