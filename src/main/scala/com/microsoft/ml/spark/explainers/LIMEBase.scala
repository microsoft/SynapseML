package com.microsoft.ml.spark.explainers

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV}
import com.microsoft.ml.spark.core.schema.DatasetExtensions
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.param._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.ml.linalg.{Vector => SV}
import com.microsoft.ml.spark.explainers.BreezeUtils._

trait LIMEParams extends HasNumSamples {
  self: LIMEBase =>

  val regularization = new DoubleParam(
    this,
    "regularization",
    "Regularization param for the lasso. Default value: 0.",
    ParamValidators.gtEq(0)
  )

  val kernelWidth = new DoubleParam(
    this,
    "kernelWidth",
    "Kernel width. Default value: sqrt (number of features) * 0.75",
    ParamValidators.gt(0)
  )

  val metricsCol = new Param[String](
    this,
    "metricsCol",
    "Column name for fitting metrics"
  )

  def getRegularization: Double = $(regularization)

  def setRegularization(v: Double): this.type = set(regularization, v)

  def getKernelWidth: Double = $(kernelWidth)

  def setKernelWidth(v: Double): this.type = set(kernelWidth, v)

  def getMetricsCol: String = $(metricsCol)

  def setMetricsCol(v: String): this.type = this.set(metricsCol, v)

  setDefault(numSamples -> 1000, regularization -> 0.0, kernelWidth -> 0.75, metricsCol -> "r2")
}

abstract class LIMEBase(override val uid: String) extends LocalExplainer with LIMEParams {
  import spark.implicits._

  protected var backgroundData: Option[DataFrame] = None

  def setBackgroundDataset(backgroundDataset: DataFrame): this.type = {
    this.backgroundData = Some(backgroundDataset)
    this
  }

  private def getSampleWeightUdf: UserDefinedFunction = {
    val kernelWidth = this.getKernelWidth

    val kernelFunc = (distance: Double) => {
      val t = distance / kernelWidth
      math.sqrt(math.exp(-t * t))
    }

    val weightUdf = UDFUtils.oldUdf(kernelFunc, DoubleType)
    weightUdf
  }

  final override def explain(instances: Dataset[_]): DataFrame = {

    this.validateSchema(instances.schema)

    val regularization = this.getRegularization
    val df = instances.toDF
    val idCol = DatasetExtensions.findUnusedColumnName("id", df)
    val weightCol = DatasetExtensions.findUnusedColumnName("weight", df)
    val featureCol = DatasetExtensions.findUnusedColumnName("feature", df)
    val distanceCol = DatasetExtensions.findUnusedColumnName("distance", df)

    val dfWithId = df.withColumn(idCol, monotonically_increasing_id()).cache

    val samples = createSamples(dfWithId, idCol, featureCol, distanceCol)
      .withColumn(weightCol, getSampleWeightUdf(col(distanceCol)))

    // DEBUG
    // samples.select(weightCol, distanceCol).show(false)

    val transformed = getModel.transform(samples)

    val explainTargetCol = DatasetExtensions.findUnusedColumnName("target", transformed)

    val modelOutput = transformed.withColumn(explainTargetCol, this.getExplainTarget(transformed.schema))

    // DEBUG
    // modelOutput.select(featureCol, explainTargetCol, weightCol, distanceCol).show(false)

    val fitted = modelOutput.groupByKey(row => row.getAs[Long](idCol)).mapGroups {
      case (id: Long, rows: Iterator[Row]) =>
        val (inputs, outputs, weights) = rows.map {
          row =>
            val input = extractInputVector(row, featureCol)
            val output = row.getAs[Double](explainTargetCol)
            val weight = row.getAs[Double](weightCol)
            (input, output, weight)
        }.toSeq.unzip3

        val inputsBV = BDM(inputs: _*)
        val outputsBV = BDV(outputs: _*)
        val weightsBV = BDV(weights: _*)
        val lassoResults = new LassoRegression(regularization).fit(inputsBV, outputsBV, weightsBV, fitIntercept = true)

        (id, lassoResults.coefficients.toArray, lassoResults.rSquared)
    }.toDF(idCol, this.getOutputCol, this.getMetricsCol)

    dfWithId.join(fitted, Seq(idCol), "inner").drop(idCol)
  }

  override def copy(extra: ParamMap): Params = this.defaultCopy(extra)

  protected def createSamples(df: DataFrame,
                              idCol: String,
                              featureCol: String,
                              distanceCol: String): DataFrame

//  protected def createFeatureStats(df: DataFrame): Seq[FeatureStats[_, _]]

  protected def extractInputVector(row: Row, featureCol: String): BDV[Double] = {
    row.getAs[SV](featureCol).toBreeze
  }

  protected override def validateSchema(schema: StructType): Unit = {
    super.validateSchema(schema)

    require(
      !schema.fieldNames.contains(getMetricsCol),
      s"Input schema (${schema.simpleString}) already contains metrics column $getMetricsCol"
    )

    require(
      !schema.fieldNames.contains(getOutputCol),
      s"Input schema (${schema.simpleString}) already contains output column $getOutputCol"
    )
  }
}
