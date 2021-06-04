package com.microsoft.ml.spark.explainers

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV}
import com.microsoft.ml.spark.codegen.Wrappable
import com.microsoft.ml.spark.core.schema.DatasetExtensions
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.param._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.ml.linalg.{Vector => SV}
import com.microsoft.ml.spark.explainers.BreezeUtils._
import com.microsoft.ml.spark.logging.BasicLogging

trait LIMEParams extends HasNumSamples with HasMetricsCol {
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

  def getRegularization: Double = $(regularization)

  def setRegularization(v: Double): this.type = set(regularization, v)

  def getKernelWidth: Double = $(kernelWidth)

  def setKernelWidth(v: Double): this.type = set(kernelWidth, v)

  setDefault(numSamples -> 1000, regularization -> 0.0, kernelWidth -> 0.75, metricsCol -> "r2")
}

abstract class LIMEBase(override val uid: String)
  extends LocalExplainer
    with LIMEParams
    // with Wrappable
    with BasicLogging {

  private def getSampleWeightUdf: UserDefinedFunction = {
    val kernelWidth = this.getKernelWidth

    val kernelFunc = (distance: Double) => {
      val t = distance / kernelWidth
      math.sqrt(math.exp(-t * t))
    }

    val weightUdf = UDFUtils.oldUdf(kernelFunc, DoubleType)
    weightUdf
  }

  protected def preprocess(df: DataFrame): DataFrame = df

  final override def explain(instances: Dataset[_]): DataFrame = logExplain {
    import instances.sparkSession.implicits._
    this.validateSchema(instances.schema)
    val regularization = this.getRegularization
    val df = instances.toDF
    val idCol = DatasetExtensions.findUnusedColumnName("id", df)
    val weightCol = DatasetExtensions.findUnusedColumnName("weight", df)
    val stateCol = DatasetExtensions.findUnusedColumnName("state", df)
    val distanceCol = DatasetExtensions.findUnusedColumnName("distance", df)

    val dfWithId = df.withColumn(idCol, monotonically_increasing_id())
    val preprocessed = preprocess(dfWithId).cache()

    val samples = createSamples(preprocessed, idCol, stateCol, distanceCol)
      .withColumn(weightCol, getSampleWeightUdf(col(distanceCol)))

    val scored = getModel.transform(samples)

    val explainTargetCol = DatasetExtensions.findUnusedColumnName("target", scored)

    val modelOutput = scored.withColumn(explainTargetCol, this.getExplainTarget(scored.schema))

    val fitted = modelOutput.groupByKey(row => row.getAs[Long](idCol)).mapGroups {
      case (id: Long, rows: Iterator[Row]) =>
        val (inputs, outputs, weights) = rows.map {
          row =>
            val input = row.getAs[SV](stateCol).toBreeze
            val output = row.getAs[Double](explainTargetCol)
            val weight = row.getAs[Double](weightCol)
            (input, output, weight)
        }.toSeq.unzip3

        val inputsBV = BDM(inputs: _*)
        val outputsBV = BDV(outputs: _*)
        val weightsBV = BDV(weights: _*)
        val lassoResults = new LassoRegression(regularization).fit(inputsBV, outputsBV, weightsBV, fitIntercept = true)

        (id, lassoResults.coefficients.toSpark, lassoResults.rSquared)
    }.toDF(idCol, this.getOutputCol, this.getMetricsCol)

    preprocessed.join(fitted, Seq(idCol), "inner").drop(idCol)
  }

  override def copy(extra: ParamMap): Params = this.defaultCopy(extra)

  protected def createSamples(df: DataFrame,
                              idCol: String,
                              stateCol: String,
                              distanceCol: String): DataFrame

  protected override def validateSchema(schema: StructType): Unit = {
    super.validateSchema(schema)

    // TODO: extract the following check to the HasMetricsCol trait
    require(
      !schema.fieldNames.contains(getMetricsCol),
      s"Input schema (${schema.simpleString}) already contains metrics column $getMetricsCol"
    )
  }
}
