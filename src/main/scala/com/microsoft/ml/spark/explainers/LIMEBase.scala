// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.explainers

import breeze.linalg.{*, DenseMatrix => BDM, DenseVector => BDV}
import com.microsoft.ml.spark.codegen.Wrappable
import com.microsoft.ml.spark.core.schema.DatasetExtensions
import com.microsoft.ml.spark.explainers.BreezeUtils._
import com.microsoft.ml.spark.logging.BasicLogging
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

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
    with Wrappable
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

  final override def transform(instances: Dataset[_]): DataFrame = logTransform {
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
      .repartition()

    val scored = getModel.transform(samples)

    val explainTargetCol = DatasetExtensions.findUnusedColumnName("target", scored)

    val modelOutput = scored.withColumn(explainTargetCol, this.getExplainTarget(scored.schema))

    val fitted = modelOutput.groupByKey(row => row.getAs[Long](idCol)).mapGroups {
      case (id: Long, rows: Iterator[Row]) =>
        val (inputs, outputs, weights) = rows.map {
          row =>
            val input = row.getAs[Vector](stateCol).toBreeze
            val output = row.getAs[Vector](explainTargetCol).toBreeze
            val weight = row.getAs[Double](weightCol)
            (input, output, weight)
        }.toSeq.unzip3

        val inputsBV = BDM(inputs: _*)
        val outputsBV = BDM(outputs: _*)
        val weightsBV = BDV(weights: _*)

        val lassoResults = outputsBV(::, *).toIndexedSeq.map {
          new LassoRegression(regularization).fit(inputsBV, _, weightsBV, fitIntercept = true)
        }

        val coefficientsMatrix = lassoResults.map(_.coefficients.toSpark)
        val metrics = BDV(lassoResults.map(_.rSquared): _*)

        (id, coefficientsMatrix, metrics.toSpark)
    }.toDF(idCol, this.getOutputCol, this.getMetricsCol)

    preprocessed.hint("broadcast").join(fitted, Seq(idCol), "inner").drop(idCol)
  }

  override def copy(extra: ParamMap): Transformer = this.defaultCopy(extra)

  protected def createSamples(df: DataFrame,
                              idCol: String,
                              stateCol: String,
                              distanceCol: String): DataFrame

  protected override def validateSchema(schema: StructType): Unit = {
    super.validateSchema(schema)

    require(
      !schema.fieldNames.contains(getMetricsCol),
      s"Input schema (${schema.simpleString}) already contains metrics column $getMetricsCol"
    )
  }

  override def transformSchema(schema: StructType): StructType = {
    this.validateSchema(schema)
    schema
      .add(getOutputCol, ArrayType(VectorType))
      .add(getMetricsCol, VectorType)
  }

  protected val sampleField = "sample"
  protected val stateField = "state"
  protected val distanceField = "distance"

  protected def getSampleSchema(sampleType: DataType): DataType = {
    ArrayType(
      StructType(Seq(
        StructField(sampleField, sampleType),
        StructField(stateField, VectorType),
        StructField(distanceField, DoubleType)
      ))
    )
  }
}
