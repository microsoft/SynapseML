// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.explainers

import breeze.linalg.{*, DenseMatrix => BDM, DenseVector => BDV}
import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.core.schema.DatasetExtensions
import com.microsoft.azure.synapse.ml.core.utils.BreezeUtils._
import com.microsoft.azure.synapse.ml.logging.SynapseMLLogging
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.{DoubleParam, ParamMap, ParamValidators}
import org.apache.spark.ml.stat.Summarizer
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

trait KernelSHAPParams extends HasNumSamples with HasMetricsCol {
  self: KernelSHAPBase =>

  val infWeight = new DoubleParam(
    this,
    "infWeight",
    "The double value to represent infinite weight. Default: 1E8.",
    ParamValidators.gtEq(1)
  )

  def getInfWeight: Double = $(infWeight)

  def setInfWeight(value: Double): this.type = this.set(infWeight, value)

  setDefault(metricsCol -> "r2", infWeight -> 1E8)
}

abstract class KernelSHAPBase(override val uid: String)
  extends LocalExplainer
    with KernelSHAPParams
    with Wrappable
    with SynapseMLLogging {

  override def transform(instances: Dataset[_]): DataFrame = logTransform ({
    import instances.sparkSession.implicits._
    this.validateSchema(instances.schema)

    val df = instances.toDF
    val idCol = DatasetExtensions.findUnusedColumnName("id", df)
    val coalitionCol = DatasetExtensions.findUnusedColumnName("coalition", df)
    val weightCol = DatasetExtensions.findUnusedColumnName("weight", df)
    val targetClasses = DatasetExtensions.findUnusedColumnName("targetClasses", df)

    val dfWithId = df
      .withColumn(idCol, monotonically_increasing_id())
      .withColumn(targetClasses, this.get(targetClassesCol).map(col).getOrElse(lit(getTargetClasses)))

    val preprocessed = preprocess(dfWithId).cache()

    val samples = createSamples(preprocessed, idCol, coalitionCol, weightCol, targetClasses)
      .repartition()

    val scored = getModel.transform(samples)
    val targetCol = DatasetExtensions.findUnusedColumnName("target", scored)

    val coalitionScores = scored
      .withColumn(targetCol, this.extractTarget(scored.schema, targetClasses))
      .groupBy(col(idCol), col(coalitionCol))
      .agg(Summarizer.mean(col(targetCol)).alias(targetCol), mean(weightCol).alias(weightCol))

    val fitted = coalitionScores.groupByKey(row => row.getAs[Long](idCol)).mapGroups {
      case (id: Long, rows: Iterator[Row]) =>
        val (inputs, outputs, weights) = rows.map {
          row =>
            val input = row.getAs[Vector](coalitionCol).toBreeze
            val output = row.getAs[Vector](targetCol).toBreeze
            val weight = row.getAs[Double](weightCol)
            (input, output, weight)
        }.toSeq.unzip3

        val inputsBV = BDM(inputs: _*)
        val outputsBV = BDM(outputs: _*)
        val weightsBV = BDV(weights: _*)

        val wlsResults = outputsBV(::, *).toIndexedSeq.map {
          new LeastSquaresRegression().fit(inputsBV, _, weightsBV, fitIntercept = true)
        }

        val coefficientsMatrix = wlsResults.map(r => Vectors.dense(r.intercept, r.coefficients.toArray: _*))
        val metrics = BDV(wlsResults.map(_.rSquared): _*)
        (id, coefficientsMatrix, metrics.toSpark)
    }.toDF(idCol, this.getOutputCol, this.getMetricsCol)

    preprocessed.join(fitted, Seq(idCol), "inner").drop(idCol)
  }, instances.columns.length)

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  protected def createSamples(df: DataFrame,
                              idCol: String,
                              coalitionCol: String,
                              weightCol: String,
                              targetClassesCol: String): DataFrame

  override def transformSchema(schema: StructType): StructType = {
    this.validateSchema(schema)
    schema
      .add(getOutputCol, ArrayType(VectorType))
      .add(getMetricsCol, VectorType)
  }

  protected val sampleField = "sample"
  protected val coalitionField = "coalition"
  protected val weightField = "weight"

  protected def getSampleSchema(sampleType: DataType): DataType = {
    ArrayType(
      StructType(Seq(
        StructField(sampleField, sampleType),
        StructField(coalitionField, VectorType),
        StructField(weightField, DoubleType)
      ))
    )
  }
}

object KernelSHAPBase {
  /**
    * For Kernel SHAP coalition sampling, the number of samples needed should be numFeature + 2 at minimum (otherwise
    * the least squares regression algorithm cannot run). The maximum should be 2^^numFeature.
    * The default value is set to 2 * numFeature + 2048, following default settings in https://github.com/slundberg/shap
    */
  private[explainers] def getEffectiveNumSamples(numSamplesParam: Option[Int], numFeature: Int): Int = {
    val minSamplesNeeded = numFeature + 2
    val maxSamplesNeeded = math.pow(2, numFeature)

    val value = numSamplesParam.getOrElse(2 * numFeature + 2048)
    math.min(math.max(value, minSamplesNeeded), maxSamplesNeeded.toInt)
  }
}
