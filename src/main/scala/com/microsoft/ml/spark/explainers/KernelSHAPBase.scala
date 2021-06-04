// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.explainers
import breeze.linalg.{sum, DenseMatrix => BDM, DenseVector => BDV}
import com.microsoft.ml.spark.codegen.Wrappable
import com.microsoft.ml.spark.core.schema.DatasetExtensions
import com.microsoft.ml.spark.explainers.BreezeUtils._
import com.microsoft.ml.spark.explainers.KernelSHAPBase.kernelWeight
import com.microsoft.ml.spark.logging.BasicLogging
import org.apache.commons.math3.util.CombinatoricsUtils.{binomialCoefficientDouble => comb}
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.linalg.{Vector => SV, Vectors => SVS}
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

trait KernelSHAPParams extends HasNumSamples with HasMetricsCol {
  self: KernelSHAPBase =>

  setDefault(metricsCol -> "r2")
}

abstract class KernelSHAPBase(override val uid: String)
  extends LocalExplainer
    with KernelSHAPParams
    // Uncomment the "Wrappable" trait and run "sbt packagePython" to generate python bindings at
    // target/scala-2.12/generated/src/python. However, the generated bindings require some manual modification
    // for explainers that implements the HasBackgroundData trait.
    // with Wrappable
    with BasicLogging {

  protected def preprocess(df: DataFrame): DataFrame = df

  override def explain(instances: Dataset[_]): DataFrame = logExplain {
    import instances.sparkSession.implicits._

    this.validateSchema(instances.schema)

    val df = instances.toDF
    val idCol = DatasetExtensions.findUnusedColumnName("id", df)
    val coalitionCol = DatasetExtensions.findUnusedColumnName("coalition", df)
    val weightCol = DatasetExtensions.findUnusedColumnName("weight", df)

    val dfWithId = df.withColumn(idCol, monotonically_increasing_id())
    val preprocessed = preprocess(dfWithId).cache()

    val sampleWeightUdf = UDFUtils.oldUdf(kernelWeight _, DoubleType)
    val samples = createSamples(preprocessed, idCol, coalitionCol)

    val scored = getModel.transform(samples)
    val explainTargetCol = DatasetExtensions.findUnusedColumnName("target", scored)

    val coalitionScores = scored
      .withColumn(explainTargetCol, this.getExplainTarget(scored.schema))
      .groupBy(col(idCol), col(coalitionCol))
      .agg(mean(col(explainTargetCol)).alias(explainTargetCol))
      .withColumn(weightCol, sampleWeightUdf(col(coalitionCol)))

    val fitted = coalitionScores.groupByKey(row => row.getAs[Long](idCol)).mapGroups {
      case (id: Long, rows: Iterator[Row]) =>
        val (inputs, outputs, weights) = rows.map {
          row =>
            val input = row.getAs[SV](coalitionCol).toBreeze
            val output = row.getAs[Double](explainTargetCol)
            val weight = row.getAs[Double](weightCol)
            (input, output, weight)
        }.toSeq.unzip3

        val inputsBV = BDM(inputs: _*)
        val outputsBV = BDV(outputs: _*)
        val weightsBV = BDV(weights: _*)

        val result = new LeastSquaresRegression().fit(inputsBV, outputsBV, weightsBV, fitIntercept = true)
        val shapValues = SVS.dense(result.intercept +: result.coefficients.toArray)
        (id, shapValues, result.rSquared)
    }.toDF(idCol, this.getOutputCol, this.getMetricsCol)

    preprocessed.join(fitted, Seq(idCol), "inner").drop(idCol)
  }

  override def copy(extra: ParamMap): Params = defaultCopy(extra)

  protected override def validateSchema(schema: StructType): Unit = {
    super.validateSchema(schema)

    // TODO: extract the following check to the HasMetricsCol trait
    require(
      !schema.fieldNames.contains(getMetricsCol),
      s"Input schema (${schema.simpleString}) already contains metrics column $getMetricsCol"
    )
  }

  protected def createSamples(df: DataFrame, idCol: String, coalitionCol: String): DataFrame
}

object KernelSHAPBase {
  private[explainers] def kernelWeight(coalition: SV): Double = {
    val activeSize = sum(coalition.toBreeze)
    val inactiveSize = coalition.size - activeSize

    if (activeSize == 0 || inactiveSize == 0) {
      1E6
    } else {
      val numerator = coalition.size - 1
      val denominator = comb(coalition.size, activeSize.toInt) * activeSize * inactiveSize
      numerator / denominator
    }
  }

  private[explainers] def getEffectiveNumSamples(numSamplesParam: Option[Int], numFeature: Int): Int = {
    val minSamplesNeeded = numFeature
    val maxSamplesNeeded = math.pow(2, numFeature)

    val value = numSamplesParam.getOrElse(2 * numFeature + 2048)
    math.max(math.min(value, maxSamplesNeeded).toInt, minSamplesNeeded)
  }
}
