// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.cb

import java.io.File
import java.nio.file.Files

import com.microsoft.ml.spark.core.test.base.TestBase
import com.microsoft.ml.spark.lightgbm.{LightGBMRegressionModel, LightGBMRegressor}
import org.apache.spark.ml.util.{Identifiable, MLReadable}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row}
import org.apache.spark.ml.Transformer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.param.ParamMap

import scala.util.Random

class UniformRandomPolicy(override val uid: String) extends Transformer {
  def this() = this(Identifiable.randomUID("UniformRandomPolicy"))

  override def copy(extra: ParamMap): UniformRandomPolicy = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = schema

  override def transform(dataset: Dataset[_]): DataFrame =
    dataset.withColumn("probabilities",
      array_repeat(
        lit(1.0) / size(col("features")),
        size(col("features"))))
}

case class Context(val user: Int,
                   val timeOfDay: Int,
                   val shared: Vector,
                   val features: Seq[Vector])

object Simulator {
  def addReward(dataset: DataFrame): DataFrame =
    // non-stochastic user preference
    dataset.withColumn("reward",
      when(expr(
        """
           (user == 0 AND ((timeOfDay == 0  AND chosenAction == 0) OR (timeOfDay == 1 AND chosenAction == 1))) OR
           (user == 1 AND ((timeOfDay == 0 AND chosenAction == 2) OR (timeOfDay == 1 AND chosenAction == 0)))
        """.stripMargin), 1).otherwise(0))

  def generateContext(n: Int): Seq[Context] = {
    for (i <- 0 to n) yield {
      val userIdx = Random.nextInt(2) // Tom, Anna
      val timeOfDayIdx = Random.nextInt(2) // morning, afternoon

      Context(
        userIdx,
        timeOfDayIdx,
        // user context
        new SparseVector(5, Array(userIdx, 2 + timeOfDayIdx), Array(1.0, 1.0)),
        // articles
        List.range(0, 7).map({ i => new SparseVector(20, Array(i), Array(1.0)) }).toSeq
      )
    }
  }

  def sampleAction(dataset: DataFrame): DataFrame = {
    val sampleUdf = udf {
      (probabilities: Seq[Double]) => {
        val scale = 1.0 / probabilities.sum
        val draw = Random.nextDouble

        probabilities
          .map { _ * scale } // re-scale
          .scanLeft(0.0)(_ + _) // cumulative sum
          .zipWithIndex
          .collectFirst({ case (p, i) if p > draw => i })
          .getOrElse(probabilities.length - 1)
      }
    }

    dataset.toDF
      .withColumn("chosenAction", sampleUdf(col("probabilities")))
      .withColumn("probability", element_at(col("probabilities"), col("chosenAction")))
  }
}

class VerifyContextualBandit extends TestBase {

  test ("Verify Contextual Bandit") {
   // epoch(pi)
    val policy = new UniformRandomPolicy

    // 1. generate context
    var df = session.createDataFrame(Simulator.generateContext(500))
      .coalesce(1)

    // feature engineering
    val assembler = new NestedVectorAssembler
    df = assembler.transform(df)

    // 2. evaluate policy (Transformer)
    df = policy.transform(df)

    // 3. sample action
    df = Simulator.sampleAction(df)

    // 4. simulate behavior
    df = Simulator.addReward(df)

    // 5. train policy -> pi'

//    df.show(5, false)
    df.printSchema

    val cb = new ContextualBandit()
      .setLabelCol("reward")
      .setEstimator(new LightGBMRegressor())

    val newPolicy = cb.fit(df)

    val importance = newPolicy.getModel
      .asInstanceOf[LightGBMRegressionModel]
      .getFeatureImportances("gain")

    println(importance.mkString(","))
  }
}
