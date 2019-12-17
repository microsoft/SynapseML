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
           (user == 0 AND ((timeOfDay == 0 AND chosenAction == 1) OR (timeOfDay == 1 AND chosenAction == 2))) OR
           (user == 1 AND ((timeOfDay == 0 AND chosenAction == 3) OR (timeOfDay == 1 AND chosenAction == 1)))
        """.stripMargin), 1).otherwise(0))

  def generateContext(n: Int): Seq[Context] = {
    for (i <- 0 to n) yield {
      val userIdx = Random.nextInt(2) // Tom, Anna
      val timeOfDayIdx = Random.nextInt(2) // morning, afternoon

      Context(
        userIdx,
        timeOfDayIdx,
        // user context
        new SparseVector(4, Array(userIdx, 2 + timeOfDayIdx), Array(1.0, 1.0)),
        // articles
        List.range(0, 4).map({ i => new SparseVector(4, Array(i), Array(1.0)) }).toSeq
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
          .getOrElse(probabilities.length)
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
    var df = session.createDataFrame(Simulator.generateContext(5000))
      .coalesce(1)

    // feature engineering
    val assembler = new NestedVectorAssembler
    df = assembler.transform(df)

    // 2. evaluate policy
    df = policy.transform(df)

    // 3. sample from distribution over actions
    df = Simulator.sampleAction(df)

    // 4. simulate behavior
    df = Simulator.addReward(df)

    df = df.cache()


//    df.printSchema

    // 5. train policy -> pi'
    val cb = new ContextualBandit()
      .setLabelCol("reward")
      .setEstimator(new LightGBMRegressor()
        //        .setNumIterations(500)
        //        .setMetric("l2")
      )

    val newPolicy = cb.fit(df)
      .setExplorationStrategy(new PassThrough)

    val importance = newPolicy.getModel
      .asInstanceOf[LightGBMRegressionModel]
      .getFeatureImportances("gain")

    println(importance.mkString(","))

    df = newPolicy.transform(df)

    df.show(10, false)
    df.where("reward == 1")
      .withColumn("predictedAction",
        array_position(col("prediction"), array_max(col("prediction"))))
      .show(10, false)

    df
      .withColumn("predictedAction",
        array_position(col("prediction"), array_max(col("prediction"))))
      .withColumn("match",
          when(
            (col("reward") === lit(1.0)) &&
            (col("chosenAction") === col("predictedAction"))
            , 1).otherwise(0))
      .selectExpr("SUM(reward) AS clicks",
        "COUNT(*) as N",
        "SUM(match) AS click_matches")
      .show
  }
}
