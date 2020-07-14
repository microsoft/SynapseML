// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.cb

import java.io.File
import java.nio.file.Files

import com.microsoft.ml.spark.core.test.base.TestBase
import com.microsoft.ml.spark.lightgbm.{LightGBMRegressionModel, LightGBMRegressor}
import com.microsoft.ml.spark.vw.{VowpalWabbitBaseModel, VowpalWabbitContextualBandit, VowpalWabbitFeaturizer}
import org.apache.spark.ml.util.{Identifiable, MLReadable}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoders, Row}
import org.apache.spark.ml.Transformer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.Pipeline
import org.vowpalwabbit.spark.VowpalWabbitMurmur

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

  def generateContext(n: Int, seed: Int): Seq[Context] = {
    val rnd = new Random(seed)

    for (i <- 0 to n) yield {
      val userIdx = rnd.nextInt(2) // Tom, Anna
      val timeOfDayIdx = rnd.nextInt(2) // morning, afternoon

      Context(
        userIdx,
        timeOfDayIdx,
        // user context
        new SparseVector(4, Array(userIdx, 2 + timeOfDayIdx), Array(1.0, 1.0)),
        // articles
        List.range(0, 7).map({ i => new SparseVector(7, Array(i), Array(1.0)) }).toSeq
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

  def epoch(policy: Transformer, n: Int, seed: Int): Transformer = {
    // 1. generate context
    var df = session.createDataFrame(Simulator.generateContext(n, seed))
      .coalesce(1)
      .cache()

    // feature engineering
    val assembler = new NestedVectorAssembler
    df = assembler.transform(df)

    // 2. evaluate policy
    df = policy.transform(df)

    // 3. sample from distribution over actions
    df = Simulator.sampleAction(df).cache()

    // 4. simulate behavior
    df = Simulator.addReward(df)

    df = df.cache()

    val regressor = new LightGBMRegressor()
      .setVerbosity(-1)

    policy match {
      case cb: ContextualBanditModel => {
        cb.getModel match {
          case lgbm: LightGBMRegressionModel => {
            regressor.setModelString(regressor.stringFromTrainedModel(lgbm))
//            println("continue training")
          }
        }
      }
      case _ => {}
    }

    // 5. train policy -> pi'
    val cb = new ContextualBandit()
      .setLabelCol("reward")
      .setEstimator(regressor)

    val newPolicy = cb.fit(df)
      .setExplorationStrategy(new EpsilonGreedy(0.2))
//       .setExplorationStrategy(new PassThrough)

//    val importance = newPolicy.getModel
//      .asInstanceOf[LightGBMRegressionModel]
//      .getFeatureImportances("gain")
//
//    println(importance.mkString(","))

    df = newPolicy.transform(df)

//    df.show(10, false)
//    df.where("reward == 1")
//      .withColumn("predictedAction",
//        array_position(col("prediction"), array_max(col("prediction"))))
//      .show(10, false)

    df
      .withColumn("predictedAction",
        array_position(col("prediction"), array_max(col("prediction"))))
      .withColumn("match",
        when((col("chosenAction") === col("predictedAction"))
          , 1).otherwise(0))
      .withColumn("matchClick",
        when(
          (col("reward") === lit(1.0)) &&
            (col("chosenAction") === col("predictedAction"))
          , 1).otherwise(0))
      .selectExpr("SUM(reward) AS clicks",
        "COUNT(*) as N",
        "SUM(match) as matches",
        "SUM(matchClick) AS matchesClick")
      .show
//
//    df.groupBy(
//      col("user"),
//      col("timeOfDay"),
//      col("chosenAction")).sum("reward")
//      .where(col("SUM(reward)") > 0)
//      .show(100)

    newPolicy.setPredictionCol("probabilities")
  }

  test ("Verify Contextual Bandit") {
   // epoch(pi)
    var policy: Transformer = new UniformRandomPolicy

    for (i <- 0 to 10) {
      policy = epoch(policy, 100, i + 42)
    }
  }

  def epochVowpalWabbit(policy: Transformer, n: Int, seed: Int): Transformer = {
    // 1. generate context
    var df = session.createDataFrame(Simulator.generateContext(n, seed))
      .coalesce(1)
      .cache()

    // feature engineering
    // Use VW -q directly

    // 2. evaluate policy
    df = policy.transform(df)
    df.show

    // 3. sample from distribution over actions
    df = Simulator.sampleAction(df).cache()

    // 4. simulate behavior
    df = Simulator.addReward(df)

    df = df.cache()

    // 5. train policy -> pi'
    val cb = new VowpalWabbitContextualBandit()
      .setArgs("--cb_explore_adf --epsilon 0.2 --quiet")
      .setLabelCol("reward")

    // continuous training
    policy match {
      case vw: VowpalWabbitBaseModel => cb.setInitialModel(vw.getModel)
      // avoid having quadratics twice by only applying them if we don have an inital model
      case _ => cb.setArgs(cb.getArgs + " -q sf")
    }

//    df.printSchema
//    df.show(5, false)

    val newPolicy = cb.fit(df)
    newPolicy.setTestArgs("--quiet")

    df = newPolicy.transform(df)
    df.show
    //    df.show(10, false)
    //    df.where("reward == 1")
    //      .withColumn("predictedAction",
    //        array_position(col("prediction"), array_max(col("prediction"))))
    //      .show(10, false)

    df
      .withColumn("predictedAction",
        array_position(col("prediction"), array_max(col("prediction"))))
      .withColumn("match",
        when((col("chosenAction") === col("predictedAction"))
          , 1).otherwise(0))
      .withColumn("matchClick",
        when(
          (col("reward") === lit(1.0)) &&
            (col("chosenAction") === col("predictedAction"))
          , 1).otherwise(0))
      .selectExpr("SUM(reward) AS clicks",
        "COUNT(*) as N",
        "SUM(match) as matches",
        "SUM(matchClick) AS matchesClick")
      .show
    //
    //    df.groupBy(
    //      col("user"),
    //      col("timeOfDay"),
    //      col("chosenAction")).sum("reward")
    //      .where(col("SUM(reward)") > 0)
    //      .show(100)

    newPolicy.setPredictionCol("probabilities")
  }

  test( "Verify VW Contextual Bandit") {
    var policy: Transformer = new UniformRandomPolicy

    for (i <- 0 to 10) {
      policy = epochVowpalWabbit(policy, 100, i + 42)
    }
  }

  /*
    transform dataframe to sparse vectors
    a thing to collapse multiple columns to one sequence

    search for withcolumn in: https://github.com/Azure/mmlspark/blob/04a2fbd31ea3adc857d7d29d6155e00df7532414/src/main/scala/com/microsoft/ml/spark/vw/VowpalWabbitClassifier.scala

  */
  test( "Verify VW Contextual  - SIMPLE") {
    import session.implicits._

    val someDF = Seq(
      (new SparseVector(1, Array(0),Array(1)), Seq(new SparseVector(1, Array(0), Array(1.0))))
    ).toDF("shared", "action")

//    val v1 = featurizer1.transform(df1).select(col("features")).collect.apply(0).getAs[Vector](0)

    someDF.show
  }

  test( "Verify VW Contextual  - featurizer") {
    import session.implicits._

    val someDF = Seq(
      ("thing", "thing", "thing", 0,1, 0.8),
        ("thing", "thing", "thing", 0,1, 0.8),
        ("thing", "thing", "thing", 0,1, 0.8)
    ).toDF("shared", "action1", "action2", "chosen_action", "reward", "prob")
      .coalesce(1)
      .cache()

    val shared_featurizer = new VowpalWabbitFeaturizer()
      .setInputCols(Array("shared"))
      .setOutputCol("shared_features")

    val action_one_featurizer = new VowpalWabbitFeaturizer()
      .setInputCols(Array("action1"))
      .setOutputCol("action1_features")

    val action_two_featurizer = new VowpalWabbitFeaturizer()
      .setInputCols(Array("action2"))
      .setOutputCol("action2_features")

    val action_merger = new ActionMerger()
      .setInputCols(Array("action1_features", "action2_features"))
      .setOutputCol("actions")

    val pipeline = new Pipeline()
      .setStages(Array(shared_featurizer, action_one_featurizer, action_two_featurizer, action_merger))
    val model = pipeline.fit(someDF)
    val v2 = model.transform(someDF)

    v2.show(false)

    val cb = new VowpalWabbitContextualBandit()
      .setArgs("--cb_explore_adf --epsilon 0.2 --quiet")
      .setLabelCol("reward")
      .setProbabilityCol("prob")
      .setChosenActionCol("chosen_action")
      .setSharedCol("shared_features")
      .setFeaturesCol("actions")

    val m = cb.fit(v2)
    println(m.get_estimate())
  }

  test( "Verify action merger can merge two columns") {
    import session.implicits._

    val someDF = Seq(
      ("thing1", "thing2")
    ).toDF("col1", "col2")

    val action_merger = new ActionMerger()
      .setInputCols(Array("col1", "col2"))
      .setOutputCol("combined_things")
    val v1 = action_merger.transform(someDF)
    val result = v1.collect.head.getAs[Seq[String]]("combined_things")
    assert(result == Seq("thing1", "thing2"))
  }
}