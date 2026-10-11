// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split1

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.lightgbm.{LightGBMRegressor, LightGBMUtils}
import org.apache.spark.{ExceptionFailure, SparkException}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.DataFrame

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

// scalastyle:off magic.number
class RandomForestGuidanceSuite extends TestBase {
  override protected def beforeAll(): Unit = {
    super.beforeAll()
    sparkProvider.resetSparkSession(numRetries = 4, numCores = Some(2))
  }

  override protected def afterAll(): Unit = {
    try {
      sparkProvider.resetSparkSession()
    } finally {
      super.afterAll()
    }
  }

  private def trainingData(sparse: Boolean): DataFrame = {
    import spark.implicits._
    (0 until 96).map { index =>
      val x = index.toDouble
      val features = Vectors.dense(x, x * x, x % 7.0)
      (2.0 * x, if (sparse) features.toSparse else features)
    }.toDF("label", "features")
  }

  private def regressor: LightGBMRegressor =
    new LightGBMRegressor()
      .setBoostingType("rf")
      .setNumTasks(1)
      .setNumThreads(1)
      .setNumIterations(4)
      .setNumLeaves(5)
      .setMinDataInLeaf(1)
      .setSeed(42)

  test("Random-forest guidance retains the first repeated native parameter value") {
    val cases = Seq(
      "boosting=rf boosting=gbdt" -> true,
      "boosting=gbdt boosting=rf" -> false,
      "boosting=rf bagging_freq=0 bagging_freq=1 bagging_fraction=0.8" -> true,
      "boosting=rf bagging_freq=1 bagging_freq=0 bagging_fraction=0.8" -> false,
      "boosting=rf bagging_freq=1 bagging_fraction=1.0 bagging_fraction=0.8" -> true,
      "boosting=rf bagging_freq=1 bagging_fraction=0.8 bagging_fraction=1.0" -> false,
      "boosting=rf feature_fraction=1.0 feature_fraction=0.8" -> true,
      "boosting=rf feature_fraction=0.8 feature_fraction=1.0" -> false)
    cases.foreach { case (parameters, expected) =>
      assert(LightGBMUtils.boosterFailureGuidance(parameters).contains("random-forest") == expected, parameters)
    }
  }

  test("Random-forest guidance respects canonical and conflicting feature sampling aliases") {
    val cases = Seq(
      "boosting=rf feature_fraction=1.0 colsample_bytree=0.8" -> true,
      "boosting=rf feature_fraction=0.8 colsample_bytree=1.0" -> false,
      "boosting=rf sub_feature=1.0 colsample_bytree=0.8" -> true,
      "boosting=rf sub_feature=0.8 colsample_bytree=1.0" -> false)
    cases.foreach { case (parameters, expected) =>
      assert(LightGBMUtils.boosterFailureGuidance(parameters).contains("random-forest") == expected, parameters)
    }
  }

  for {
    transferMode <- Seq("bulk", "streaming")
    sparse <- Seq(false, true)
    sampling <- Seq("bagging", "features")
  } {
    test(s"Random-forest regression trains with $sampling sampling in $transferMode mode, sparse=$sparse") {
      val data = trainingData(sparse)
      val learner = regressor.setDataTransferMode(transferMode)
      sampling match {
        case "bagging" => learner.setBaggingFreq(1).setBaggingFraction(0.8)
        case "features" => learner.setFeatureFraction(0.8)
      }
      val predictions = learner.fit(data).transform(data).select("prediction").collect().map(_.getDouble(0))
      assert(predictions.length == 96)
      assert(predictions.forall(value => !value.isNaN && !value.isInfinity))
      assert(predictions.distinct.length > 1)
    }
  }

  test("Random-forest failure keeps guidance on the first attempt and identifies the later retry failures") {
    val failures = new LinkedBlockingQueue[(Int, String)]()
    val listener = new SparkListener {
      override def onTaskEnd(event: SparkListenerTaskEnd): Unit = event.reason match {
        case failure: ExceptionFailure
          if failure.description.contains("Booster call failed in LightGBM") ||
            failure.description.contains("driver network topology endpoint") =>
          failures.add((event.taskInfo.attemptNumber, failure.description))
        case _ =>
      }
    }
    spark.sparkContext.addSparkListener(listener)
    try {
      val failure = intercept[SparkException] {
        regressor.fit(trainingData(sparse = false))
      }
      val attempts = (0 until 4).map { _ =>
        val event = Option(failures.poll(10, TimeUnit.SECONDS))
        assert(event.isDefined, "Expected the native failure followed by three failed task retries")
        event.get
      }.sortBy(_._1)
      assert(attempts.map(_._1) == Seq(0, 1, 2, 3))
      assert(attempts.head._2.contains("config->bagging_freq"))
      assert(attempts.head._2.contains("random-forest sampling requirement"))
      assert(attempts.head._2.contains("baggingFreq greater than 0"))
      assert(attempts.tail.forall(_._2.contains("driver network topology endpoint")))
      assert(failure.getMessage.contains("first failed attempt"))
      assert(failure.getMessage.contains("retry attempt 3"))
      assert(!failure.getMessage.contains("random-forest sampling requirement"))
    } finally {
      spark.sparkContext.removeSparkListener(listener)
    }
  }
}
