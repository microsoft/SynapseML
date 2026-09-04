// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split1

import com.microsoft.azure.synapse.ml.io.http.SharedSingleton
import com.microsoft.azure.synapse.ml.lightgbm.booster.LightGBMBooster
import com.microsoft.azure.synapse.ml.lightgbm.{ColumnParams, LightGBMRegressor, NetworkManager, NetworkParams,
  NetworkTopologyInfo, PartitionTaskContext, PartitionTaskTrainingState, SharedState, TaskInstrumentationMeasures,
  TrainingContext, TrainUtils}
import org.apache.spark.ml.linalg.SQLDataTypes
import org.apache.spark.sql.types.{StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.LoggerFactory

class TrainUtilsSuite extends AnyFunSuite {

  private val log = LoggerFactory.getLogger(classOf[TrainUtilsSuite])

  private val lowerIsBetterMetrics = Seq(
    "rmse",
    "l1",
    "mae",
    "l2",
    "mse",
    "binary_logloss",
    "binary_error",
    "multi_logloss",
    "multi_error",
    "mape",
    "quantile",
    "huber",
    "fair",
    "poisson",
    "gamma",
    "gamma_deviance",
    "tweedie",
    "cross_entropy",
    "kullback_leibler")

  private val higherIsBetterMetrics = Seq(
    "auc",
    "auc_mu",
    "ndcg@1",
    "ndcg@10",
    "map@1",
    "map@10",
    "average_precision")

  private val tolerances = Seq(0.0, 0.25, 2.0, 25.0)

  private def newTrainingState(booster: LightGBMBooster): PartitionTaskTrainingState = {
    val featuresField = StructField("features", SQLDataTypes.VectorType)
    val trainParams = new LightGBMRegressor().getTrainParams(
      numTasks = 1,
      featuresSchema = featuresField,
      numTasksPerExec = 1)
    val trainingContext = TrainingContext(
      batchIndex = 0,
      sharedStateSingleton = SharedSingleton(new SharedState(trainParams)),
      schema = StructType(Seq(featuresField)),
      numCols = 1,
      numInitScoreClasses = 0,
      trainingParams = trainParams,
      networkParams = NetworkParams(12400, "127.0.0.1", 12400, barrierExecutionMode = false),
      columnParams = ColumnParams("label", "features", None, None, None),
      datasetParams = "",
      featureNames = None,
      numTasksPerExecutor = 1,
      validationData = None,
      serializedReferenceDataset = None,
      partitionCounts = Some(Array(1L)))
    val taskContext = PartitionTaskContext(
      trainingCtx = trainingContext,
      partitionId = 0,
      taskId = 0L,
      measures = new TaskInstrumentationMeasures(0),
      networkTopologyInfo = NetworkTopologyInfo("127.0.0.1:12400", Array(0), 12400),
      shouldExecuteTraining = true,
      isEmptyPartition = false,
      shouldReturnBooster = true,
      shouldCalcValidationDataset = false)

    PartitionTaskTrainingState(taskContext, booster)
  }

  test("Improvement tolerance is symmetric across metrics and tolerance values") {
    val bestScore = 100.0
    val margin = 0.125

    tolerances.foreach { tolerance =>
      lowerIsBetterMetrics.foreach { metric =>
        assert(TrainUtils.isImprovement(metric, bestScore - tolerance - margin, bestScore, tolerance))
        assert(!TrainUtils.isImprovement(metric, bestScore - tolerance, bestScore, tolerance))
        assert(!TrainUtils.isImprovement(metric, bestScore - tolerance / 2, bestScore, tolerance))
        assert(!TrainUtils.isImprovement(metric, bestScore + margin, bestScore, tolerance))
      }

      higherIsBetterMetrics.foreach { metric =>
        assert(TrainUtils.isImprovement(metric, bestScore + tolerance + margin, bestScore, tolerance))
        assert(!TrainUtils.isImprovement(metric, bestScore + tolerance, bestScore, tolerance))
        assert(!TrainUtils.isImprovement(metric, bestScore + tolerance / 2, bestScore, tolerance))
        assert(!TrainUtils.isImprovement(metric, bestScore - margin, bestScore, tolerance))
      }
    }
  }

  test("Main worker endpoint parser keeps hostname and IPv4 behavior") {
    val endpoints = Seq(
      "worker.example.test:12404" -> ("worker.example.test", 12404),
      "localhost:80" -> ("localhost", 80),
      "192.0.2.10:65535" -> ("192.0.2.10", 65535),
      "127.0.0.1:00080" -> ("127.0.0.1", 80))

    endpoints.foreach { case (endpoint, expected) =>
      assert(NetworkManager.parseHostAndPort(endpoint) == expected)
      assert(NetworkManager.getMainWorkerPort(s"$endpoint,backup.example.test:12405", log) == expected._2)
    }
  }

  test("Main worker endpoint parser supports bracketed and practical bare IPv6") {
    val endpoints = Seq(
      "[2001:db8::1]:12404" -> ("2001:db8::1", 12404),
      "2001:db8::1:12404" -> ("2001:db8::1", 12404),
      "2001:db8:0:1:2:3:4:5:12404" -> ("2001:db8:0:1:2:3:4:5", 12404),
      "[::1]:443" -> ("::1", 443),
      "::1:12404" -> ("::1", 12404),
      "2001:db8:::12404" -> ("2001:db8::", 12404),
      "[fe80::a%eth0]:12404" -> ("fe80::a%eth0", 12404),
      "fe80::a%3:12404" -> ("fe80::a%3", 12404))

    endpoints.foreach { case (endpoint, expected) =>
      assert(NetworkManager.parseHostAndPort(endpoint) == expected)
      assert(NetworkManager.getMainWorkerPort(endpoint, log) == expected._2)
    }
  }

  test("Main worker endpoint parser rejects malformed hosts and ports with actionable errors") {
    val malformedEndpoints = Seq(
      "" -> "endpoint is empty",
      "worker.example.test" -> "missing ':' port separator",
      ":12404" -> "host is empty",
      "worker.example.test:" -> "port is empty",
      "worker.example.test:not-a-port" -> "not a decimal integer",
      "worker.example.test:+80" -> "not a decimal integer",
      "worker.example.test:-1" -> "not a decimal integer",
      "worker.example.test:0" -> "outside the valid range",
      "worker.example.test:65536" -> "outside the valid range",
      "worker.example.test:999999999999999999999" -> "too large",
      "[2001:db8::1]12404" -> "must be followed by a ':' port separator",
      "[2001:db8::1" -> "missing its closing ']'",
      "[2001:db8::1]" -> "missing its port",
      "[worker.example.test]:12404" -> "brackets are only valid around an IPv6 literal",
      "2001:db8::1" -> "bare IPv6 endpoint is ambiguous",
      "2001:db8::1:10" -> "bare IPv6 endpoint is ambiguous",
      "2001:db8:::1:12404" -> "not a valid IPv6 literal",
      "fe80::1%:12404" -> "IPv6 zone identifier is empty",
      "fe80::1%eth0%extra:12404" -> "IPv6 zone identifier is malformed",
      "worker name:12404" -> "host contains whitespace, control characters, or an endpoint delimiter")

    malformedEndpoints.foreach { case (endpoint, expectedMessage) =>
      val failure = intercept[IllegalArgumentException](NetworkManager.parseHostAndPort(endpoint))
      assert(failure.getMessage.contains(expectedMessage))
      assert(failure.getMessage.contains("Expected hostname:port"))
    }

    val nullFailure = intercept[IllegalArgumentException] {
      NetworkManager.parseHostAndPort(null) //scalastyle:ignore null
    }
    assert(nullFailure.getMessage.contains("endpoint is null"))
    assert(intercept[IllegalArgumentException](NetworkManager.getMainWorkerPort(
      null, log)).getMessage.contains("network node list is null")) //scalastyle:ignore null
    assert(intercept[IllegalArgumentException](NetworkManager.getMainWorkerPort(
      ",worker.example.test:12404", log)).getMessage.contains("endpoint is empty"))
  }

  test("Zero early stopping rounds disable wrapper early stopping") {
    assert(!TrainUtils.shouldStopEarly(
      iteration = 100,
      bestIteration = 0,
      earlyStoppingRound = 0))
  }

  test("Positive early stopping rounds stop only when the round boundary is reached") {
    assert(!TrainUtils.shouldStopEarly(iteration = 4, bestIteration = 0, earlyStoppingRound = 5))
    assert(TrainUtils.shouldStopEarly(iteration = 5, bestIteration = 0, earlyStoppingRound = 5))
    assert(TrainUtils.shouldStopEarly(iteration = 10, bestIteration = 5, earlyStoppingRound = 5))
  }

  test("A native iteration failure is not reported as completed training") {
    val nativeFailure = new RuntimeException("injected native iteration failure")
    val booster = new LightGBMBooster() {
      override def updateOneIteration(): Boolean = throw nativeFailure
    }
    val state = newTrainingState(booster)

    val thrown = intercept[RuntimeException] {
      TrainUtils.updateOneIteration(state, log)
    }

    assert(thrown eq nativeFailure)
    assert(!state.isFinished)
  }

  test("Early stopping parameters accept valid values and reject invalid values") {
    val learner = new LightGBMRegressor()

    assert(learner.getEarlyStoppingRound == 0)
    assert(learner.getImprovementTolerance == 0.0)

    Seq(0, 1, 100, Int.MaxValue).foreach { earlyStoppingRound =>
      assert(learner.setEarlyStoppingRound(earlyStoppingRound).getEarlyStoppingRound == earlyStoppingRound)
    }
    assertThrows[IllegalArgumentException](learner.setEarlyStoppingRound(-1))

    Seq(0.0, 0.25, 25.0, Double.MaxValue).foreach { tolerance =>
      assert(learner.setImprovementTolerance(tolerance).getImprovementTolerance == tolerance)
    }

    Seq(-0.25, Double.NegativeInfinity, Double.NaN).foreach { tolerance =>
      assertThrows[IllegalArgumentException](learner.setImprovementTolerance(tolerance))
    }
  }
}
