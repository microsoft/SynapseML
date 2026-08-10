// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split1

import com.microsoft.azure.synapse.ml.lightgbm.{LightGBMRegressor, TrainUtils}
import org.scalatest.funsuite.AnyFunSuite

class TrainUtilsSuite extends AnyFunSuite {

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
