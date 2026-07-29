// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split1

import com.microsoft.azure.synapse.ml.lightgbm.TrainUtils
import org.scalatest.funsuite.AnyFunSuite

class TrainUtilsSuite extends AnyFunSuite {

  test("Improvement tolerance requires sufficient improvement for lower-is-better metrics") {
    val bestScore = 1.0
    val tolerance = 0.25

    assert(TrainUtils.isImprovement("rmse", 0.5, bestScore, tolerance))
    assert(!TrainUtils.isImprovement("rmse", 0.75, bestScore, tolerance))
    assert(!TrainUtils.isImprovement("rmse", 0.875, bestScore, tolerance))
    assert(!TrainUtils.isImprovement("rmse", 1.125, bestScore, tolerance))
  }

  test("Improvement tolerance remains correct for higher-is-better metrics") {
    val bestScore = 0.5
    val tolerance = 0.25

    Seq("auc", "ndcg@1", "map@1", "average_precision").foreach { metric =>
      assert(TrainUtils.isImprovement(metric, 1.0, bestScore, tolerance))
      assert(!TrainUtils.isImprovement(metric, 0.75, bestScore, tolerance))
      assert(!TrainUtils.isImprovement(metric, 0.625, bestScore, tolerance))
      assert(!TrainUtils.isImprovement(metric, 0.375, bestScore, tolerance))
    }
  }

  test("Zero improvement tolerance requires strict improvement") {
    assert(TrainUtils.isImprovement("rmse", 0.9, 1.0, 0.0))
    assert(!TrainUtils.isImprovement("rmse", 1.0, 1.0, 0.0))
    assert(TrainUtils.isImprovement("auc", 0.6, 0.5, 0.0))
    assert(!TrainUtils.isImprovement("auc", 0.5, 0.5, 0.0))
  }
}
