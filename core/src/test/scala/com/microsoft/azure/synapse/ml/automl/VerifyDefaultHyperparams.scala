// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.automl

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.ml.classification._

class VerifyDefaultHyperparams extends TestBase {

  test("defaultRange for LogisticRegression returns non-empty array") {
    val lr = new LogisticRegression()
    val ranges = DefaultHyperparams.defaultRange(lr)
    assert(ranges.nonEmpty)
    assert(ranges.length === 3) // regParam, elasticNetParam, maxIter
  }

  test("defaultRange for LogisticRegression includes expected params") {
    val lr = new LogisticRegression()
    val ranges = DefaultHyperparams.defaultRange(lr)
    val paramNames = ranges.map(_._1.name).toSet
    assert(paramNames.contains("regParam"))
    assert(paramNames.contains("elasticNetParam"))
    assert(paramNames.contains("maxIter"))
  }

  test("defaultRange for DecisionTreeClassifier returns non-empty array") {
    val dt = new DecisionTreeClassifier()
    val ranges = DefaultHyperparams.defaultRange(dt)
    assert(ranges.nonEmpty)
    assert(ranges.length === 4) // maxBins, maxDepth, minInfoGain, minInstancesPerNode
  }

  test("defaultRange for DecisionTreeClassifier includes expected params") {
    val dt = new DecisionTreeClassifier()
    val ranges = DefaultHyperparams.defaultRange(dt)
    val paramNames = ranges.map(_._1.name).toSet
    assert(paramNames.contains("maxBins"))
    assert(paramNames.contains("maxDepth"))
    assert(paramNames.contains("minInfoGain"))
    assert(paramNames.contains("minInstancesPerNode"))
  }

  test("defaultRange for GBTClassifier returns non-empty array") {
    val gbt = new GBTClassifier()
    val ranges = DefaultHyperparams.defaultRange(gbt)
    assert(ranges.nonEmpty)
    assert(ranges.length === 7)
  }

  test("defaultRange for GBTClassifier includes expected params") {
    val gbt = new GBTClassifier()
    val ranges = DefaultHyperparams.defaultRange(gbt)
    val paramNames = ranges.map(_._1.name).toSet
    assert(paramNames.contains("maxBins"))
    assert(paramNames.contains("maxDepth"))
    assert(paramNames.contains("minInfoGain"))
    assert(paramNames.contains("minInstancesPerNode"))
    assert(paramNames.contains("maxIter"))
    assert(paramNames.contains("stepSize"))
    assert(paramNames.contains("subsamplingRate"))
  }

  test("defaultRange for RandomForestClassifier returns non-empty array") {
    val rf = new RandomForestClassifier()
    val ranges = DefaultHyperparams.defaultRange(rf)
    assert(ranges.nonEmpty)
    assert(ranges.length === 6)
  }

  test("defaultRange for RandomForestClassifier includes expected params") {
    val rf = new RandomForestClassifier()
    val ranges = DefaultHyperparams.defaultRange(rf)
    val paramNames = ranges.map(_._1.name).toSet
    assert(paramNames.contains("maxBins"))
    assert(paramNames.contains("maxDepth"))
    assert(paramNames.contains("minInfoGain"))
    assert(paramNames.contains("minInstancesPerNode"))
    assert(paramNames.contains("numTrees"))
    assert(paramNames.contains("subsamplingRate"))
  }

  test("defaultRange for MultilayerPerceptronClassifier returns non-empty array") {
    val mlp = new MultilayerPerceptronClassifier()
    val ranges = DefaultHyperparams.defaultRange(mlp)
    assert(ranges.nonEmpty)
    assert(ranges.length === 4) // blockSize, maxIter, tol, layers
  }

  test("defaultRange for MultilayerPerceptronClassifier includes expected params") {
    val mlp = new MultilayerPerceptronClassifier()
    val ranges = DefaultHyperparams.defaultRange(mlp)
    val paramNames = ranges.map(_._1.name).toSet
    assert(paramNames.contains("blockSize"))
    assert(paramNames.contains("maxIter"))
    assert(paramNames.contains("tol"))
    assert(paramNames.contains("layers"))
  }

  test("defaultRange for NaiveBayes returns non-empty array") {
    val nb = new NaiveBayes()
    val ranges = DefaultHyperparams.defaultRange(nb)
    assert(ranges.nonEmpty)
    assert(ranges.length === 1) // smoothing
  }

  test("defaultRange for NaiveBayes includes smoothing param") {
    val nb = new NaiveBayes()
    val ranges = DefaultHyperparams.defaultRange(nb)
    val paramNames = ranges.map(_._1.name).toSet
    assert(paramNames.contains("smoothing"))
  }

  test("all defaultRange methods return valid Dist instances") {
    val lr = new LogisticRegression()
    val ranges = DefaultHyperparams.defaultRange(lr)
    ranges.foreach { case (param, dist) =>
      assert(param != null)
      assert(dist != null)
      // Verify we can get a next value from the distribution
      val value = dist.getNext()
      assert(value != null)
    }
  }
}
