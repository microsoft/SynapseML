// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.automl

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.ml.classification._

// scalastyle:off magic.number
class VerifyDefaultHyperparams extends TestBase {

  test("LogisticRegression default range is non-empty") {
    val lr = new LogisticRegression()
    val params = DefaultHyperparams.defaultRange(lr)
    assert(params.nonEmpty)
    val paramNames = params.map(_._1.name).toSet
    assert(paramNames.contains("regParam"))
    assert(paramNames.contains("elasticNetParam"))
    assert(paramNames.contains("maxIter"))
  }

  test("DecisionTreeClassifier default range is non-empty") {
    val dt = new DecisionTreeClassifier()
    val params = DefaultHyperparams.defaultRange(dt)
    assert(params.nonEmpty)
    val paramNames = params.map(_._1.name).toSet
    assert(paramNames.contains("maxBins"))
    assert(paramNames.contains("maxDepth"))
  }

  test("GBTClassifier default range is non-empty") {
    val gbt = new GBTClassifier()
    val params = DefaultHyperparams.defaultRange(gbt)
    assert(params.nonEmpty)
    assert(params.length >= 5)
  }

  test("RandomForestClassifier default range is non-empty") {
    val rf = new RandomForestClassifier()
    val params = DefaultHyperparams.defaultRange(rf)
    assert(params.nonEmpty)
    val paramNames = params.map(_._1.name).toSet
    assert(paramNames.contains("numTrees"))
  }

  test("MultilayerPerceptronClassifier default range is non-empty") {
    val mlp = new MultilayerPerceptronClassifier()
    val params = DefaultHyperparams.defaultRange(mlp)
    assert(params.nonEmpty)
    val paramNames = params.map(_._1.name).toSet
    assert(paramNames.contains("blockSize"))
    assert(paramNames.contains("layers"))
  }

  test("NaiveBayes default range is non-empty") {
    val nb = new NaiveBayes()
    val params = DefaultHyperparams.defaultRange(nb)
    assert(params.nonEmpty)
    val paramNames = params.map(_._1.name).toSet
    assert(paramNames.contains("smoothing"))
  }

  test("default ranges produce valid distributions") {
    val lr = new LogisticRegression()
    val params = DefaultHyperparams.defaultRange(lr)
    params.foreach { case (param, dist) =>
      val value = dist.getNext
      assert(value != null) // scalastyle:ignore null
    }
  }
}
// scalastyle:on magic.number
