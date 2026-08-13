// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.automl

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.ml.classification._

// scalastyle:off magic.number
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

  test("LogisticRegression default range is non-empty") {
    val lr = new LogisticRegression()
    val params = DefaultHyperparams.defaultRange(lr)
    assert(params.nonEmpty)
    val paramNames = params.map(_._1.name).toSet
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

  test("DecisionTreeClassifier default range is non-empty") {
    val dt = new DecisionTreeClassifier()
    val params = DefaultHyperparams.defaultRange(dt)
    assert(params.nonEmpty)
    val paramNames = params.map(_._1.name).toSet
    assert(paramNames.contains("maxBins"))
    assert(paramNames.contains("maxDepth"))
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

  test("GBTClassifier default range is non-empty") {
    val gbt = new GBTClassifier()
    val params = DefaultHyperparams.defaultRange(gbt)
    assert(params.nonEmpty)
    assert(params.length >= 5)
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

  test("RandomForestClassifier default range is non-empty") {
    val rf = new RandomForestClassifier()
    val params = DefaultHyperparams.defaultRange(rf)
    assert(params.nonEmpty)
    val paramNames = params.map(_._1.name).toSet
    assert(paramNames.contains("numTrees"))
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

  test("MultilayerPerceptronClassifier default range is non-empty") {
    val mlp = new MultilayerPerceptronClassifier()
    val params = DefaultHyperparams.defaultRange(mlp)
    assert(params.nonEmpty)
    val paramNames = params.map(_._1.name).toSet
    assert(paramNames.contains("blockSize"))
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

  test("NaiveBayes default range is non-empty") {
    val nb = new NaiveBayes()
    val params = DefaultHyperparams.defaultRange(nb)
    assert(params.nonEmpty)
    val paramNames = params.map(_._1.name).toSet
    assert(paramNames.contains("smoothing"))
  }

  test("all defaultRange entries are concrete distributions that sample in range") {
    val lr = new LogisticRegression()
    val ranges = DefaultHyperparams.defaultRange(lr)
    assert(ranges.nonEmpty)
    ranges.foreach { case (param, dist) =>
      assert(param != null)
      dist match {
        case d: IntRangeHyperParam =>
          val v = d.getNext(); assert(v >= d.min && v < d.max, s"${param.name} sampled $v")
        case d: LongRangeHyperParam =>
          val v = d.getNext(); assert(v >= d.min && v < d.max, s"${param.name} sampled $v")
        case d: DoubleRangeHyperParam =>
          val v = d.getNext(); assert(v >= d.min && v < d.max, s"${param.name} sampled $v")
        case d: FloatRangeHyperParam =>
          val v = d.getNext(); assert(v >= d.min && v < d.max, s"${param.name} sampled $v")
        case d: DiscreteHyperParam[_] =>
          assert(d.getValues.size > 0, s"${param.name} has no discrete values")
        case other =>
          fail(s"${param.name} mapped to an unsupported Dist: ${other.getClass.getName}")
      }
    }
  }

  test("default ranges produce values inside the declared bounds") {
    val lr = new LogisticRegression()
    val params = DefaultHyperparams.defaultRange(lr)
    def dist(name: String): Dist[_] = params.find(_._1.name == name).get._2
    // Bounds come from DefaultHyperparams.defaultRange(LogisticRegression). Asserting only
    // non-null would pass even if every distribution returned a constant.
    (1 to 50).foreach { _ =>
      val reg = dist("regParam").getNext.asInstanceOf[Double]
      assert(reg >= 0.001 && reg < 1.0)
      val elastic = dist("elasticNetParam").getNext.asInstanceOf[Double]
      assert(elastic >= 0.001 && elastic < 1.0)
      val iters = dist("maxIter").getNext.asInstanceOf[Int]
      assert(iters >= 5 && iters < 10)
    }
  }
}
// scalastyle:on magic.number
