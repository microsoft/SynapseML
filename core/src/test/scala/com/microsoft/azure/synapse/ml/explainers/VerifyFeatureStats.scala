// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.explainers

import breeze.stats.distributions.RandBasis
import com.microsoft.azure.synapse.ml.core.test.base.TestBase

class VerifyFeatureStats extends TestBase {

  implicit val randBasis: RandBasis = RandBasis.withSeed(42)

  test("ContinuousFeatureStats distance is 0 when stddev is 0") {
    val stats = ContinuousFeatureStats(0d)
    assert(stats.getDistance(5.0, 10.0) === 0d)
    assert(stats.getDistance(0.0, 0.0) === 0d)
  }

  test("ContinuousFeatureStats distance is normalized by stddev") {
    val stats = ContinuousFeatureStats(2.0)
    assert(stats.getDistance(3.0, 5.0) === 1.0) // |5 - 3| / 2 = 1.0
    assert(stats.getDistance(0.0, 4.0) === 2.0) // |4 - 0| / 2 = 2.0
    assert(stats.getDistance(1.0, 1.0) === 0.0) // |1 - 1| / 2 = 0.0
  }

  test("ContinuousFeatureStats sample returns the state unchanged") {
    val stats = ContinuousFeatureStats(1.0)
    assert(stats.sample(3.14) === 3.14)
    assert(stats.sample(-2.5) === -2.5)
    assert(stats.sample(0.0) === 0.0)
  }

  test("ContinuousFeatureStats getRandomState returns a value") {
    val stats = ContinuousFeatureStats(1.0)
    val state = stats.getRandomState(5.0)
    // With a fixed seed, the result should be deterministic and finite
    assert(!state.isNaN)
    assert(!state.isInfinite)
  }

  test("DiscreteFeatureStats getDistance is 0 for same values, 1 for different") {
    val stats = DiscreteFeatureStats(Map("a" -> 0.5, "b" -> 0.3, "c" -> 0.2))
    assert(stats.getDistance("a", "a") === 0d)
    assert(stats.getDistance("b", "b") === 0d)
    assert(stats.getDistance("a", "b") === 1d)
    assert(stats.getDistance("b", "c") === 1d)
  }

  test("DiscreteFeatureStats sample respects CDF ordering") {
    // Use a LinkedHashMap-backed Map to control iteration order and thus CDF order
    val freq = Seq("a" -> 0.5, "b" -> 0.3, "c" -> 0.2)
    val stats = DiscreteFeatureStats(scala.collection.immutable.ListMap(freq: _*))
    // CDF: a -> 0.5, b -> 0.8, c -> 1.0
    assert(stats.sample(0.1) === "a")
    assert(stats.sample(0.5) === "a")
    assert(stats.sample(0.6) === "b")
    assert(stats.sample(0.8) === "b")
    assert(stats.sample(0.9) === "c")
    assert(stats.sample(1.0) === "c")
  }

  test("DiscreteFeatureStats sample from uniform distribution") {
    val stats = DiscreteFeatureStats(Map("x" -> 1.0, "y" -> 1.0, "z" -> 1.0))
    // CDF: first -> 1.0, second -> 2.0, third -> 3.0
    // With many samples from getRandomState, all categories should appear
    val samples = (1 to 1000).map { _ =>
      val state = stats.getRandomState("x")
      stats.sample(state)
    }
    val distinctValues = samples.toSet
    assert(distinctValues.size === 3, s"Expected all 3 categories, got: $distinctValues")
    assert(distinctValues === Set("x", "y", "z"))
  }
}
