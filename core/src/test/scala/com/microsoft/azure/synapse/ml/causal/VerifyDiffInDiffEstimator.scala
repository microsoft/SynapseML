// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.causal

import breeze.stats.distributions.RandBasis
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{EstimatorFuzzing, TestObject}
import org.apache.spark.ml.util.MLReadable

class VerifyDiffInDiffEstimator
  extends EstimatorFuzzing[DiffInDiffEstimator] {

  import spark.implicits._

  private lazy val rand = RandBasis.withSeed(47).uniform
  private lazy val data =
    (1 to 100).map(_ => (0, 0, rand.sample + 2)) ++
      (1 to 100).map(_ => (0, 1, rand.sample + 3)) ++
      (1 to 100).map(_ => (1, 0, rand.sample + 5)) ++
      (1 to 100).map(_ => (1, 1, rand.sample + 8))

  private lazy val df = data.toDF("treatment", "postTreatment", "outcome")
  private lazy val estimator = new DiffInDiffEstimator()
    .setTreatmentCol("treatment")
    .setPostTreatmentCol("postTreatment")
    .setOutcomeCol("outcome")

  test("DiffInDiffEstimator can estimate the treatment effect") {
    val summary = estimator.fit(df).getSummary
    // treatment effect is approximately 2
    assert(math.abs(summary.treatmentEffect - 2) < 1E-2)
  }

  override def testObjects(): Seq[TestObject[DiffInDiffEstimator]] = Seq(
    new TestObject(estimator, df, df)
  )

  override def reader: MLReadable[_] = DiffInDiffEstimator

  override def modelReader: MLReadable[_] = DiffInDiffModel
}

