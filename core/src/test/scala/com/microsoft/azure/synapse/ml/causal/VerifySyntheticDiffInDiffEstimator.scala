// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.causal

import breeze.linalg.sum
import breeze.stats.distributions.RandBasis
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{EstimatorFuzzing, TestObject}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.util.MLReadable
import org.scalactic.{Equality, TolerantNumerics}
import com.microsoft.azure.synapse.ml.causal.linalg._
import org.apache.spark.ml.param.ParamMap

class VerifySyntheticDiffInDiffEstimator
  extends EstimatorFuzzing[SyntheticDiffInDiffEstimator] {

  import spark.implicits._

  spark.sparkContext.setLogLevel("INFO")
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  private lazy val rb = RandBasis.withSeed(47)
  private implicit val equalityDouble: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(1E-8)

  private lazy val data = {
    val rand1 = rb.gaussian(100, 1)
    val controlPre1 = for {
      unit <- 1 to 99
      time <- 1 to 10
    } yield (unit, time, 0, 0, rand1.sample + time)

    val rand2 = rb.gaussian(50, 1)
    val controlPre2 = for {
      unit <- 100 to 100
      time <- 1 to 10
    } yield (unit, time, 0, 0, rand2.sample + time)

    val rand3 = rb.gaussian(110, 1)
    val controlPost1 = for {
      unit <- 1 to 99
      time <- 11 to 20
    } yield (unit, time, 0, 1, rand3.sample)

    val rand4 = rb.gaussian(60, 1)
    val controlPost2 = for {
      unit <- 100 to 100
      time <- 11 to 20
    } yield (unit, time, 0, 1, rand4.sample)

    val rand5 = rb.gaussian(50, 1)
    val treatPre = for {
      unit <- 101 to 200
      time <- 1 to 10
    } yield (unit, time, 1, 0, rand5.sample)

    val rand6 = rb.gaussian(40, 1)
    val treatPost = for {
      unit <- 101 to 200
      time <- 11 to 20
    } yield (unit, time, 1, 1, rand6.sample)

    controlPre1 ++ controlPre2 ++ controlPost1 ++ controlPost2 ++ treatPre ++ treatPost
  }

  private lazy val df = data.toDF("Unit", "Time", "treatment", "postTreatment", "outcome")

  private lazy val estimator = new SyntheticDiffInDiffEstimator()
    .setTreatmentCol("treatment")
    .setPostTreatmentCol("postTreatment")
    .setOutcomeCol("outcome")
    .setUnitCol("Unit")
    .setTimeCol("Time")
    .setMaxIter(500)

  test("SyntheticDiffInDiffEstimator can estimate the treatment effect in local mode") {
    implicit val vectorOps: VectorOps[DVector] = DVectorOps

    val summary = estimator.fit(df).getSummary
    assert(summary.timeIntercept.get === 4.948917186627611)

    val timeWeights = summary.timeWeights.get.toBreeze
    assert(sum(timeWeights) === 1.0)
    assert(timeWeights.size === 10)
    assert(timeWeights.forall(0 <= _ && _ <= 1))

    assert(summary.unitIntercept.get === -54.625356763024584)
    val unitWeights = summary.unitWeights.get.toBreeze
    assert(sum(unitWeights) === 1.0)
    assert(unitWeights.size === 100)
    assert(unitWeights.forall(0 <= _ && _ <= 1))

    assert(summary.treatmentEffect === -14.934064851225985)
    assert(summary.standardError === 0.30221430259614196)
  }

  test("SyntheticDiffInDiffEstimator can estimate the treatment effect in distributed mode") {
    // Set LocalSolverThreshold to 1 to force Spark mode
    val distributedEstimator = estimator.copy(ParamMap.empty)
      .asInstanceOf[SyntheticDiffInDiffEstimator]
      .setLocalSolverThreshold(1)
      // Set maxIter to smaller value to reduce unit test time. Result will be slightly different than local mode.
      .setMaxIter(20)

    implicit val vectorOps: VectorOps[DVector] = DVectorOps

    val summary = distributedEstimator.fit(df).getSummary
    assert(summary.timeIntercept.get === 4.924434349260821)

    val timeWeights = summary.timeWeights.get.toBreeze
    assert(sum(timeWeights) === 1.0)
    assert(timeWeights.size === 10)
    assert(timeWeights.forall(0 <= _ && _ <= 1))

    assert(summary.unitIntercept.get === -54.83322195267364)
    val unitWeights = summary.unitWeights.get.toBreeze

    assert(sum(unitWeights) === 1.0)
    assert(unitWeights.size === 100)
    assert(unitWeights.forall(0 <= _ && _ <= 1))

    // The values slightly differ from local mode since we're running less iterations to save time.
    assert(summary.treatmentEffect === -14.886230487053334)
    assert(summary.standardError === 0.27395418857682896)
  }

  override def testObjects(): Seq[TestObject[SyntheticDiffInDiffEstimator]] = Seq(
    new TestObject(estimator, df, df)
  )

  override def reader: MLReadable[_] = SyntheticControlEstimator

  override def modelReader: MLReadable[_] = DiffInDiffModel
}
