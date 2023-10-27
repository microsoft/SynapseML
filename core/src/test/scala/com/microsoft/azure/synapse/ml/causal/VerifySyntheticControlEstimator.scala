// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.causal

import breeze.linalg.sum
import breeze.stats.distributions.RandBasis
import com.microsoft.azure.synapse.ml.causal.linalg.{DVector, DVectorOps, VectorOps}
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{EstimatorFuzzing, TestObject}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.util.MLReadable
import org.scalactic.{Equality, TolerantNumerics}

class VerifySyntheticControlEstimator
  extends EstimatorFuzzing[SyntheticControlEstimator]{
  import spark.implicits._

  spark.sparkContext.setLogLevel("INFO")
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  private lazy val rb = RandBasis.withSeed(47)
  private implicit val equalityDouble: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(1E-8)

  private lazy val data = {
    val rand1 = rb.gaussian(100, 2)
    val controlPre1 = for {
      unit <- 1 to 99
      time <- 1 to 10
    } yield (unit, time, 0, 0, rand1.sample)

    val rand2 = rb.gaussian(50, 2)
    val controlPre2 = for {
      unit <- 100 to 100
      time <- 1 to 10
    } yield (unit, time, 0, 0, rand2.sample)

    val rand3 = rb.gaussian(90, 2)
    val controlPost1 = for {
      unit <- 1 to 99
      time <- 11 to 20
    } yield (unit, time, 0, 1, rand3.sample)

    val rand4 = rb.gaussian(30, 2)
    val controlPost2 = for {
      unit <- 100 to 100
      time <- 11 to 20
    } yield (unit, time, 0, 1, rand4.sample)

    val rand5 = rb.gaussian(50, 2)
    val treatPre = for {
      unit <- 101 to 200
      time <- 1 to 10
    } yield (unit, time, 1, 0, rand5.sample)

    val rand6 = rb.gaussian(40, 2)
    val treatPost = for {
      unit <- 101 to 200
      time <- 11 to 20
    } yield (unit, time, 1, 1, rand6.sample)

    controlPre1 ++ controlPre2 ++ controlPost1 ++ controlPost2 ++ treatPre ++ treatPost
  }

  private lazy val df = data.toDF("Unit", "Time", "treatment", "postTreatment", "outcome")

  private lazy val estimator = new SyntheticControlEstimator()
    .setTreatmentCol("treatment")
    .setPostTreatmentCol("postTreatment")
    .setOutcomeCol("outcome")
    .setUnitCol("Unit")
    .setTimeCol("Time")
    .setMaxIter(500)
    // Set LocalSolverThreshold to 1 to force Spark mode
    // Spark mode and breeze mode should get same loss history and same solution
    // .setLocalSolverThreshold(1)

  test("SyntheticControlEstimator can estimate the treatment effect") {
    val summary = estimator.fit(df).getSummary

    assert(summary.unitIntercept.get === 0d)

    implicit val vectorOps: VectorOps[DVector] = DVectorOps
    val unitWeights = summary.unitWeights.get.toBreeze
    assert(sum(unitWeights) === 1d)
    assert(unitWeights.size === 100)
    // Almost all weights go to the last unit
    // since all other units are intentionally placed far away from treatment units.
    assert(math.abs(unitWeights(99) - 1.0) < 0.01)
    assert(summary.treatmentEffect === 10.402769542126478)
    assert(summary.standardError === 0.1808473095549071)
  }

  override def testObjects(): Seq[TestObject[SyntheticControlEstimator]] = Seq(
    new TestObject(estimator, df, df)
  )

  override def reader: MLReadable[_] = SyntheticControlEstimator

  override def modelReader: MLReadable[_] = DiffInDiffModel
}
