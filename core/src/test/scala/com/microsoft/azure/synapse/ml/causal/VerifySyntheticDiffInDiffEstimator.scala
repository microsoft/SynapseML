package com.microsoft.azure.synapse.ml.causal

import breeze.linalg.sum
import breeze.stats.distributions.RandBasis
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{EstimatorFuzzing, TestObject}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.util.MLReadable
import org.scalactic.{Equality, TolerantNumerics}
import com.microsoft.azure.synapse.ml.causal.linalg._

class VerifySyntheticDiffInDiffEstimator
  extends EstimatorFuzzing[SyntheticDiffInDiffEstimator] {

  import spark.implicits._

  spark.sparkContext.setLogLevel("INFO")
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  private lazy val rb = RandBasis.withSeed(47)
  private implicit val DoubleEquality: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(1E-8)

  private lazy val data = {

    // 101 - 110
    val rand1 = rb.gaussian(100, 1)
    val controlPre1 = for {
      unit <- 1 to 99
      time <- 1 to 10
    } yield (unit, time, 0, 0, rand1.sample + time)

    // 51 - 60
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
  // Set LocalSolverThreshold to 1 to force Spark mode
  // Spark mode and breeze mode should get same loss history and same solution
  // .setLocalSolverThreshold(1)

  test("SyntheticDiffInDiffEstimator can estimate the treatment effect") {
    implicit val VectorOps: VectorOps[DVector] = DVectorOps

    val summary = estimator.fit(df).getSummary
    assert(summary.timeIntercept.get === 4.945419871092341)

    val timeWeights = summary.timeWeights.get.toBreeze
    assert(sum(timeWeights) === 1.0)
    assert(timeWeights.size === 10)
    assert(timeWeights.forall(0 <= _ && _ <= 1))

    assert(summary.unitIntercept.get === -54.712303815108314)
    val unitWeights = summary.unitWeights.get.toBreeze
    assert(sum(unitWeights) === 1.0)
    assert(unitWeights.size === 100)
    assert(unitWeights.forall(0 <= _ && _ <= 1))

    assert(summary.treatmentEffect === -14.92249952070377)
    assert(summary.standardError === 0.28948753364970986)
  }

  override def testObjects(): Seq[TestObject[SyntheticDiffInDiffEstimator]] = Seq(
    new TestObject(estimator, df, df)
  )

  override def reader: MLReadable[_] = SyntheticControlEstimator

  override def modelReader: MLReadable[_] = DiffInDiffModel
}
