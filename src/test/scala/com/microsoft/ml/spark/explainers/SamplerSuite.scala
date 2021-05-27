package com.microsoft.ml.spark.explainers

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV}
import breeze.numerics.abs
import breeze.stats.distributions.RandBasis
import breeze.stats.{mean, stddev}
import com.microsoft.ml.spark.core.test.base.TestBase
import com.microsoft.ml.spark.explainers.BreezeUtils._
import org.apache.spark.ml.linalg.{Vectors => SVS}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._

class SamplerSuite extends TestBase {
  test("ContinuousFeatureStats can draw samples") {
    implicit val randBasis: RandBasis = RandBasis.withSeed(123)

    val featureStats = ContinuousFeatureStats(0, 1.5)
    val (samples, distances) = (1 to 1000).map {
      _ => featureStats.sample(3.0)
    }.unzip

    // println(samples(0 to 100))
    assert(abs(mean(samples) - 2.9557393788483997) < 1e-5)
    assert(abs(stddev(samples) - 1.483087711702025) < 1e-5)
    assert(distances.forall(_ > 0))
  }

  test("DiscreteFeatureStats can draw samples") {
    implicit val randBasis: RandBasis = RandBasis.withSeed(123)

    val freqTable = Map(2d -> 60d, 1d -> 900d, 3d -> 40d)

    val featureStats = DiscreteFeatureStats(0, freqTable)

    val (samples, distances) = (1 to 1000).map {
      _ => featureStats.sample(3.0)
    }.unzip

//    println(samples(0 to 100))
//    println(distances(0 to 100))

    assert(samples.count(_ == 1.0) == 897)
    assert(samples.count(_ == 2.0) == 63)
    assert(samples.count(_ == 3.0) == 40)

    assert(distances.count(_ == 0.0) == 40)
    assert(distances.count(_ == 1.0) == 960)
  }

  test("LIMEVectorSampler can draw samples") {
    implicit val randBasis: RandBasis = RandBasis.withSeed(123)
    val featureStats = Seq(
      ContinuousFeatureStats(0, 5.3),
      DiscreteFeatureStats(1, Map(2d -> 60d, 1d -> 900d, 3d -> 40d))
    )

    val sampler = new LIMEVectorSampler(featureStats)
    val (samples, distances) = (1 to 1000).map {
      _ => sampler.sample(SVS.dense(3.2, 1.0))
    }.unzip

    val sampleMatrix = BDM(samples.map(_.toBreeze): _*)

//    println(mean(sampleMatrix(::, 0)))
//    println(stddev(sampleMatrix(::, 0)))

    assert(abs(mean(sampleMatrix(::, 0)) - 2.9636538120292903) < 1e-5)
    assert(abs(stddev(sampleMatrix(::, 0)) - 5.3043309761267565) < 1e-5)

    assert(sampleMatrix(::, 1).findAll(_ == 1d).size == 883)
    assert(sampleMatrix(::, 1).findAll(_ == 2d).size == 71)
    assert(sampleMatrix(::, 1).findAll(_ == 3d).size == 46)

    assert(distances.forall(_ > 0d))
  }

  test("LIMETabularSampler can draw samples") {
    implicit val randBasis: RandBasis = RandBasis.withSeed(123)
    val featureStats = Seq(
      ContinuousFeatureStats(0, 5.3),
      DiscreteFeatureStats(1, Map(2d -> 60d, 1d -> 900d, 3d -> 40d))
    )

    val sampler = new LIMETabularSampler(featureStats)

    val row = new GenericRowWithSchema(
      Array[Any](3.2d, 1),
      StructType(Array(StructField("feature1", DoubleType), StructField("feature2", IntegerType)))
    )

    val (samples, distances) = (1 to 1000).map {
      _ =>
        val (r, d) = sampler.sample(row)
        (BDV(r.getAs[Double](0), r.getAs[Double](1)), d)
    }.unzip

    val sampleMatrix = BDM(samples: _*)
//    println(mean(sampleMatrix(::, 0)))
//    println(stddev(sampleMatrix(::, 0)))

    assert(abs(mean(sampleMatrix(::, 0)) - 2.9636538120292903) < 1e-5)
    assert(abs(stddev(sampleMatrix(::, 0)) - 5.3043309761267565) < 1e-5)

    assert(sampleMatrix(::, 1).findAll(_ == 1d).size == 883)
    assert(sampleMatrix(::, 1).findAll(_ == 2d).size == 71)
    assert(sampleMatrix(::, 1).findAll(_ == 3d).size == 46)

    assert(distances.forall(_ > 0d))
  }
}
