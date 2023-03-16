// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.explainers.split1

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV}
import breeze.stats.distributions.RandBasis
import breeze.stats.{mean, stddev}
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.core.utils.BreezeUtils._
import com.microsoft.azure.synapse.ml.explainers._
import com.microsoft.azure.synapse.ml.image.{Superpixel, SuperpixelData}
import com.microsoft.azure.synapse.ml.io.image.ImageUtils
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.scalactic.{Equality, TolerantNumerics}
import org.scalatest.matchers.should.Matchers._

import java.nio.file.{Files, Paths}
import javax.imageio.ImageIO

class SamplerSuite extends TestBase {

  implicit val doubleEquality: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(1E-5)

  test("ContinuousFeatureStats can draw samples") {
    implicit val randBasis: RandBasis = RandBasis.withSeed(123)

    val featureStats = ContinuousFeatureStats(1.5)
    val (samples, distances) = (1 to 1000).map {
      _ =>
        val state = featureStats.getRandomState(3.0)
        val sample = featureStats.sample(state)
        val distance = featureStats.getDistance(3.0, sample)
        (sample, distance)
    }.unzip

    // println(samples(0 to 100))
    assert(mean(samples) === 2.9557393788483997)
    assert(stddev(samples) === 1.483087711702025)
    assert(distances.forall(_ > 0))
  }

  test("DiscreteFeatureStats can draw samples") {
    implicit val randBasis: RandBasis = RandBasis.withSeed(123)

    val freqTable = Map(2d -> 60d, 1d -> 900d, 3d -> 40d)

    val featureStats = DiscreteFeatureStats(freqTable)

    val (samples, distances) = (1 to 1000).map {
      _ =>
        val state = featureStats.getRandomState(3.0)
        val sample = featureStats.sample(state)
        val distance = featureStats.getDistance(3.0, sample)
        (sample, distance)
    }.unzip

    assert(samples.count(_ == 1.0) == 897)
    assert(samples.count(_ == 2.0) == 63)
    assert(samples.count(_ == 3.0) == 40)

    assert(distances.count(_ == 0.0) == 40)
    assert(distances.count(_ == 1.0) == 960)
  }

  test("LIMEVectorSampler can draw samples") {
    implicit val randBasis: RandBasis = RandBasis.withSeed(123)
    val featureStats = Seq(
      ContinuousFeatureStats(5.3),
      ContinuousFeatureStats(1.5)
    )

    val sampler = new LIMEVectorSampler(Vectors.dense(15.2, 1.0), featureStats)
    val (samples, _, distances) = (1 to 1000).map {
      _ => sampler.sample
    }.unzip3

    val sampleMatrix = BDM(samples.map(_.toBreeze): _*)

    assert(mean(sampleMatrix(::, 0)) === 15.24238095323594)
    assert(stddev(sampleMatrix(::, 0)) === 5.362969946915462)

    assert(mean(sampleMatrix(::, 1)) === 1.018507479024409)
    assert(stddev(sampleMatrix(::, 1)) === 1.4940793291730479)

    assert(distances.forall(_ > 0d))
  }

  test("LIMETabularSampler can draw samples") {
    implicit val randBasis: RandBasis = RandBasis.withSeed(123)
    val featureStats = Seq(
      ContinuousFeatureStats(5.3),
      DiscreteFeatureStats(Map(2d -> 60d, 1d -> 900d, 3d -> 40d))
    )

    val row = new GenericRowWithSchema(
      Array[Any](3.2d, 1),
      StructType(Array(StructField("feature1", DoubleType), StructField("feature2", IntegerType)))
    )

    val sampler = new LIMETabularSampler(row, featureStats)

    val (samples, states, distances) = (1 to 1000).map {
      _ =>
        val (r, s, d) = sampler.sample
        (BDV(r.getAs[Double](0), r.getAs[Double](1)), s.toBreeze, d)
    }.unzip3

    val sampleMatrix = BDM(samples: _*)

    val statesMatrix = BDM(states: _*)

    assert(mean(sampleMatrix(::, 0)) === 2.9636538120292903)
    assert(stddev(sampleMatrix(::, 0)) === 5.3043309761267565)

    assert(sampleMatrix(::, 1).findAll(_ == 1d).size == 883)
    assert(sampleMatrix(::, 1).findAll(_ == 2d).size == 71)
    assert(sampleMatrix(::, 1).findAll(_ == 3d).size == 46)

    assert(distances.forall(_ > 0d))

    // For categorical variables, the state should be 1 if the sample is same as original, otherwise 0.
    assert(statesMatrix(::, 1).findAll(_ == 1d).size == 883)
    assert(statesMatrix(::, 1).findAll(_ != 1d).size == 117)

    // For continuous variables, the state should match with sample.
    assert(mean(statesMatrix(::, 0)) === 2.9636538120292903)
    assert(stddev(statesMatrix(::, 0)) === 5.3043309761267565)
  }

  test("LIMEImageSampler can draw samples") {

    implicit val randBasis: RandBasis = RandBasis.withSeed(123)
    val imageResource = this.getClass.getResource("/greyhound.jpg")
    val bi = ImageIO.read(imageResource)

    val spd: SuperpixelData = SuperpixelData.fromSuperpixel(new Superpixel(bi, 30d, 50d))

    val imageSampler = new LIMEImageSampler(bi, 0.7, spd)

    val (sample, mask, distance) = imageSampler.sample

    val (_, height, width, nChannels, _, data) = ImageUtils.toSparkImageTuple(sample)
    assert(width == 209)
    assert(height == 201)
    assert(nChannels == 3)

    // 35 superpixel clusters should be active.
    assert((mask.toBreeze :== 1.0).activeSize == 35)

    // In this test case, 10/45 superpixel clusters are turned off by black background,
    // so the distance should be sqrt(10/45).
    assert(distance === math.sqrt(10d / 45d))

    // Uncomment the following lines lines to view the randomly masked image.
    // Change the RandBasis seed to see a different mask image.
    // import com.microsoft.azure.synapse.ml.io.image.ImageUtils
    // import com.microsoft.azure.synapse.ml.image.Superpixel
    // val maskedImage = ImageUtils.toBufferedImage(data, width, height, nChannels)
    // Superpixel.displayImage(maskedImage)
    // Thread.sleep(100000)
  }

  test("LIMETextSampler can draw samples") {
    implicit val randBasis: RandBasis = RandBasis.withSeed(123)
    val file = this.getClass.getResource("/audio1.txt")

    val text = new String(Files.readAllBytes(Paths.get(file.toURI)))

    val tokens = text.toLowerCase.split("\\s")

    val textSampler = new LIMETextSampler(tokens, 0.7)
    val (sampled, state, distance) = textSampler.sample

    assert(sampled.length == (state.toBreeze :== 1.0).activeSize)
    assert(distance == math.sqrt((tokens.size - 80) / tokens.size.toDouble))
  }

  test("KernelSHAPTabularSampler can draw samples") {
    implicit val randBasis: RandBasis = RandBasis.mt0

    val numSamples = 8
    val backgroundRow = Row.fromSeq(Seq(1d, 2L, "3"))

    val instance = Row.fromSeq(Seq(4d, 5L, "6"))

    val sampler = new KernelSHAPTabularSampler(instance, backgroundRow, numSamples, 1E8)

    val samples = (1 to numSamples).map {
      _ =>
        val (r, _, _) = sampler.sample
        (r.getAs[Double](0), r.getAs[Long](1), r.getAs[String](2))
    }

    // samples.foreach(println)

    // Should get all 8 unique combinations
    samples.distinct.size shouldBe numSamples
    samples.foreach {
      case (col1, col2, col3) =>
        Seq(1d, 4d) should contain(col1)
        Seq(2L, 5L) should contain(col2)
        Seq("3", "6") should contain(col3)
    }

    // No more coalitions can be generated anymore
    an[NoSuchElementException] shouldBe thrownBy {
      sampler.sample
    }
  }

  test("KernelSHAPVectorSampler can draw samples") {
    implicit val randBasis: RandBasis = RandBasis.mt0
    val numSamples = 32
    val background = BDV.rand[Double](5, randBasis.uniform)
    val instance = BDV.rand[Double](5, randBasis.uniform)

    val sampler = new KernelSHAPVectorSampler(instance.toSpark, background.toSpark, numSamples, 1E8)

    val samples = (1 to numSamples).map {
      _ =>
        val (r, _, _) = sampler.sample
        r.toBreeze
    }

    samples.distinct.size shouldBe numSamples
    samples.foreach {
      vec =>
        vec.foreachPair {
          case (i, v) =>
            Seq(instance(i), background(i)) should contain(v)
        }
    }

    // No more coalitions can be generated anymore
    an[NoSuchElementException] shouldBe thrownBy {
      sampler.sample
    }
  }

  test("KernelSHAPImageSampler can draw samples") {

    implicit val randBasis: RandBasis = RandBasis.withSeed(123)
    val imageResource = this.getClass.getResource("/greyhound.jpg")
    val bi = ImageIO.read(imageResource)

    val spd: SuperpixelData = SuperpixelData.fromSuperpixel(new Superpixel(bi, 30d, 50d))

    val imageSampler = new KernelSHAPImageSampler(bi, spd, 150, 1E8)

    (0 to 1) foreach {
      _ =>
        val (_, mask, _) = imageSampler.sample
        val num1 = (mask.toBreeze :== 1.0).activeSize
        assert(num1 == 0 || num1 == 45)
    }

    (2 to 37) foreach {
      _ =>
        val (_, mask, _) = imageSampler.sample
        val num1 = (mask.toBreeze :== 1.0).activeSize
        assert(num1 == 1 || num1 == 44)
    }

    (38 to 55) foreach {
      _ =>
        val (_, mask, _) = imageSampler.sample
        val num1 = (mask.toBreeze :== 1.0).activeSize
        assert(num1 == 2 || num1 == 43)
    }

    val (next, _, _) = imageSampler.sample
    val (_, height, width, nChannels, _, data) = ImageUtils.toSparkImageTuple(next)

    // Uncomment the following lines lines to view the randomly masked image.
    // Change the RandBasis seed to see a different mask image.
    // import com.microsoft.azure.synapse.ml.io.image.ImageUtils
    // import com.microsoft.azure.synapse.ml.image.Superpixel
    // val maskedImage = ImageUtils.toBufferedImage(data, width, height, nChannels)
    // Superpixel.displayImage(maskedImage)
    // Thread.sleep(100000)
  }

  test("KernelSHAPTextSampler can draw samples") {
    implicit val randBasis: RandBasis = RandBasis.withSeed(123)

    val text = "Lorem ipsum dolor sit amet vivendum principes sadipscing cu has"
    val tokens = text.toLowerCase.split("\\s")

    val textSampler = new KernelSHAPTextSampler(tokens, 150, 1E8)
    val (samples, states) = (1 to 150).map {
      _ =>
        val (sampled, state, _) = textSampler.sample
        (sampled, state)
    }.unzip

    // No more coalitions can be generated anymore
    an[NoSuchElementException] shouldBe thrownBy {
      textSampler.sample
    }

    assert(samples.size == 150)
    assert(states.size == 150)
  }
}
