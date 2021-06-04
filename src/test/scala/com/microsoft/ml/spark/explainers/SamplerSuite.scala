// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.explainers

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV}
import breeze.numerics.abs
import breeze.stats.distributions.RandBasis
import breeze.stats.{mean, stddev}
import com.microsoft.ml.spark.core.test.base.TestBase
import com.microsoft.ml.spark.explainers.BreezeUtils._
import com.microsoft.ml.spark.io.image.ImageUtils
import com.microsoft.ml.spark.lime.{Superpixel, SuperpixelData}
import org.apache.spark.ml.linalg.{Vectors => SVS}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.scalatest.Matchers._

import java.nio.file.{Files, Path}
import javax.imageio.ImageIO

class SamplerSuite extends TestBase {
  test("ContinuousFeatureStats can draw samples") {
    implicit val randBasis: RandBasis = RandBasis.withSeed(123)

    val featureStats = ContinuousFeatureStats(1.5)
    val (samples, distances) = (1 to 1000).map {
      _ =>
        val sample = featureStats.sample(3.0)
        val distance = featureStats.getDistance(3.0, sample)
        (sample, distance)
    }.unzip

    // println(samples(0 to 100))
    assert(abs(mean(samples) - 2.9557393788483997) < 1e-5)
    assert(abs(stddev(samples) - 1.483087711702025) < 1e-5)
    assert(distances.forall(_ > 0))
  }

  test("DiscreteFeatureStats can draw samples") {
    implicit val randBasis: RandBasis = RandBasis.withSeed(123)

    val freqTable = Map(2d -> 60d, 1d -> 900d, 3d -> 40d)

    val featureStats = DiscreteFeatureStats(freqTable)

    val (samples, distances) = (1 to 1000).map {
      _ =>
        val sample = featureStats.sample(3.0)
        val distance = featureStats.getDistance(3.0, sample)
        (sample, distance)
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
      ContinuousFeatureStats(5.3),
      DiscreteFeatureStats(Map(2d -> 60d, 1d -> 900d, 3d -> 40d))
    )

    val sampler = new LIMEVectorSampler(SVS.dense(3.2, 1.0), featureStats)
    val (samples, _, distances) = (1 to 1000).map {
      _ => sampler.sample
    }.unzip3

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

//    println(mean(sampleMatrix(::, 0)))
//    println(stddev(sampleMatrix(::, 0)))

    assert(abs(mean(sampleMatrix(::, 0)) - 2.9636538120292903) < 1e-5)
    assert(abs(stddev(sampleMatrix(::, 0)) - 5.3043309761267565) < 1e-5)

    assert(sampleMatrix(::, 1).findAll(_ == 1d).size == 883)
    assert(sampleMatrix(::, 1).findAll(_ == 2d).size == 71)
    assert(sampleMatrix(::, 1).findAll(_ == 3d).size == 46)

    assert(distances.forall(_ > 0d))

    // For categorical variables, the state should be 1 if the sample is same as original, otherwise 0.
    assert(statesMatrix(::, 1).findAll(_ == 1d).size == 883)
    assert(statesMatrix(::, 1).findAll(_ != 1d).size == 117)
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
    assert(math.abs(distance - math.sqrt(10d / 45d)) < 1e-6)

    // Uncomment the following lines lines to view the randomly masked image.
    // Change the RandBasis seed to see a different mask image.
    // import com.microsoft.ml.spark.io.image.ImageUtils
    // import com.microsoft.ml.spark.lime.Superpixel
    // val maskedImage = ImageUtils.toBufferedImage(data, width, height, nChannels)
    // Superpixel.displayImage(maskedImage)
    // Thread.sleep(100000)
  }

  test("LIMETextSampler can draw samples") {
    implicit val randBasis: RandBasis = RandBasis.withSeed(123)
    val file = this.getClass.getResource("/audio1.txt")
    val text = Files.readString(Path.of(file.toURI))

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

    val sampler = new KernelSHAPTabularSampler(instance, backgroundRow, numSamples)

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

    val sampler = new KernelSHAPVectorSampler(instance.toSpark, background.toSpark, numSamples)

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

    val imageSampler = new KernelSHAPImageSampler(bi, spd, 150)

    (0 to 1) foreach {
      _ =>
        val (_, mask, _) = imageSampler.sample
        val num1 = (mask.toBreeze :== 1.0).activeSize
        assert(num1 == 0 || num1 == 45)
    }

    (2 to 91) foreach {
      _ =>
        val (_, mask, _) = imageSampler.sample
        val num1 = (mask.toBreeze :== 1.0).activeSize
        assert(num1 == 1 || num1 == 44)
    }

    (92 to 148) foreach {
      i =>
        val (_, mask, _) = imageSampler.sample
        val num1 = (mask.toBreeze :== 1.0).activeSize
        assert(num1 == 2 || num1 == 43, i)
    }

    val (last, _, _) = imageSampler.sample
    val (_, height, width, nChannels, _, data) = ImageUtils.toSparkImageTuple(last)

    // No more coalitions can be generated anymore
    an[NoSuchElementException] shouldBe thrownBy {
      imageSampler.sample
    }

    // Uncomment the following lines lines to view the randomly masked image.
    // Change the RandBasis seed to see a different mask image.
    // import com.microsoft.ml.spark.io.image.ImageUtils
    // import com.microsoft.ml.spark.lime.Superpixel
    // val maskedImage = ImageUtils.toBufferedImage(data, width, height, nChannels)
    // Superpixel.displayImage(maskedImage)
    // Thread.sleep(100000)
  }

  test("KernelSHAPTextSampler can draw samples") {
    implicit val randBasis: RandBasis = RandBasis.withSeed(123)

    val text = "Lorem ipsum dolor sit amet vivendum principes sadipscing cu has"
    val tokens = text.toLowerCase.split("\\s")

    val textSampler = new KernelSHAPTextSampler(tokens, 150)
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
