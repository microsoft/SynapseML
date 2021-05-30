package com.microsoft.ml.spark.explainers

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV}
import breeze.numerics.abs
import breeze.stats.distributions.RandBasis
import breeze.stats.{mean, stddev}
import com.microsoft.ml.spark.core.test.base.TestBase
import com.microsoft.ml.spark.explainers.BreezeUtils._
import com.microsoft.ml.spark.io.IOImplicits._
import com.microsoft.ml.spark.io.image.ImageUtils
import com.microsoft.ml.spark.lime.{Superpixel, SuperpixelData}
import org.apache.spark.ml.linalg.{Vectors => SVS}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._

import java.nio.file.{Files, Path}

class SamplerSuite extends TestBase {
  test("ContinuousFeatureStats can draw samples") {
    implicit val randBasis: RandBasis = RandBasis.withSeed(123)

    val featureStats = ContinuousFeatureStats(0, 1.5)
    val (samples, _, distances) = (1 to 1000).map {
      _ => featureStats.sample(3.0)
    }.unzip3

    // println(samples(0 to 100))
    assert(abs(mean(samples) - 2.9557393788483997) < 1e-5)
    assert(abs(stddev(samples) - 1.483087711702025) < 1e-5)
    assert(distances.forall(_ > 0))
  }

  test("DiscreteFeatureStats can draw samples") {
    implicit val randBasis: RandBasis = RandBasis.withSeed(123)

    val freqTable = Map(2d -> 60d, 1d -> 900d, 3d -> 40d)

    val featureStats = DiscreteFeatureStats(0, freqTable)

    val (samples, _, distances) = (1 to 1000).map {
      _ => featureStats.sample(3.0)
    }.unzip3

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
    val (samples, _, distances) = (1 to 1000).map {
      _ => sampler.sample(SVS.dense(3.2, 1.0))
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
        val (r, _, d) = sampler.sample(row)
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

  test("ImageFeatureSampler can draw samples") {
    import spark.implicits._

    implicit val randBasis: RandBasis = RandBasis.withSeed(123)
    val imageResource = this.getClass.getResource("/greyhound.jpg")

    val df = spark.read.image.load(imageResource.toString)

    val Tuple1(image) = df.select("image").as[Tuple1[ImageFormat]].head

    val bi = ImageUtils.toBufferedImage(
      image.data, image.width, image.height, image.nChannels
    )

    val spd: SuperpixelData = SuperpixelData.fromSuperpixel(new Superpixel(bi, 30d, 50d))

    val imageSampler = ImageFeature(0.7, spd)

    val (sample, mask, distance) = imageSampler.sample(bi)

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

  test("TextFeatureSampler can draw samples") {
    implicit val randBasis: RandBasis = RandBasis.withSeed(123)
    val file = this.getClass.getResource("/audio1.txt")
    val text = Files.readString(Path.of(file.toURI))

    val tokens = text.toLowerCase.split("\\s")

    val textSampler = TextFeature(0.7)
    val (sampled, features, distance) = textSampler.sample(tokens)

    assert(sampled.length == (features.toBreeze :== 1.0).activeSize)
    assert(distance == math.sqrt((tokens.size - 80) / tokens.size.toDouble))
  }
}
