// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.explainers

import com.microsoft.ml.spark.core.test.base.TestBase
import com.microsoft.ml.spark.core.test.fuzzing.{ExperimentFuzzing, PyTestFuzzing, TestObject}
import com.microsoft.ml.spark.explainers.BreezeUtils._
import com.microsoft.ml.spark.image.{ImageFeaturizer, NetworkUtils}
import com.microsoft.ml.spark.io.IOImplicits._
import com.microsoft.ml.spark.lime.SuperpixelData
import org.apache.spark.ml.linalg.{Vector => SV}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import java.net.URL

abstract class ImageExplainersSuite extends TestBase with NetworkUtils {
  val resNetTransformer: ImageFeaturizer = resNetModel().setCutOutputLayers(0).setInputCol("image")

  val cellSize = 30.0
  val modifier = 50.0
  val shap: ImageSHAP = LocalExplainer.KernelSHAP.image
    .setModel(resNetTransformer)
    .setTargetCol(resNetTransformer.getOutputCol)
    .setTargetClasses(Array(172))
    .setOutputCol("weights")
    .setSuperpixelCol("superpixels")
    .setMetricsCol("r2")
    .setInputCol("image")
    .setCellSize(cellSize)
    .setModifier(modifier)
    .setNumSamples(90)

  val lime: ImageLIME = LocalExplainer.LIME.image
    .setModel(resNetTransformer)
    .setTargetCol(resNetTransformer.getOutputCol)
    .setSamplingFraction(0.7)
    .setTargetClasses(Array(172))
    .setOutputCol("weights")
    .setSuperpixelCol("superpixels")
    .setMetricsCol("r2")
    .setInputCol("image")
    .setCellSize(cellSize)
    .setModifier(modifier)
    .setNumSamples(50)

  val imageResource: URL = this.getClass.getResource("/greyhound.jpg")
  val imageDf: DataFrame = spark.read.image.load(imageResource.toString)
}

class ImageSHAPSuite extends ImageExplainersSuite
  with ExperimentFuzzing[ImageSHAP]
  with PyTestFuzzing[ImageSHAP] {

  import spark.implicits._

  test("ImageKernelSHAP can explain a model locally") {
    val (image, superpixels, shapValues, r2) = shap
      .transform(imageDf)
      .select("image", "superpixels", "weights", "r2")
      .as[(ImageFormat, SuperpixelData, Seq[SV], SV)]
      .head

    // println(shapValues)
    // println(r2)

    // Base value should be almost zero.
    assert(math.abs(shapValues.head(0)) < 1e-4)

    // R2 should be almost 1.
    assert(math.abs(r2(0) - 1.0) < 1e-5)

    val spStates = shapValues.head.toBreeze(1 to -1).map(_ >= 0.05).toArray
    // println(spStates.count(identity))
    assert(spStates.count(identity) >= 6)
    // Uncomment the following lines lines to view the censoredImage image.
    // import com.microsoft.ml.spark.io.image.ImageUtils
    // import com.microsoft.ml.spark.lime.Superpixel
    // import java.awt.image.BufferedImage
    // val originalImage = ImageUtils.toBufferedImage(image.data, image.width, image.height, image.nChannels)
    // val censoredImage: BufferedImage = Superpixel.maskImage(originalImage, superpixels, spStates)
    // Superpixel.displayImage(censoredImage)
    // Thread.sleep(100000)
  }

  private lazy val testObjects: Seq[TestObject[ImageSHAP]] = Seq(new TestObject(shap, imageDf))

  override def experimentTestObjects(): Seq[TestObject[ImageSHAP]] = testObjects

  override def pyTestObjects(): Seq[TestObject[ImageSHAP]] = testObjects

}

class ImageLIMESuite extends ImageExplainersSuite
  with ExperimentFuzzing[ImageLIME]
  with PyTestFuzzing[ImageLIME] {

  import spark.implicits._

  test("ImageLIME can explain a model locally for image type observation") {
    val imageDf = spark.read.image.load(imageResource.toString)

    val (image, superpixels, weights, r2) = lime
      .transform(imageDf)
      .select("image", "superpixels", "weights", "r2")
      .as[(ImageFormat, SuperpixelData, Seq[SV], SV)]
      .head

    // println(weights)
    // println(r2)
    assert(math.abs(r2(0) - 0.91754) < 1e-2)

    val spStates = weights.head.toBreeze.map(_ >= 0.2).toArray
    // println(spStates.count(identity))
    assert(spStates.count(identity) == 8)

    // Uncomment the following lines lines to view the censoredImage image.
    // import com.microsoft.ml.spark.io.image.ImageUtils
    // import com.microsoft.ml.spark.lime.{Superpixel, SuperpixelData}
    // import java.awt.image.BufferedImage
    // val originalImage = ImageUtils.toBufferedImage(image.data, image.width, image.height, image.nChannels)
    // val censoredImage: BufferedImage = Superpixel.maskImage(originalImage, superpixels, spStates)
    // Superpixel.displayImage(censoredImage)
    // Thread.sleep(100000)
  }

  test("ImageLIME can explain a model locally for binary type observation") {
    val binaryDf = spark.read.binary.load(imageResource.toString)
      .select(col("value.bytes").alias("image"))

    val (weights, r2) = lime
      .transform(binaryDf)
      .select("weights", "r2")
      .as[(Seq[SV], SV)]
      .head

    // println(weights)
    // println(r2)
    assert(math.abs(r2(0) - 0.91754) < 1e-2)

    val spStates = weights.head.toBreeze.map(_ >= 0.2).toArray
    // println(spStates.count(identity))
    assert(spStates.count(identity) == 8)
  }

  private lazy val testObjects: Seq[TestObject[ImageLIME]] = Seq(new TestObject(lime, imageDf))

  override def experimentTestObjects(): Seq[TestObject[ImageLIME]] = testObjects

  override def pyTestObjects(): Seq[TestObject[ImageLIME]] = testObjects
}