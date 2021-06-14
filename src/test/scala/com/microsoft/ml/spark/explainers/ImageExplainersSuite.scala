// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.explainers

import com.microsoft.ml.spark.core.test.base.TestBase
import com.microsoft.ml.spark.core.test.fuzzing.{TestObject, TransformerFuzzing}
import com.microsoft.ml.spark.explainers.BreezeUtils._
import com.microsoft.ml.spark.image.{ImageFeaturizer, NetworkUtils}
import com.microsoft.ml.spark.io.IOImplicits._
import com.microsoft.ml.spark.lime.SuperpixelData
import org.apache.commons.io.FileUtils
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import java.io.File
import java.net.URL

abstract class ImageExplainersSuite extends TestBase with NetworkUtils {
  val resNetTransformer: ImageFeaturizer = resNetModel()
    .setCutOutputLayers(0)
    .setInputCol("image")
    .setMiniBatchSize(1)

  val shap: ImageSHAP = LocalExplainer.KernelSHAP.image
    .setModel(resNetTransformer)
    .setTargetCol(resNetTransformer.getOutputCol)
    .setTargetClasses(Array(172))
    .setOutputCol("weights")
    .setSuperpixelCol("superpixels")
    .setMetricsCol("r2")
    .setInputCol("image")
    .setCellSize(120.0)
    .setModifier(20.0)
    .setNumSamples(8)

  val lime: ImageLIME = LocalExplainer.LIME.image
    .setModel(resNetTransformer)
    .setTargetCol(resNetTransformer.getOutputCol)
    .setSamplingFraction(0.7)
    .setTargetClasses(Array(172))
    .setOutputCol("weights")
    .setSuperpixelCol("superpixels")
    .setMetricsCol("r2")
    .setInputCol("image")
    .setCellSize(100)
    .setModifier(20)
    .setNumSamples(3)

  lazy val greyhoundImageLocation: String = {
    val loc = "/tmp/greyhound.jpg"
    val f = new File(loc)
    if (f.exists()) {
      f.delete()
    }
    FileUtils.copyURLToFile(new URL("https://mmlspark.blob.core.windows.net/datasets/LIME/greyhound.jpg"), f)
    loc
  }

  val imageDf: DataFrame = spark.read.image.load(greyhoundImageLocation)
}

class ImageSHAPExplainerSuite extends ImageExplainersSuite
  with TransformerFuzzing[ImageSHAP] {

  import spark.implicits._

  test("ImageKernelSHAP can explain a model locally") {
    val (image, superpixels, shapValues, r2) = shap
      .transform(imageDf)
      .select("image", "superpixels", "weights", "r2")
      .as[(ImageFormat, SuperpixelData, Seq[Vector], Vector)]
      .head

    // println(shapValues)
    // println(r2)

    // R2 should be almost 1.

    val spStates = shapValues.head.toBreeze(1 to -1).map(_ >= 0.05).toArray
    // println(spStates.count(identity))

    // Uncomment the following lines lines to view the censoredImage image.
    // import com.microsoft.ml.spark.io.image.ImageUtils
    // import com.microsoft.ml.spark.lime.Superpixel
    // import java.awt.image.BufferedImage
    // val originalImage = ImageUtils.toBufferedImage(image.data, image.width, image.height, image.nChannels)
    // val censoredImage: BufferedImage = Superpixel.maskImage(originalImage, superpixels, spStates)
    // Superpixel.displayImage(censoredImage)
    // Thread.sleep(100000)
  }

  override def testObjects(): Seq[TestObject[ImageSHAP]] = Seq(new TestObject(shap, imageDf))

  override def reader: MLReadable[_] = ImageSHAP
}

class ImageLIMEExplainerSuite extends ImageExplainersSuite
  with TransformerFuzzing[ImageLIME] {

  import spark.implicits._

  test("ImageLIME can explain a model locally for image type observation") {
    val (image, superpixels, weights, r2) = lime
      .transform(imageDf)
      .select("image", "superpixels", "weights", "r2")
      .as[(ImageFormat, SuperpixelData, Seq[Vector], Vector)]
      .head

    // println(weights)
    // println(r2)

    val spStates = weights.head.toBreeze.map(_ >= 0.2).toArray
    // println(spStates.count(identity))

    // Uncomment the following lines lines to view the censoredImage image.
    // import com.microsoft.ml.spark.io.image.ImageUtils
    // import com.microsoft.ml.spark.lime.{Superpixel, SuperpixelData}
    // import java.awt.image.BufferedImage
    // val originalImage = ImageUtils.toBufferedImage(image.data, image.width, image.height, image.nChannels)
    // val censoredImage: BufferedImage = Superpixel.maskImage(originalImage, superpixels, spStates)
    // Superpixel.displayImage(censoredImage)
    // Thread.sleep(100000)
  }

  // Do not run in CI pipeline due to high memory needs
  test("ImageLIME can explain a model locally for binary type observation") {
    val binaryDf = spark.read.binary.load(greyhoundImageLocation)
      .select(col("value.bytes").alias("image"))

    val (weights, r2) = lime
      .transform(binaryDf)
      .select("weights", "r2")
      .as[(Seq[Vector], Vector)]
      .head

    // println(weights)
    // println(r2)

    val spStates = weights.head.toBreeze.map(_ >= 0.2).toArray
    // println(spStates.count(identity))

    // Uncomment the following lines lines to view the censoredImage image.
    // import com.microsoft.ml.spark.io.image.ImageUtils
    // import com.microsoft.ml.spark.lime.{Superpixel, SuperpixelData}
    // import java.awt.image.BufferedImage
    // val originalImage = ImageUtils.toBufferedImage(image.data, image.width, image.height, image.nChannels)
    // val censoredImage: BufferedImage = Superpixel.maskImage(originalImage, superpixels, spStates)
    // Superpixel.displayImage(censoredImage)
    // Thread.sleep(100000)
  }


  override def testObjects(): Seq[TestObject[ImageLIME]] = Seq(new TestObject(lime, imageDf))

  override def reader: MLReadable[_] = ImageLIME
}
