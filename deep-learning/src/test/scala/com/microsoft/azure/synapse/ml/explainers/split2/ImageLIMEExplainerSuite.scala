// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.explainers.split2

import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import com.microsoft.azure.synapse.ml.core.utils.BreezeUtils._
import com.microsoft.azure.synapse.ml.explainers.LocalExplainer.LIME
import com.microsoft.azure.synapse.ml.explainers.{ImageExplainersSuite, ImageFormat, ImageLIME}
import com.microsoft.azure.synapse.ml.image.SuperpixelData
import com.microsoft.azure.synapse.ml.io.IOImplicits._
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.functions.col

class ImageLIMEExplainerSuite extends ImageExplainersSuite
  with TransformerFuzzing[ImageLIME] {

  import spark.implicits._

  lazy val lime: ImageLIME = LIME.image
    .setModel(resNetTransformer)
    .setTargetCol(resNetTransformer.getOutputCol)
    .setSamplingFraction(0.7)
    .setTargetClasses(Array(172))
    .setOutputCol("weights")
    .setSuperpixelCol("superpixels")
    .setMetricsCol("r2")
    .setInputCol("image")
    .setCellSize(120.0)
    .setModifier(20.0)
    .setNumSamples(3)

  test("ImageLIME can explain a model locally for image type observation") {
    val (image, superpixels, weights, r2) = lime
      .transform(imageDf)
      .select("image", "superpixels", "weights", "r2")
      .as[(ImageFormat, SuperpixelData, Seq[Vector], Vector)]
      .head

    val spStates = weights.head.toBreeze.map(_ >= 0.2).toArray

    // Uncomment the following lines lines to view the censoredImage image.
    // import com.microsoft.azure.synapse.ml.io.image.ImageUtils
    // import com.microsoft.azure.synapse.ml.lime.{Superpixel, SuperpixelData}
    // import java.awt.image.BufferedImage
    // val originalImage = ImageUtils.toBufferedImage(image.data, image.width, image.height, image.nChannels)
    // val censoredImage: BufferedImage = Superpixel.maskImage(originalImage, superpixels, spStates)
    // Superpixel.displayImage(censoredImage)
    // Thread.sleep(100000)
  }

  test("ImageLIME can explain a model locally for binary type observation") {
    val binaryDf = spark.read.binary.load(greyhoundImageLocation)
      .select(col("value.bytes").alias("image"))

    val (weights, r2) = lime
      .transform(binaryDf)
      .select("weights", "r2")
      .as[(Seq[Vector], Vector)]
      .head

    val spStates = weights.head.toBreeze.map(_ >= 0.2).toArray

    // Uncomment the following lines lines to view the censoredImage image.
    // import com.microsoft.azure.synapse.ml.io.image.ImageUtils
    // import com.microsoft.azure.synapse.ml.lime.{Superpixel, SuperpixelData}
    // import java.awt.image.BufferedImage
    // val originalImage = ImageUtils.toBufferedImage(image.data, image.width, image.height, image.nChannels)
    // val censoredImage: BufferedImage = Superpixel.maskImage(originalImage, superpixels, spStates)
    // Superpixel.displayImage(censoredImage)
    // Thread.sleep(100000)
  }

  override def testObjects(): Seq[TestObject[ImageLIME]] = Seq(new TestObject(lime, imageDf))

  override def reader: MLReadable[_] = ImageLIME
}
