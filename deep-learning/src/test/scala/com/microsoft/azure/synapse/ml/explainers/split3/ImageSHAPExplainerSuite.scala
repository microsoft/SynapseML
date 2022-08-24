// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.explainers.split3

import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import com.microsoft.azure.synapse.ml.core.utils.BreezeUtils._
import com.microsoft.azure.synapse.ml.explainers.LocalExplainer.KernelSHAP
import com.microsoft.azure.synapse.ml.explainers.{ImageExplainersSuite, ImageFormat, ImageSHAP}
import com.microsoft.azure.synapse.ml.image.SuperpixelData
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.util.MLReadable

class ImageSHAPExplainerSuite extends ImageExplainersSuite
  with TransformerFuzzing[ImageSHAP] {

  import spark.implicits._

  lazy val shap: ImageSHAP = KernelSHAP.image
    .setModel(resNetOnnxTransformer)
    .setTargetCol("probability")
    .setTargetClasses(Array(172))
    .setOutputCol("weights")
    .setSuperpixelCol("superpixels")
    .setMetricsCol("r2")
    .setInputCol("image")
    .setCellSize(120.0)
    .setModifier(20.0)
    .setNumSamples(8)

  test("ImageKernelSHAP can explain a model locally") {
    val (image, superpixels, shapValues, r2) = shap
      .transform(imageDf)
      .select("image", "superpixels", "weights", "r2")
      .as[(ImageFormat, SuperpixelData, Seq[Vector], Vector)]
      .head

    // R2 should be almost 1.

    val spStates = shapValues.head.toBreeze(1 to -1).map(_ >= 0.05).toArray

    // Uncomment the following lines lines to view the censoredImage image.
    // import com.microsoft.azure.synapse.ml.io.image.ImageUtils
    // import com.microsoft.azure.synapse.ml.image.Superpixel
    // import java.awt.image.BufferedImage
    // val originalImage = ImageUtils.toBufferedImage(image.data, image.width, image.height, image.nChannels)
    // val censoredImage: BufferedImage = Superpixel.maskImage(originalImage, superpixels, spStates)
    // Superpixel.displayImage(censoredImage)
    // Thread.sleep(100000)
  }

  override def testObjects(): Seq[TestObject[ImageSHAP]] = Seq(new TestObject(shap, imageDf))

  override def reader: MLReadable[_] = ImageSHAP
}
