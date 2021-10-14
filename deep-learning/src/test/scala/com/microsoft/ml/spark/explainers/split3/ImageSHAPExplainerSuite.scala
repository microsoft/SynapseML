// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.explainers.split3

import com.microsoft.ml.spark.core.test.fuzzing.{TestObject, TransformerFuzzing}
import com.microsoft.ml.spark.core.utils.BreezeUtils._
import com.microsoft.ml.spark.explainers.{ImageExplainersSuite, ImageFormat, ImageSHAP, LocalExplainer}
import com.microsoft.ml.spark.lime.SuperpixelData
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.Row

class ImageSHAPExplainerSuite extends ImageExplainersSuite
  with TransformerFuzzing[ImageSHAP] {

  import spark.implicits._

  val shap: ImageSHAP = LocalExplainer.KernelSHAP.image
    .setModel(resNetTransformer)
    .setTargetCol(resNetTransformer.getOutputCol)
    .setTargetClasses(Array(172))
    .setOutputCol("weights")
    .setSuperpixelCol("superpixels")
    .setMetricsCol("r2")
    .setInputCol("image")
    .setRegionSize(120)
    .setRuler(60f)
    .setMinElementSize(25)
    .setNumSamples(8)

  test("ImageKernelSHAP can explain a model locally") {
    val (image, superpixels, shapValues, r2) = shap
      .transform(imageDf)
      .select("image", "superpixels", "weights", "r2")
      .as[(ImageFormat, SuperpixelData, Seq[Vector], Vector)]
      .head

    // R2 should be almost 1.
    val imageRow = Row(image.origin, image.height, image.width, image.nChannels, image.mode, image.data)
    val spStates = shapValues.head.toBreeze(1 to -1).map(_ >= 0.05).toArray

    // Uncomment the following lines lines to view the censoredImage image.
    // import com.microsoft.ml.spark.io.image.ImageUtils
    // import com.microsoft.ml.spark.lime.Superpixel
    // val originalImage = ImageUtils.toCVMat(imageRow)
    // val censoredImage = Superpixel.maskImage(originalImage, superpixels, spStates)
    // Superpixel.displayImage(censoredImage)
  }

  override def testObjects(): Seq[TestObject[ImageSHAP]] = Seq(new TestObject(shap, imageDf))

  override def reader: MLReadable[_] = ImageSHAP
}
