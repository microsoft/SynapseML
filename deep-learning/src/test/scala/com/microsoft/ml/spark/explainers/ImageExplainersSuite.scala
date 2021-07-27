// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.explainers

import java.io.File
import java.net.URL

import com.microsoft.ml.spark.cntk.{ImageFeaturizer, TrainedCNTKModelUtils}
import com.microsoft.ml.spark.core.test.base.TestBase
import com.microsoft.ml.spark.io.IOImplicits._
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.DataFrame

abstract class ImageExplainersSuite extends TestBase with TrainedCNTKModelUtils {
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

  val resNetTransformer: ImageFeaturizer = new ImageFeaturizer()
    .setInputCol(inputCol)
    .setOutputCol(outputCol)
    .setModel(resNet)
    .setCutOutputLayers(0)
    .setInputCol("image")
    .setMiniBatchSize(1)
}
