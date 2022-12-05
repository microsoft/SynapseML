// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.explainers

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.io.IOImplicits._
import com.microsoft.azure.synapse.ml.onnx.{ImageFeaturizer, ONNXModel, TrainedONNXModelUtils}
import com.microsoft.azure.synapse.ml.opencv.ImageTransformer
import org.apache.commons.io.FileUtils
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.FloatType

import java.io.File
import java.net.URL

abstract class ImageExplainersSuite extends TestBase with TrainedONNXModelUtils {
  lazy val greyhoundImageLocation: String = {
    val loc = "/tmp/greyhound.jpg"
    val f = new File(loc)
    if (f.exists()) {
      f.delete()
    }
    FileUtils.copyURLToFile(new URL("https://mmlspark.blob.core.windows.net/datasets/LIME/greyhound.jpg"), f)
    loc
  }

  lazy val imageDf: DataFrame = spark.read.image.load(greyhoundImageLocation)

  lazy val resNetTransformer: ImageFeaturizer = new ImageFeaturizer()
    .setInputCol(inputCol)
    .setOutputCol(outputCol)
    .setModel("resnet18")
    .setInputCol("image")
    .setMiniBatchSize(1)

  lazy val resNetOnnxTransformer: PipelineModel = {
    val featurizer = new ImageTransformer()
      .setInputCol("image")
      .setOutputCol("features")
      .resize(224, keepAspectRatio = true)
      .centerCrop(224, 224)
      .normalize(Array(0.485, 0.456, 0.406), Array(0.229, 0.224, 0.225), 1d / 255d)
      .setTensorElementType(FloatType)

    val onnx = new ONNXModel()
      .setModelPayload(resNetOnnxPayload)
      .setFeedDict(Map("data" -> "features"))
      .setFetchDict(Map("rawPrediction" -> "resnetv17_dense0_fwd"))
      .setSoftMaxDict(Map("rawPrediction" -> "probability"))
      .setMiniBatchSize(1)

    new Pipeline().setStages(Array(featurizer, onnx)).fit(imageDf)
  }
}
