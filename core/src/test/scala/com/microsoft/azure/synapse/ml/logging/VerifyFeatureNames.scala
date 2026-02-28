// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging

import com.microsoft.azure.synapse.ml.core.test.base.TestBase

class VerifyFeatureNames extends TestBase {

  test("AiServices constants have expected values") {
    assert(FeatureNames.AiServices.Anomaly === "aiservice-anomalydetection")
    assert(FeatureNames.AiServices.Face === "aiservice-face")
    assert(FeatureNames.AiServices.Form === "aiservice-form")
    assert(FeatureNames.AiServices.Language === "aiservice-language")
    assert(FeatureNames.AiServices.OpenAI === "aiservice-openai")
    assert(FeatureNames.AiServices.Search === "aiservice-search")
    assert(FeatureNames.AiServices.Speech === "aiservice-speech")
    assert(FeatureNames.AiServices.Text === "aiservice-text")
    assert(FeatureNames.AiServices.Translate === "aiservice-translate")
    assert(FeatureNames.AiServices.Vision === "aiservice-vision")
  }

  test("ML feature constants have expected values") {
    assert(FeatureNames.AutoML === "automl")
    assert(FeatureNames.Causal === "causal")
    assert(FeatureNames.Explainers === "explainers")
    assert(FeatureNames.Featurize === "featurize")
    assert(FeatureNames.Geospatial === "geospatial")
    assert(FeatureNames.Image === "image")
    assert(FeatureNames.IsolationForest === "isolationforest")
    assert(FeatureNames.NearestNeighbor === "nearestneighbor")
    assert(FeatureNames.Recommendation === "recommendation")
  }

  test("Deep learning and model feature constants have expected values") {
    assert(FeatureNames.DeepLearning === "deeplearning")
    assert(FeatureNames.OpenCV === "opencv")
    assert(FeatureNames.LightGBM === "lightgbm")
    assert(FeatureNames.VowpalWabbit === "vowpalwabbit")
  }

  test("Core constant has expected value") {
    assert(FeatureNames.Core === "core")
  }

  test("All AiServices constants start with 'aiservice-' prefix") {
    val aiServices = Seq(
      FeatureNames.AiServices.Anomaly,
      FeatureNames.AiServices.Face,
      FeatureNames.AiServices.Form,
      FeatureNames.AiServices.Language,
      FeatureNames.AiServices.OpenAI,
      FeatureNames.AiServices.Search,
      FeatureNames.AiServices.Speech,
      FeatureNames.AiServices.Text,
      FeatureNames.AiServices.Translate,
      FeatureNames.AiServices.Vision
    )
    aiServices.foreach { name =>
      assert(name.startsWith("aiservice-"), s"$name should start with 'aiservice-'")
    }
  }
}
