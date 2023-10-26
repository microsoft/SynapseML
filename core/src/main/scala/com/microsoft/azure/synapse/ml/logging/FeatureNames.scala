// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging

object FeatureNames {
  object AiServices {
    val Anomaly = "aiservice-anomalydetection"
    val BingImage = "aiservice-bingimage"
    val Face = "aiservice-face"
    val Form = "aiservice-form"
    val Language = "aiservice-language"
    val OpenAI = "aiservice-openai"
    val Search = "aiservice-search"
    val Speech = "aiservice-speech"
    val Text = "aiservice-text"
    val Translate = "aiservice-translate"
    val Vision = "aiservice-vision"
  }

  val AutoML = "automl"
  val Causal = "causal"
  val Explainers = "explainers"
  val Featurize = "featurize"
  val Geospatial = "geospatial"
  val Image = "image"
  val IsolationForest = "isolationforest"
  val NearestNeighbor = "nearestneighbor"
  val Recommendation = "recommendation"
  val DeepLearning = "deeplearning"
  val OpenCV = "opencv"
  val LightGBM = "lightgbm"
  val VowpalWabbit = "vowpalwabbit"

  val Core = "core"
}
